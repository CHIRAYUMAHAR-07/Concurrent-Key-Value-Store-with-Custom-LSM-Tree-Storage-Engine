package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	CmdPUT   byte = 0x01
	CmdGET   byte = 0x02
	CmdDEL   byte = 0x03
	CmdSCAN  byte = 0x04
	CmdPING  byte = 0x05
	CmdSTATS byte = 0x06

	StatusOK       byte = 0x00
	StatusErr      byte = 0x01
	StatusNotFound byte = 0x02

	DefaultDialTimeout = 5 * time.Second
	DefaultIOTimeout   = 10 * time.Second
)

type Conn struct {
	conn   net.Conn
	mu     sync.Mutex
	closed bool
}

func Dial(addr string) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, DefaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("kvstore dial %s: %w", addr, err)
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	return &Conn{conn: c}, nil
}

func (c *Conn) Close() { c.mu.Lock(); defer c.mu.Unlock(); c.conn.Close(); c.closed = true }

func (c *Conn) Put(key, value string) error {
	c.mu.Lock(); defer c.mu.Unlock()
	if err := c.sendCmd(CmdPUT, []byte(key), []byte(value)); err != nil {
		return err
	}
	status, _, err := c.readResp()
	if err != nil { return err }
	if status != StatusOK { return fmt.Errorf("PUT error (status %02x)", status) }
	return nil
}

func (c *Conn) Get(key string) (string, error) {
	c.mu.Lock(); defer c.mu.Unlock()
	if err := c.sendCmd(CmdGET, []byte(key), nil); err != nil {
		return "", err
	}
	status, data, err := c.readResp()
	if err != nil { return "", err }
	if status == StatusNotFound { return "", ErrNotFound }
	if status != StatusOK      { return "", fmt.Errorf("GET error (status %02x)", status) }
	return string(data), nil
}

func (c *Conn) Delete(key string) error {
	c.mu.Lock(); defer c.mu.Unlock()
	if err := c.sendCmd(CmdDEL, []byte(key), nil); err != nil {
		return err
	}
	status, _, err := c.readResp()
	if err != nil { return err }
	if status != StatusOK { return fmt.Errorf("DEL error (status %02x)", status) }
	return nil
}

func (c *Conn) Scan(start, end string) ([]KV, error) {
	c.mu.Lock(); defer c.mu.Unlock()
	if err := c.sendCmd(CmdSCAN, []byte(start), []byte(end)); err != nil {
		return nil, err
	}
	status, data, err := c.readResp()
	if err != nil { return nil, err }
	if status != StatusOK { return nil, fmt.Errorf("SCAN error (status %02x)", status) }
	return parseScanResult(data), nil
}

func (c *Conn) Ping() error {
	c.mu.Lock(); defer c.mu.Unlock()
	if err := c.sendCmd(CmdPING, nil, nil); err != nil { return err }
	status, data, err := c.readResp()
	if err != nil { return err }
	if status != StatusOK || string(data) != "PONG" {
		return fmt.Errorf("ping failed")
	}
	return nil
}

func (c *Conn) Stats() (string, error) {
	c.mu.Lock(); defer c.mu.Unlock()
	if err := c.sendCmd(CmdSTATS, nil, nil); err != nil { return "", err }
	status, data, err := c.readResp()
	if err != nil { return "", err }
	if status != StatusOK { return "", fmt.Errorf("STATS error") }
	return string(data), nil
}

func (c *Conn) sendCmd(cmd byte, key, val []byte) error {
	c.conn.SetWriteDeadline(time.Now().Add(DefaultIOTimeout))
	buf := make([]byte, 0, 9+len(key)+len(val))
	buf = append(buf, cmd)
	buf = appendFrame(buf, key)
	buf = appendFrame(buf, val)
	_, err := c.conn.Write(buf)
	return err
}

func appendFrame(buf, data []byte) []byte {
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(data)))
	buf = append(buf, lb[:]...)
	buf = append(buf, data...)
	return buf
}

func (c *Conn) readResp() (byte, []byte, error) {
	c.conn.SetReadDeadline(time.Now().Add(DefaultIOTimeout))
	var hdr [5]byte
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return 0, nil, err
	}
	status := hdr[0]
	n      := int(binary.LittleEndian.Uint32(hdr[1:5]))
	if n == 0 { return status, nil, nil }
	data := make([]byte, n)
	_, err := io.ReadFull(c.conn, data)
	return status, data, err
}

type KV struct{ Key, Value string }
var ErrNotFound = fmt.Errorf("key not found")

func parseScanResult(data []byte) []KV {
	if len(data) < 4 { return nil }
	count := int(binary.LittleEndian.Uint32(data[:4]))
	out := make([]KV, 0, count)
	off := 4
	for i := 0; i < count && off < len(data); i++ {
		if off+4 > len(data) { break }
		kl := int(binary.LittleEndian.Uint32(data[off:])): off += 4
		if off+kl > len(data) { break }
		k := string(data[off : off+kl]); off += kl
		if off+4 > len(data) { break }
		vl := int(binary.LittleEndian.Uint32(data[off:])); off += 4
		if off+vl > len(data) { break }
		v := string(data[off : off+vl]); off += vl
		out = append(out, KV{k, v})
	}
	return out
}

type Pool struct {
	addr  string
	mu    sync.Mutex
	idle  []*Conn
	size  int
}

func NewPool(addr string, size int) *Pool {
	return &Pool{addr: addr, size: size}
}

func (p *Pool) Get() (*Conn, error) {
	p.mu.Lock()
	if len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	return Dial(p.addr)
}

func (p *Pool) Put(c *Conn) {
	if c.closed { return }
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) < p.size {
		p.idle = append(p.idle, c)
	} else {
		c.Close()
	}
}
