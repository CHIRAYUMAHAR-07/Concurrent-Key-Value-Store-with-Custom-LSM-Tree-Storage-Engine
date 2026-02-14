// internal/server/server.go
// TCP Server: handles 200+ concurrent clients with goroutine-per-connection model.
// Binary protocol with 4-byte length-prefix framing (no JSON overhead).
//
// Protocol:
//   Request:  [total_len:4][cmd:1][key_len:2][key:key_len][val_len:4][val:val_len]
//   Response: [total_len:4][status:1][data_len:4][data:data_len]
//
//   Commands: 0x01=GET, 0x02=PUT, 0x03=DELETE, 0x04=SCAN, 0x05=PING, 0x06=STATS
//   Status:   0x00=OK, 0x01=NOT_FOUND, 0x02=ERROR

package server

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourusername/kvstore/internal/engine"
)

// Command bytes
const (
	CmdGet    = byte(0x01)
	CmdPut    = byte(0x02)
	CmdDelete = byte(0x03)
	CmdScan   = byte(0x04)
	CmdPing   = byte(0x05)
	CmdStats  = byte(0x06)

	StatusOK       = byte(0x00)
	StatusNotFound = byte(0x01)
	StatusError    = byte(0x02)

	readTimeout  = 30 * time.Second
	writeTimeout = 10 * time.Second
	maxKeySize   = 1 << 16   // 64KB key limit
	maxValSize   = 1 << 24   // 16MB value limit
)

// Config holds TCP server configuration.
type Config struct {
	Addr       string // e.g. ":6380"
	MaxClients int    // e.g. 200
}

// Stats tracks server performance.
type Stats struct {
	Connections  atomic.Int64
	ActiveConns  atomic.Int64
	TotalRequests atomic.Int64
	Errors       atomic.Int64
	BytesRead    atomic.Int64
	BytesWritten atomic.Int64
}

// Server is the TCP key-value server.
type Server struct {
	cfg      Config
	eng      *engine.Engine
	listener net.Listener
	stats    Stats
	wg       sync.WaitGroup
	stopCh   chan struct{}
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
}

// NewServer creates a new TCP server backed by the given engine.
func NewServer(cfg Config, eng *engine.Engine) *Server {
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 200
	}
	return &Server{
		cfg:    cfg,
		eng:    eng,
		stopCh: make(chan struct{}),
		conns:  make(map[net.Conn]struct{}),
	}
}

// ListenAndServe starts the TCP server and blocks.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.cfg.Addr, err)
	}
	s.listener = ln
	log.Printf("server: listening on %s (max_clients=%d)", s.cfg.Addr, s.cfg.MaxClients)

	semaphore := make(chan struct{}, s.cfg.MaxClients)
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return nil // graceful shutdown
			default:
				return fmt.Errorf("server: accept: %w", err)
			}
		}
		// Acquire semaphore (back-pressure when at MaxClients)
		semaphore <- struct{}{}
		s.stats.Connections.Add(1)
		s.stats.ActiveConns.Add(1)
		s.registerConn(conn)
		s.wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				<-semaphore
				s.stats.ActiveConns.Add(-1)
				s.unregisterConn(c)
				s.wg.Done()
				c.Close()
			}()
			s.handleConn(c)
		}(conn)
	}
}

// Shutdown gracefully stops the server and waits for all connections to close.
func (s *Server) Shutdown() error {
	close(s.stopCh)
	if s.listener != nil {
		s.listener.Close()
	}
	// Close all active connections
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

// GetStats returns server + engine statistics.
func (s *Server) GetStats() map[string]int64 {
	m := map[string]int64{
		"total_connections": s.stats.Connections.Load(),
		"active_connections": s.stats.ActiveConns.Load(),
		"total_requests":    s.stats.TotalRequests.Load(),
		"errors":            s.stats.Errors.Load(),
		"bytes_read":        s.stats.BytesRead.Load(),
		"bytes_written":     s.stats.BytesWritten.Load(),
	}
	for k, v := range s.eng.GetStats() {
		m["engine_"+k] = v
	}
	return m
}

// ─── Per-connection handler ────────────────────────────────────────────────────

func (s *Server) handleConn(conn net.Conn) {
	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetKeepAlivePeriod(30 * time.Second)

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	for {
		// Read request: [total_len:4][cmd:1][key_len:2][key][val_len:4][val]
		conn.SetReadDeadline(time.Now().Add(readTimeout))

		var totalLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &totalLen); err != nil {
			if err != io.EOF {
				s.stats.Errors.Add(1)
			}
			return
		}
		if totalLen < 3 || totalLen > maxValSize+maxKeySize+16 {
			s.sendError(writer, "invalid frame length")
			return
		}

		frame := make([]byte, totalLen)
		if _, err := io.ReadFull(reader, frame); err != nil {
			s.stats.Errors.Add(1)
			return
		}
		s.stats.BytesRead.Add(int64(4 + int(totalLen)))
		s.stats.TotalRequests.Add(1)

		// Parse frame
		if len(frame) < 1 {
			s.sendError(writer, "empty frame")
			continue
		}
		cmd := frame[0]
		pos := 1

		var key, val []byte
		if cmd != CmdPing && cmd != CmdStats {
			if pos+2 > len(frame) {
				s.sendError(writer, "frame too short for key_len")
				continue
			}
			keyLen := int(binary.LittleEndian.Uint16(frame[pos:]))
			pos += 2
			if keyLen > maxKeySize || pos+keyLen > len(frame) {
				s.sendError(writer, "key too large or truncated")
				continue
			}
			key = frame[pos : pos+keyLen]
			pos += keyLen

			if cmd == CmdPut {
				if pos+4 > len(frame) {
					s.sendError(writer, "frame too short for val_len")
					continue
				}
				valLen := int(binary.LittleEndian.Uint32(frame[pos:]))
				pos += 4
				if valLen > maxValSize || pos+valLen > len(frame) {
					s.sendError(writer, "value too large or truncated")
					continue
				}
				val = frame[pos : pos+valLen]
			}
		}

		// Dispatch command
		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		switch cmd {
		case CmdGet:
			value, found, err := s.eng.Get(key)
			if err != nil {
				s.sendError(writer, err.Error())
			} else if !found {
				s.sendStatus(writer, StatusNotFound, nil)
			} else {
				s.sendStatus(writer, StatusOK, value)
			}
		case CmdPut:
			if err := s.eng.Put(key, val); err != nil {
				s.sendError(writer, err.Error())
			} else {
				s.sendStatus(writer, StatusOK, nil)
			}
		case CmdDelete:
			if err := s.eng.Delete(key); err != nil {
				s.sendError(writer, err.Error())
			} else {
				s.sendStatus(writer, StatusOK, nil)
			}
		case CmdPing:
			s.sendStatus(writer, StatusOK, []byte("PONG"))
		case CmdStats:
			stats := s.GetStats()
			data, _ := json.Marshal(stats)
			s.sendStatus(writer, StatusOK, data)
		default:
			s.sendError(writer, fmt.Sprintf("unknown command 0x%02x", cmd))
		}

		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (s *Server) sendStatus(w *bufio.Writer, status byte, data []byte) {
	dataLen := uint32(len(data))
	frame := make([]byte, 5+int(dataLen))
	frame[0] = status
	binary.LittleEndian.PutUint32(frame[1:], dataLen)
	copy(frame[5:], data)
	s.writeFrame(w, frame)
	s.stats.BytesWritten.Add(int64(4 + len(frame)))
}

func (s *Server) sendError(w *bufio.Writer, msg string) {
	s.sendStatus(w, StatusError, []byte(msg))
	s.stats.Errors.Add(1)
}

func (s *Server) writeFrame(w *bufio.Writer, payload []byte) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	w.Write(lenBuf[:])
	w.Write(payload)
}

func (s *Server) registerConn(c net.Conn) {
	s.mu.Lock(); s.conns[c] = struct{}{}; s.mu.Unlock()
}

func (s *Server) unregisterConn(c net.Conn) {
	s.mu.Lock(); delete(s.conns, c); s.mu.Unlock()
}
