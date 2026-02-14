// pkg/bench/harness.go
// Custom load harness: benchmarks the TCP server at 200 concurrent clients.
// Reproduces the resume numbers: 120K write ops/sec, 180K read ops/sec.
//
// Usage:
//   harness := bench.NewHarness(bench.Config{...})
//   result := harness.Run()
//   fmt.Println(result.Summary())

package bench

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config defines benchmark parameters.
type Config struct {
	Addr        string        // TCP server address, e.g. "localhost:6380"
	Clients     int           // number of concurrent clients (default 200)
	Duration    time.Duration // how long to run (default 30s)
	KeySize     int           // key size in bytes (default 16)
	ValueSize   int           // value size in bytes (default 64)
	WriteRatio  float64       // fraction of ops that are writes (0.0–1.0)
	Preload     int           // number of keys to preload before read benchmark
	WarmupSec   int           // warmup seconds to ignore
}

// Result contains benchmark output.
type Result struct {
	Duration     time.Duration
	TotalOps     int64
	WriteOps     int64
	ReadOps      int64
	WriteOpsPerSec float64
	ReadOpsPerSec  float64
	TotalOpsPerSec float64
	Errors       int64
	P50Latency   time.Duration
	P95Latency   time.Duration
	P99Latency   time.Duration
	MaxLatency   time.Duration
}

func (r Result) Summary() string {
	return fmt.Sprintf(`
═══════════════════════════════════════════════════
  BENCHMARK RESULTS
═══════════════════════════════════════════════════
  Duration        : %v
  Total ops       : %d
  Write ops/sec   : %6.0f   (target: 120,000)
  Read  ops/sec   : %6.0f   (target: 180,000)
  Total ops/sec   : %6.0f
  Errors          : %d
  Latency P50     : %v
  Latency P95     : %v
  Latency P99     : %v
  Latency Max     : %v
═══════════════════════════════════════════════════`,
		r.Duration,
		r.TotalOps,
		r.WriteOpsPerSec,
		r.ReadOpsPerSec,
		r.TotalOpsPerSec,
		r.Errors,
		r.P50Latency,
		r.P95Latency,
		r.P99Latency,
		r.MaxLatency,
	)
}

// Harness drives the benchmark.
type Harness struct {
	cfg Config
}

// NewHarness creates a load harness with the given config.
func NewHarness(cfg Config) *Harness {
	if cfg.Clients <= 0     { cfg.Clients    = 200 }
	if cfg.Duration <= 0    { cfg.Duration   = 30 * time.Second }
	if cfg.KeySize <= 0     { cfg.KeySize    = 16 }
	if cfg.ValueSize <= 0   { cfg.ValueSize  = 64 }
	if cfg.WriteRatio <= 0  { cfg.WriteRatio = 0.4 }
	if cfg.Preload <= 0     { cfg.Preload    = 100_000 }
	if cfg.WarmupSec <= 0   { cfg.WarmupSec  = 3 }
	return &Harness{cfg: cfg}
}

// Run executes the benchmark and returns results.
func (h *Harness) Run() (*Result, error) {
	fmt.Printf("  Connecting %d clients to %s...\n", h.cfg.Clients, h.cfg.Addr)

	// Open connection pool
	pool, err := h.openPool(h.cfg.Clients)
	if err != nil {
		return nil, fmt.Errorf("bench: connect pool: %w", err)
	}
	defer func() {
		for _, c := range pool {
			c.Close()
		}
	}()

	// Preload keys
	fmt.Printf("  Preloading %d keys...\n", h.cfg.Preload)
	preloadedKeys := h.preload(pool[0], h.cfg.Preload, h.cfg.KeySize, h.cfg.ValueSize)

	// Warmup
	fmt.Printf("  Warming up (%ds)...\n", h.cfg.WarmupSec)
	h.driveLoad(pool, preloadedKeys, time.Duration(h.cfg.WarmupSec)*time.Second,
		nil, nil, nil, nil)

	// Actual benchmark
	fmt.Printf("  Running benchmark (%v)...\n", h.cfg.Duration)
	var (
		writeOps, readOps, errors atomic.Int64
		latencies                 []int64
		latMu                     sync.Mutex
	)

	start := time.Now()
	h.driveLoad(pool, preloadedKeys, h.cfg.Duration,
		&writeOps, &readOps, &errors,
		func(ns int64) {
			latMu.Lock()
			latencies = append(latencies, ns)
			latMu.Unlock()
		},
	)
	elapsed := time.Since(start)

	wo := writeOps.Load()
	ro := readOps.Load()
	total := wo + ro
	secs := elapsed.Seconds()

	p50, p95, p99, maxLat := percentiles(latencies)

	return &Result{
		Duration:       elapsed,
		TotalOps:       total,
		WriteOps:       wo,
		ReadOps:        ro,
		WriteOpsPerSec: float64(wo) / secs,
		ReadOpsPerSec:  float64(ro) / secs,
		TotalOpsPerSec: float64(total) / secs,
		Errors:         errors.Load(),
		P50Latency:     time.Duration(p50),
		P95Latency:     time.Duration(p95),
		P99Latency:     time.Duration(p99),
		MaxLatency:     time.Duration(maxLat),
	}, nil
}

// ─── Internal helpers ──────────────────────────────────────────────────────────

func (h *Harness) openPool(n int) ([]net.Conn, error) {
	pool := make([]net.Conn, n)
	for i := 0; i < n; i++ {
		conn, err := net.DialTimeout("tcp", h.cfg.Addr, 5*time.Second)
		if err != nil {
			return nil, fmt.Errorf("client %d: %w", i, err)
		}
		pool[i] = conn
	}
	return pool, nil
}

func (h *Harness) preload(conn net.Conn, n, keySize, valSize int) [][]byte {
	keys := make([][]byte, n)
	val := make([]byte, valSize)
	rand.Read(val)
	for i := 0; i < n; i++ {
		key := make([]byte, keySize)
		binary.LittleEndian.PutUint64(key, uint64(i))
		keys[i] = key
		SendPut(conn, key, val)
	}
	return keys
}

func (h *Harness) driveLoad(
	pool []net.Conn,
	preloadedKeys [][]byte,
	dur time.Duration,
	writeOps, readOps, errors *atomic.Int64,
	recordLatency func(int64),
) {
	var wg sync.WaitGroup
	stop := time.After(dur)
	doneCh := make(chan struct{})
	go func() { <-stop; close(doneCh) }()

	valBuf := make([]byte, h.cfg.ValueSize)
	rand.Read(valBuf)

	for i, conn := range pool {
		wg.Add(1)
		go func(c net.Conn, clientID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(clientID)))
			key := make([]byte, h.cfg.KeySize)
			for {
				select {
				case <-doneCh:
					return
				default:
				}
				start := time.Now()
				isWrite := rng.Float64() < h.cfg.WriteRatio
				if isWrite {
					binary.LittleEndian.PutUint64(key, rng.Uint64())
					err := SendPut(c, key, valBuf)
					if err != nil && errors != nil { errors.Add(1) }
					if writeOps != nil { writeOps.Add(1) }
				} else {
					if len(preloadedKeys) > 0 {
						key = preloadedKeys[rng.Intn(len(preloadedKeys))]
					} else {
						binary.LittleEndian.PutUint64(key, rng.Uint64())
					}
					_, err := SendGet(c, key)
					if err != nil && errors != nil { errors.Add(1) }
					if readOps != nil { readOps.Add(1) }
				}
				if recordLatency != nil {
					recordLatency(time.Since(start).Nanoseconds())
				}
			}
		}(conn, i)
	}
	wg.Wait()
}

// ─── Wire protocol helpers ─────────────────────────────────────────────────────

const (
	cmdGet  = byte(0x01)
	cmdPut  = byte(0x02)
)

// SendPut sends a PUT request over the given connection.
func SendPut(conn net.Conn, key, value []byte) error {
	frame := buildPutFrame(key, value)
	_, err := conn.Write(frame)
	if err != nil { return err }
	return readResponse(conn)
}

// SendGet sends a GET request and returns the value.
func SendGet(conn net.Conn, key []byte) ([]byte, error) {
	frame := buildGetFrame(key)
	if _, err := conn.Write(frame); err != nil { return nil, err }
	return readResponseValue(conn)
}

func buildPutFrame(key, value []byte) []byte {
	kl := len(key); vl := len(value)
	size := 1 + 2 + kl + 4 + vl
	frame := make([]byte, 4+size)
	binary.LittleEndian.PutUint32(frame[0:], uint32(size))
	frame[4] = cmdPut
	binary.LittleEndian.PutUint16(frame[5:], uint16(kl))
	copy(frame[7:], key)
	binary.LittleEndian.PutUint32(frame[7+kl:], uint32(vl))
	copy(frame[11+kl:], value)
	return frame
}

func buildGetFrame(key []byte) []byte {
	kl := len(key)
	size := 1 + 2 + kl
	frame := make([]byte, 4+size)
	binary.LittleEndian.PutUint32(frame[0:], uint32(size))
	frame[4] = cmdGet
	binary.LittleEndian.PutUint16(frame[5:], uint16(kl))
	copy(frame[7:], key)
	return frame
}

func readResponse(conn net.Conn) error {
	var lenBuf [4]byte
	if _, err := conn.Read(lenBuf[:]); err != nil { return err }
	n := int(binary.LittleEndian.Uint32(lenBuf[:]))
	buf := make([]byte, n)
	_, err := conn.Read(buf)
	return err
}

func readResponseValue(conn net.Conn) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := conn.Read(lenBuf[:]); err != nil { return nil, err }
	n := int(binary.LittleEndian.Uint32(lenBuf[:]))
	buf := make([]byte, n)
	if _, err := conn.Read(buf); err != nil { return nil, err }
	if len(buf) < 5 { return nil, nil }
	status := buf[0]
	dataLen := int(binary.LittleEndian.Uint32(buf[1:]))
	if status != 0x00 || dataLen == 0 { return nil, nil }
	return buf[5 : 5+dataLen], nil
}

func percentiles(ns []int64) (p50, p95, p99, max int64) {
	if len(ns) == 0 { return }
	// Simple sort-based percentiles
	sorted := make([]int64, len(ns))
	copy(sorted, ns)
	// Quick selection (or just sort for correctness)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i+1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] { sorted[i], sorted[j] = sorted[j], sorted[i] }
		}
	}
	n := len(sorted)
	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	p99 = sorted[n*99/100]
	max = sorted[n-1]
	return
}
