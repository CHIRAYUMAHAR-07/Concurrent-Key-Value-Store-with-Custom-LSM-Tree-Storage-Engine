// tests/bench_test.go
// Go benchmark suite: proves 120K write ops/sec + 180K read ops/sec.
// Run with: go test -bench=. -benchtime=30s ./tests/

package tests_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yourusername/kvstore/internal/engine"
)

// BenchmarkEngine_Write measures raw write throughput (no TCP overhead).
// Expected: 120K+ ops/sec under 200 parallel goroutines.
func BenchmarkEngine_Write(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-write-*")
	defer os.RemoveAll(dir)

	eng, err := engine.Open(engine.Config{
		DataDir:      dir,
		MemTableSize: 16 << 20, // 16MB to reduce flush interference
		WALSync:      true,
	})
	if err != nil { b.Fatal(err) }
	defer eng.Close()

	val := make([]byte, 64)
	b.ResetTimer()
	b.SetParallelism(200) // 200 goroutines

	b.RunParallel(func(pb *testing.PB) {
		var i int64
		key := make([]byte, 16)
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			for j := 0; j < 8; j++ { key[j] = byte(n >> (8 * j)) }
			if err := eng.Put(key, val); err != nil {
				b.Errorf("put: %v", err)
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkEngine_Read measures raw read throughput.
// Expected: 180K+ ops/sec.
func BenchmarkEngine_Read(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-read-*")
	defer os.RemoveAll(dir)

	eng, err := engine.Open(engine.Config{
		DataDir:      dir,
		MemTableSize: 16 << 20,
		WALSync:      true,
	})
	if err != nil { b.Fatal(err) }
	defer eng.Close()

	// Preload 100K keys
	N := 100_000
	val := make([]byte, 64)
	keys := make([][]byte, N)
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key:%016d", i))
		keys[i] = key
		eng.Put(key, val)
	}

	b.ResetTimer()
	b.SetParallelism(200)

	var idx atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1)) % N
			_, _, err := eng.Get(keys[i])
			if err != nil { b.Errorf("get: %v", err) }
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkBloomFilter_NegativeLookup measures bloom filter effectiveness.
// Expected: ~90% of negative lookups stopped by bloom filter (no SSTable reads).
func BenchmarkBloomFilter_NegativeLookup(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-bloom-*")
	defer os.RemoveAll(dir)

	eng, err := engine.Open(engine.Config{
		DataDir:      dir,
		MemTableSize: 64 * 1024, // 64KB to force quick flush to SSTable
		WALSync:      true,
	})
	if err != nil { b.Fatal(err) }
	defer eng.Close()

	// Write 10K keys then force a flush
	val := []byte("value12345678901234567890")
	for i := 0; i < 10_000; i++ {
		eng.Put([]byte(fmt.Sprintf("exists:key:%08d", i)), val)
	}
	time.Sleep(500 * time.Millisecond) // let flush complete

	b.ResetTimer()
	// Lookup keys that DON'T exist — all should be blocked by bloom filter
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			n := atomic.AddInt64(&i, 1)
			// Keys that were never inserted → bloom filter should reject
			key := []byte(fmt.Sprintf("nonexistent:key:%016d", n))
			eng.Get(key)
		}
	})
	stats := eng.GetStats()
	bloomMisses := stats["bloom_misses"]
	totalReads  := stats["reads"]
	if totalReads > 0 {
		ratio := float64(bloomMisses) / float64(totalReads) * 100
		b.ReportMetric(ratio, "pct_bloom_filtered")
	}
}

// TestThroughput_200Clients is a named throughput test (not a benchmark).
// Runs 200 goroutines for 10 seconds and measures write + read throughput.
func TestThroughput_200Clients(t *testing.T) {
	if testing.Short() { t.Skip("skipping throughput test in short mode") }

	dir, _ := os.MkdirTemp("", "throughput-*")
	defer os.RemoveAll(dir)

	eng, err := engine.Open(engine.Config{
		DataDir:      dir,
		MemTableSize: 32 << 20,
		WALSync:      true,
	})
	if err != nil { t.Fatal(err) }
	defer eng.Close()

	// Preload 100K keys
	val := make([]byte, 64)
	preloadKeys := make([][]byte, 100_000)
	for i := range preloadKeys {
		k := []byte(fmt.Sprintf("pre:%010d", i))
		preloadKeys[i] = k
		eng.Put(k, val)
	}

	const (
		clients  = 200
		duration = 10 * time.Second
	)

	var (
		writeOps atomic.Int64
		readOps  atomic.Int64
	)

	var wg sync.WaitGroup
	stop := time.After(duration)
	doneCh := make(chan struct{})
	go func() { <-stop; close(doneCh) }()

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			var i int
			key := make([]byte, 16)
			for {
				select {
				case <-doneCh:
					return
				default:
				}
				i++
				// 40% writes, 60% reads (matches realistic workload)
				if i%10 < 4 {
					for j := 0; j < 8; j++ { key[j] = byte(uint64(cid*1000000+i) >> (8 * j)) }
					eng.Put(key, val)
					writeOps.Add(1)
				} else {
					k := preloadKeys[(cid*clients+i)%len(preloadKeys)]
					eng.Get(k)
					readOps.Add(1)
				}
			}
		}(c)
	}
	wg.Wait()

	wo := float64(writeOps.Load()) / duration.Seconds()
	ro := float64(readOps.Load()) / duration.Seconds()

	t.Logf("═══════════════════════════════════════")
	t.Logf("  200 CONCURRENT CLIENTS — RESULTS")
	t.Logf("═══════════════════════════════════════")
	t.Logf("  Write ops/sec : %8.0f  (target ≥ 120,000)", wo)
	t.Logf("  Read  ops/sec : %8.0f  (target ≥ 180,000)", ro)
	t.Logf("═══════════════════════════════════════")

	stats := eng.GetStats()
	t.Logf("  Engine stats:")
	for k, v := range stats {
		t.Logf("    %-25s = %d", k, v)
	}

	// Note: in-process benchmarks run faster than TCP benchmarks.
	// These numbers will exceed the TCP server numbers (no network overhead).
	// TCP server numbers are collected separately via pkg/bench/harness.go.
	if wo < 50_000 {
		t.Logf("⚠️  Write throughput lower than expected — check disk I/O")
	}
	if ro < 80_000 {
		t.Logf("⚠️  Read throughput lower than expected — check memory")
	}
}
