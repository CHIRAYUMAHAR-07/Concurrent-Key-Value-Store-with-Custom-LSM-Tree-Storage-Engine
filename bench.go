package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourusername/kvstore/client"
)

type Config struct {
	Addr       string
	Clients    int
	Duration   time.Duration
	KeySize    int
	ValueSize  int
	ReadRatio  float64
	WarmupKeys int
}

type Result struct {
	Writes, Reads, Errors uint64
	Latencies             []time.Duration // sampled
}

func runBench(cfg Config) {
	fmt.Printf("\n╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  KV Store Benchmark — Custom Go Load Harness         ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  addr=%-46s║\n", cfg.Addr)
	fmt.Printf("║  clients=%-2d  duration=%-5s  keySize=%-4d  valSize=%-4d  ║\n",
		cfg.Clients, cfg.Duration, cfg.KeySize, cfg.ValueSize)
	fmt.Printf("║  readRatio=%.0f%%                                        ║\n", cfg.ReadRatio*100)
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n\n")

	if cfg.WarmupKeys > 0 {
		fmt.Printf("[warmup] loading %d keys...\n", cfg.WarmupKeys)
		wc, err := client.Dial(cfg.Addr)
		if err != nil {
			log.Fatalf("warmup dial: %v", err)
		}
		val := randStr(cfg.ValueSize)
		for i := 0; i < cfg.WarmupKeys; i++ {
			key := fmt.Sprintf("warmup:%08d", i)
			wc.Put(key, val)
		}
		wc.Close()
		fmt.Printf("[warmup] done (%d keys loaded)\n", cfg.WarmupKeys)
	}

	var (
		totalWrites  atomic.Uint64
		totalReads   atomic.Uint64
		totalErrors  atomic.Uint64
		latMu        sync.Mutex
		allLatencies []time.Duration
	)

	wg := &sync.WaitGroup{}
	stop := make(chan struct{})
	startTime := time.Now()

	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c, err := client.Dial(cfg.Addr)
			if err != nil {
				log.Printf("[worker %d] dial: %v", workerID, err)
				totalErrors.Add(1)
				return
			}
			defer c.Close()

			rng := rand.New(rand.NewSource(int64(workerID) * 1337))
			var localLats []time.Duration

			for {
				select {
				case <-stop:
					latMu.Lock()
					allLatencies = append(allLatencies, localLats...)
					latMu.Unlock()
					return
				default:
				}

				key := fmt.Sprintf("key:%08d", rng.Intn(cfg.WarmupKeys+100000))
				t0 := time.Now()

				if rng.Float64() < cfg.ReadRatio {
					_, err := c.Get(key)
					lat := time.Since(t0)
					if err == nil || err == client.ErrNotFound {
						totalReads.Add(1)
					} else {
						totalErrors.Add(1)
					}
					if rng.Intn(100) < 5 {
						localLats = append(localLats, lat)
					}
				} else {
					val := fmt.Sprintf("val-%d-%d", workerID, rng.Int63())
					err := c.Put(key, val)
					lat := time.Since(t0)
					if err == nil {
						totalWrites.Add(1)
					} else {
						totalErrors.Add(1)
					}
					if rng.Intn(100) < 5 {
						localLats = append(localLats, lat)
					}
				}
			}
		}(i)
	}

	ticker := time.NewTicker(1 * time.Second)
	deadline := time.After(cfg.Duration)

	var prevW, prevR uint64
	second := 0
	for {
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			ticker.Stop()
			printResults(cfg, startTime, totalWrites.Load(), totalReads.Load(),
				totalErrors.Load(), allLatencies)
			return

		case <-ticker.C:
			second++
			w := totalWrites.Load()
			r := totalReads.Load()
			wps := w - prevW
			rps := r - prevR
			prevW, prevR = w, r
			fmt.Printf("[t=%3ds] writes=%8d (%6d/s)  reads=%8d (%6d/s)  errors=%d\n",
				second, w, wps, r, rps, totalErrors.Load())
		}
	}
}

func printResults(cfg Config, start time.Time, writes, reads, errors uint64, lats []time.Duration) {
	elapsed := time.Since(start).Seconds()
	wps := float64(writes) / elapsed
	rps := float64(reads) / elapsed

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	p50 := percentile(lats, 0.50)
	p95 := percentile(lats, 0.95)
	p99 := percentile(lats, 0.99)
	p999 := percentile(lats, 0.999)

	fmt.Printf("\n╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║  BENCHMARK RESULTS                                   ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Duration     : %-35s ║\n", fmt.Sprintf("%.2fs", elapsed))
	fmt.Printf("║  Clients      : %-35d ║\n", cfg.Clients)
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Write ops    : %-35d ║\n", writes)
	fmt.Printf("║  Write ops/s  : %-35s ║\n", fmt.Sprintf("%.0f  (target: 120,000/s)", wps))
	fmt.Printf("║  Read  ops    : %-35d ║\n", reads)
	fmt.Printf("║  Read  ops/s  : %-35s ║\n", fmt.Sprintf("%.0f  (target: 180,000/s)", rps))
	fmt.Printf("║  Errors       : %-35d ║\n", errors)
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Latency P50  : %-35s ║\n", p50)
	fmt.Printf("║  Latency P95  : %-35s ║\n", p95)
	fmt.Printf("║  Latency P99  : %-35s ║\n", p99)
	fmt.Printf("║  Latency P999 : %-35s ║\n", p999)
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n\n")

	writeOK := wps >= 120_000
	readOK := rps >= 180_000
	fmt.Printf("Resume validation:\n")
	fmt.Printf("  120K writes/sec: %s (%.0f/s)\n", checkmark(writeOK), wps)
	fmt.Printf("  180K reads/sec:  %s (%.0f/s)\n", checkmark(readOK), rps)
}

func percentile(lats []time.Duration, p float64) string {
	if len(lats) == 0 {
		return "N/A"
	}
	idx := int(float64(len(lats)) * p)
	if idx >= len(lats) {
		idx = len(lats) - 1
	}
	return lats[idx].String()
}

func checkmark(ok bool) string {
	if ok {
		return " PASS"
	}
	return " FAIL (server may need tuning — see README)"
}

func randStr(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "Server address")
	clients := flag.Int("clients", 200, "Parallel clients")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	keySize := flag.Int("keysize", 16, "Key size bytes")
	valSize := flag.Int("valsize", 100, "Value size bytes")
	readRatio := flag.Float64("readratio", 0.6, "Read ratio (0–1)")
	warmup := flag.Int("warmup", 100000, "Warmup keys")
	flag.Parse()

	runBench(Config{
		Addr:       *addr,
		Clients:    *clients,
		Duration:   *duration,
		KeySize:    *keySize,
		ValueSize:  *valSize,
		ReadRatio:  *readRatio,
		WarmupKeys: *warmup,
	})
}
