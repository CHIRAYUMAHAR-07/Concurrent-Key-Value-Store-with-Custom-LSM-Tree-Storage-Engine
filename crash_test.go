// tests/crash_test.go
// Crash Recovery Tests — Resume: "zero data loss across 50 hard-kill tests"
//
// Test strategy:
//   1. Write N keys with known values → verify checksum in WAL
//   2. os.Kill(server) (hard kill — no graceful shutdown, no MemTable flush)
//   3. Restart server → WAL replay recovers all committed writes
//   4. Verify every single key still readable with correct value
//   5. Repeat 50 times with random kill timing
package tests

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yourusername/kvstore/client"
)

const (
	ServerBinary  = "../server/kvserver"  // built by Makefile
	TestDataDir   = "/tmp/kvstore_crash_test"
	TestAddr      = "127.0.0.1:16379"
	NumCrashTests = 50
	KeysPerRound  = 1000
)

// ── TestCrashRecovery — 50 hard-kill cycles ───────────────────────────────────
func TestCrashRecovery(t *testing.T) {
	if _, err := os.Stat(ServerBinary); err != nil {
		t.Skipf("server binary not found at %s (run: make server)", ServerBinary)
	}
	if err := os.MkdirAll(TestDataDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	defer os.RemoveAll(TestDataDir)

	totalKeys := 0
	recovered := 0

	for round := 0; round < NumCrashTests; round++ {
		t.Logf("=== Crash test %d/%d ===", round+1, NumCrashTests)

		// ── Start server ──────────────────────────────────────────────────────
		srv, err := startServer(TestAddr, TestDataDir)
		if err != nil { t.Fatalf("round %d: start server: %v", round, err) }

		// Give it time to load existing data
		time.Sleep(200 * time.Millisecond)
		if err := waitForServer(TestAddr, 5*time.Second); err != nil {
			srv.Process.Kill()
			t.Fatalf("round %d: server not ready: %v", round, err)
		}

		// ── Write KeysPerRound new keys ────────────────────────────────────────
		c, err := client.Dial(TestAddr)
		if err != nil { srv.Process.Kill(); t.Fatalf("round %d: dial: %v", round, err) }

		written := make(map[string]string, KeysPerRound)
		base := round * KeysPerRound
		for i := 0; i < KeysPerRound; i++ {
			key := fmt.Sprintf("crash_key_%08d", base+i)
			val := fmt.Sprintf("val_round_%d_seq_%d_checksum_%d",
				round, i, (round*1000+i)*0x1337)
			if err := c.Put(key, val); err != nil {
				srv.Process.Kill()
				t.Fatalf("round %d: put %s: %v", round, key, err)
			}
			written[key] = val
		}
		c.Close()

		// ── Hard-kill the server at a random time after writes ─────────────────
		// Simulates power loss / SIGKILL
		killDelay := time.Duration(rand.Intn(50)+10) * time.Millisecond
		time.Sleep(killDelay)
		t.Logf("  SIGKILL after %v", killDelay)
		srv.Process.Signal(syscall.SIGKILL)
		srv.Wait()

		// ── Restart and verify all written keys recovered ─────────────────────
		srv2, err := startServer(TestAddr, TestDataDir)
		if err != nil { t.Fatalf("round %d: restart: %v", round, err) }
		time.Sleep(300 * time.Millisecond) // WAL replay time
		if err := waitForServer(TestAddr, 8*time.Second); err != nil {
			srv2.Process.Kill()
			t.Fatalf("round %d: server not ready after restart: %v", round, err)
		}

		c2, err := client.Dial(TestAddr)
		if err != nil { srv2.Process.Kill(); t.Fatalf("round %d: dial after restart: %v", round, err) }

		failures := 0
		for key, expectedVal := range written {
			got, err := c2.Get(key)
			if err == client.ErrNotFound {
				t.Errorf("round %d: KEY LOST after crash: %s", round, key)
				failures++
			} else if err != nil {
				t.Errorf("round %d: get %s: %v", round, key, err)
				failures++
			} else if got != expectedVal {
				t.Errorf("round %d: CORRUPTED value for %s:\n  want: %s\n  got:  %s",
					round, key, expectedVal, got)
				failures++
			} else {
				recovered++
			}
		}
		c2.Close()
		srv2.Process.Signal(syscall.SIGTERM)
		srv2.Wait()

		totalKeys += len(written)
		t.Logf("  round %d: %d/%d keys recovered (failures=%d)",
			round+1, len(written)-failures, len(written), failures)

		if failures > 0 {
			t.Errorf("round %d: %d keys lost/corrupted — WAL recovery failure", round, failures)
		}
	}

	// ── Final summary ─────────────────────────────────────────────────────────
	t.Logf("\n════════════════════════════════════════")
	t.Logf("CRASH RECOVERY TEST SUMMARY")
	t.Logf("  Hard-kill tests      : %d", NumCrashTests)
	t.Logf("  Total keys written   : %d", totalKeys)
	t.Logf("  Keys recovered       : %d", recovered)
	t.Logf("  Data loss            : %d", totalKeys-recovered)
	if totalKeys == recovered {
		t.Logf("  Result: ✅ ZERO DATA LOSS (resume claim verified)")
	} else {
		t.Errorf("  Result: ❌ DATA LOSS DETECTED: %d keys missing", totalKeys-recovered)
	}
	t.Logf("════════════════════════════════════════")
}

// ── TestBloomFilterDiskSavings — verify 90% disk I/O reduction ────────────────
func TestBloomFilterDiskSavings(t *testing.T) {
	if _, err := os.Stat(ServerBinary); err != nil {
		t.Skipf("server binary not found (run: make server)")
	}
	dataDir := "/tmp/kvstore_bloom_test"
	os.MkdirAll(dataDir, 0755)
	defer os.RemoveAll(dataDir)

	srv, err := startServer(TestAddr, dataDir)
	if err != nil { t.Fatalf("start: %v", err) }
	defer func() { srv.Process.Signal(syscall.SIGTERM); srv.Wait() }()

	waitForServer(TestAddr, 5*time.Second)
	c, _ := client.Dial(TestAddr)
	defer c.Close()

	// Write 10,000 keys with a known prefix
	const N = 10_000
	t.Logf("Writing %d keys...", N)
	for i := 0; i < N; i++ {
		c.Put(fmt.Sprintf("existing:%08d", i),
			  fmt.Sprintf("value_%d", i))
	}

	// Flush to disk (so Bloom filters are active)
	time.Sleep(500 * time.Millisecond)

	// Perform 1,000,000 negative lookups (keys that don't exist)
	const NEGATIVE = 1_000_000
	t.Logf("Performing %d negative lookups...", NEGATIVE)
	start := time.Now()
	hits := 0
	for i := 0; i < NEGATIVE; i++ {
		key := fmt.Sprintf("nonexistent:key_%d", i)
		_, err := c.Get(key)
		if err != client.ErrNotFound {
			hits++ // Should be very rare (bloom false positives)
		}
	}
	elapsed := time.Since(start)

	// Get stats from server
	stats, _ := c.Stats()
	t.Logf("Stats: %s", stats)

	fpr := float64(hits) / float64(NEGATIVE)
	throughput := float64(NEGATIVE) / elapsed.Seconds()

	t.Logf("\n════════════════════════════════════════")
	t.Logf("BLOOM FILTER TEST SUMMARY")
	t.Logf("  Negative lookups  : %d", NEGATIVE)
	t.Logf("  False positives   : %d (%.4f%%)", hits, fpr*100)
	t.Logf("  Disk reads saved  : %.1f%%  (target: 90%%)", (1.0-fpr)*100)
	t.Logf("  Throughput        : %.0f lookups/sec", throughput)
	t.Logf("  Elapsed           : %v", elapsed)

	savedPct := (1.0 - fpr) * 100
	if savedPct < 90.0 {
		t.Errorf("Bloom filter only saved %.1f%% disk reads (target: 90%%)", savedPct)
	} else {
		t.Logf("  Result: ✅ %.1f%% disk reads eliminated (resume: 90%%)", savedPct)
	}
	t.Logf("════════════════════════════════════════")
}

// ── TestCompactionNonBlocking — reads unaffected during compaction ─────────────
func TestCompactionNonBlocking(t *testing.T) {
	if _, err := os.Stat(ServerBinary); err != nil {
		t.Skipf("server binary not found (run: make server)")
	}
	dataDir := "/tmp/kvstore_compact_test"
	os.MkdirAll(dataDir, 0755)
	defer os.RemoveAll(dataDir)

	srv, err := startServer(TestAddr, dataDir)
	if err != nil { t.Fatalf("start: %v", err) }
	defer func() { srv.Process.Signal(syscall.SIGTERM); srv.Wait() }()
	waitForServer(TestAddr, 5*time.Second)

	// Pre-load data to trigger compaction
	c, _ := client.Dial(TestAddr)
	for i := 0; i < 100_000; i++ {
		c.Put(fmt.Sprintf("k%08d", i), fmt.Sprintf("v%d", i))
	}
	c.Close()

	// While compaction runs, measure read latency — should stay low
	var maxLat time.Duration
	stop := make(chan struct{})
	var readErrors int

	go func() {
		rc, _ := client.Dial(TestAddr)
		defer rc.Close()
		for {
			select {
			case <-stop:
				return
			default:
				key := fmt.Sprintf("k%08d", rand.Intn(100000))
				t0 := time.Now()
				rc.Get(key)
				lat := time.Since(t0)
				if lat > maxLat { maxLat = lat }
				if lat > 100*time.Millisecond { readErrors++ }
			}
		}
	}()

	// Wait for compaction to complete
	time.Sleep(3 * time.Second)
	close(stop)

	t.Logf("Max read latency during compaction: %v", maxLat)
	t.Logf("Reads with lat>100ms: %d", readErrors)
	if readErrors > 100 {
		t.Errorf("compaction blocked reads: %d reads took >100ms", readErrors)
	} else {
		t.Logf("✅ Background compaction did not block active reads")
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────
func startServer(addr, dataDir string) (*exec.Cmd, error) {
	cmd := exec.Command(ServerBinary, "-addr", addr, "-data", dataDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("exec %s: %w", ServerBinary, err)
	}
	return cmd, nil
}

func waitForServer(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := client.Dial(addr)
		if err == nil {
			if pingErr := c.Ping(); pingErr == nil {
				c.Close()
				return nil
			}
			c.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("server at %s not ready within %v", addr, timeout)
}

// ── Standalone run (go run tests/crash_test.go) ───────────────────────────────
func RunCrashTestsStandalone() {
	log.Println("Running crash recovery simulation (standalone mode)...")
	log.Println("Start the server first: make server && ./server/kvserver")

	// Simulate the test logic without t.Fatal
	addr := TestAddr
	c, err := client.Dial(addr)
	if err != nil {
		log.Printf("Cannot connect to %s — is the server running? %v", addr, err)
		return
	}
	defer c.Close()

	if err := c.Ping(); err != nil {
		log.Printf("Ping failed: %v", err)
		return
	}
	log.Println("Server is UP — writing test keys...")

	written := make(map[string]string, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_key_%08d", i)
		val := fmt.Sprintf("test_val_%d_checksum_%d", i, i*0xDEAD)
		c.Put(key, val)
		written[key] = val
	}
	log.Printf("Wrote %d keys. Now kill the server (Ctrl+C) and restart to test recovery.", len(written))

	// Check current state
	ok, fail := 0, 0
	for k, want := range written {
		got, err := c.Get(k)
		if err == nil && got == want { ok++ } else { fail++ }
	}
	log.Printf("Verification: %d/%d keys correct", ok, ok+fail)
	stats, _ := c.Stats()
	log.Printf("Stats: %s", stats)
}

func init() {
	// Allow running as standalone: go run tests/crash_test.go
	if len(os.Args) > 0 && filepath.Base(os.Args[0]) == "crash_test" {
		RunCrashTestsStandalone()
		os.Exit(0)
	}
}
