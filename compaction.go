// internal/compaction/compaction.go
// Background compaction: merges SSTables, removes tombstones, reduces read amplification.
// Runs in a dedicated goroutine — NEVER blocks active reads or writes.
//
// Strategy: Level-based (similar to LevelDB)
//   L0: up to 4 SSTables (written by MemTable flushes)
//   L1: up to 10 SSTables (10x size of L0 files)
//   L2: up to 100 SSTables (10x size of L1 files)
//
// Trigger: L0 has >= 4 files → compact L0 → L1
//          L1 total size > threshold → compact L1 → L2

package compaction

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/yourusername/kvstore/internal/sstable"
)

const (
	L0MaxFiles    = 4              // trigger compaction when L0 has 4 files
	L1SizeLimit   = 10 << 20      // 10 MB L1 limit
	L2SizeLimit   = 100 << 20     // 100 MB L2 limit
	CompactPeriod = 200 * time.Millisecond
)

// Level represents a set of SSTables at one level.
type Level struct {
	mu    sync.RWMutex
	files []string // sorted SSTable paths
	level int
}

// Manager orchestrates background compaction.
type Manager struct {
	dataDir   string
	levels    [3]*Level // L0, L1, L2
	mu        sync.Mutex
	stopCh    chan struct{}
	doneCh    chan struct{}
	compacted int64 // stats: bytes compacted

	// Hook: called when compaction removes SSTable files
	// (LSMEngine uses this to update its file list)
	onCompact func(removed []string, added []string)
}

// NewManager creates a compaction manager.
func NewManager(dataDir string, onCompact func(removed, added []string)) *Manager {
	m := &Manager{
		dataDir: dataDir,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		onCompact: onCompact,
	}
	for i := range m.levels {
		m.levels[i] = &Level{level: i}
	}
	return m
}

// Start launches the background compaction goroutine.
func (m *Manager) Start() {
	go m.run()
}

// Stop gracefully shuts down the compaction goroutine.
func (m *Manager) Stop() {
	close(m.stopCh)
	<-m.doneCh
}

// AddL0File registers a new SSTable file in L0 (called after MemTable flush).
func (m *Manager) AddL0File(path string) {
	m.levels[0].mu.Lock()
	m.levels[0].files = append(m.levels[0].files, path)
	m.levels[0].mu.Unlock()
}

// GetAllFiles returns all SSTable paths across all levels (for GETs).
// Returns files in order: L0 newest first, then L1, L2.
func (m *Manager) GetAllFiles() []string {
	var all []string
	// L0: reverse order (newest first) for correct version ordering
	m.levels[0].mu.RLock()
	l0 := append([]string{}, m.levels[0].files...)
	m.levels[0].mu.RUnlock()
	for i := len(l0) - 1; i >= 0; i-- {
		all = append(all, l0[i])
	}
	// L1, L2: sorted order
	for _, lvl := range m.levels[1:] {
		lvl.mu.RLock()
		all = append(all, lvl.files...)
		lvl.mu.RUnlock()
	}
	return all
}

// BytesCompacted returns total bytes processed by compaction.
func (m *Manager) BytesCompacted() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.compacted
}

// ─── Background compaction loop ───────────────────────────────────────────────

func (m *Manager) run() {
	defer close(m.doneCh)
	ticker := time.NewTicker(CompactPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.maybeCompact()
		}
	}
}

func (m *Manager) maybeCompact() {
	// Check L0 → L1 compaction trigger
	m.levels[0].mu.RLock()
	l0Count := len(m.levels[0].files)
	m.levels[0].mu.RUnlock()

	if l0Count >= L0MaxFiles {
		if err := m.compactL0toL1(); err != nil {
			log.Printf("compaction: L0→L1 error: %v", err)
		}
	}

	// Check L1 → L2 compaction trigger
	l1Size := m.levelSize(1)
	if l1Size > L1SizeLimit {
		if err := m.compactL1toL2(); err != nil {
			log.Printf("compaction: L1→L2 error: %v", err)
		}
	}
}

// compactL0toL1 merges ALL L0 files into one new L1 file.
// This is the most critical compaction path (stops L0 from growing unbounded).
func (m *Manager) compactL0toL1() error {
	m.levels[0].mu.Lock()
	inputFiles := append([]string{}, m.levels[0].files...)
	m.levels[0].mu.Unlock()

	if len(inputFiles) == 0 {
		return nil
	}

	log.Printf("compaction: L0→L1 merging %d files...", len(inputFiles))

	// Also include all L1 files (to produce sorted, non-overlapping L1)
	m.levels[1].mu.RLock()
	l1Files := append([]string{}, m.levels[1].files...)
	m.levels[1].mu.RUnlock()

	allInput := append(inputFiles, l1Files...)
	merged, bytesIn, err := m.mergeFiles(allInput)
	if err != nil {
		return fmt.Errorf("compact L0→L1: merge: %w", err)
	}

	// Write merged result to a new L1 file
	outPath := filepath.Join(m.dataDir, fmt.Sprintf("l1_%d.sst", time.Now().UnixNano()))
	writer  := sstable.NewWriter(outPath)
	for _, e := range merged {
		writer.Add(sstable.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Tombstone: e.Tombstone,
			SeqNum:    e.SeqNum,
		})
	}
	bytesOut, err := writer.Flush()
	if err != nil {
		return fmt.Errorf("compact L0→L1: flush: %w", err)
	}

	// Atomically update level membership
	m.levels[0].mu.Lock()
	m.levels[1].mu.Lock()
	removed := append(inputFiles, l1Files...)
	m.levels[0].files = nil          // L0 is now empty
	m.levels[1].files = []string{outPath} // L1 has one merged file
	m.levels[0].mu.Unlock()
	m.levels[1].mu.Unlock()

	// Delete old input files
	for _, f := range removed {
		os.Remove(f)
	}

	m.mu.Lock()
	m.compacted += bytesIn
	m.mu.Unlock()

	if m.onCompact != nil {
		m.onCompact(removed, []string{outPath})
	}

	log.Printf("compaction: L0→L1 done: %d entries, %d→%d bytes, tombstones removed",
		len(merged), bytesIn, bytesOut)
	return nil
}

func (m *Manager) compactL1toL2() error {
	m.levels[1].mu.Lock()
	inputFiles := append([]string{}, m.levels[1].files...)
	m.levels[1].mu.Unlock()

	if len(inputFiles) == 0 {
		return nil
	}

	m.levels[2].mu.RLock()
	l2Files := append([]string{}, m.levels[2].files...)
	m.levels[2].mu.RUnlock()

	allInput := append(inputFiles, l2Files...)
	merged, bytesIn, err := m.mergeFiles(allInput)
	if err != nil {
		return fmt.Errorf("compact L1→L2: %w", err)
	}

	outPath := filepath.Join(m.dataDir, fmt.Sprintf("l2_%d.sst", time.Now().UnixNano()))
	writer  := sstable.NewWriter(outPath)
	for _, e := range merged {
		writer.Add(sstable.Entry{
			Key: e.Key, Value: e.Value,
			Tombstone: e.Tombstone, SeqNum: e.SeqNum,
		})
	}
	if _, err := writer.Flush(); err != nil {
		return fmt.Errorf("compact L1→L2 flush: %w", err)
	}

	m.levels[1].mu.Lock()
	m.levels[2].mu.Lock()
	removed := allInput
	m.levels[1].files = nil
	m.levels[2].files = []string{outPath}
	m.levels[1].mu.Unlock()
	m.levels[2].mu.Unlock()

	for _, f := range removed {
		os.Remove(f)
	}
	m.mu.Lock()
	m.compacted += bytesIn
	m.mu.Unlock()

	if m.onCompact != nil {
		m.onCompact(removed, []string{outPath})
	}
	return nil
}

// mergeFiles reads all entries from the given SSTable files,
// merges them (newest sequence wins), removes tombstones, sorts by key.
// Returns the merged entries and total input bytes.
func (m *Manager) mergeFiles(paths []string) ([]sstable.Entry, int64, error) {
	// key → latest entry (highest SeqNum wins)
	latest := make(map[string]sstable.Entry)
	var totalBytes int64

	for _, path := range paths {
		r, err := sstable.OpenReader(path)
		if err != nil {
			continue // skip missing/corrupt files
		}
		entries, err := r.Iterator()
		if err != nil {
			continue
		}
		totalBytes += r.Size()
		for _, e := range entries {
			k := string(e.Key)
			if existing, ok := latest[k]; !ok || e.SeqNum > existing.SeqNum {
				latest[k] = e
			}
		}
	}

	// Collect and sort, dropping tombstones (they've served their purpose)
	result := make([]sstable.Entry, 0, len(latest))
	for _, e := range latest {
		if !e.Tombstone { // tombstones removed during compaction
			result = append(result, e)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return compareBytes(result[i].Key, result[j].Key) < 0
	})
	return result, totalBytes, nil
}

func (m *Manager) levelSize(l int) int64 {
	m.levels[l].mu.RLock()
	defer m.levels[l].mu.RUnlock()
	var total int64
	for _, f := range m.levels[l].files {
		if stat, err := os.Stat(f); err == nil {
			total += stat.Size()
		}
	}
	return total
}

func compareBytes(a, b []byte) int {
	la, lb := len(a), len(b)
	min := la
	if lb < min { min = lb }
	for i := 0; i < min; i++ {
		if a[i] < b[i] { return -1 }
		if a[i] > b[i] { return 1 }
	}
	if la < lb { return -1 }
	if la > lb { return 1 }
	return 0
}
