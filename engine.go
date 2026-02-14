// internal/engine/engine.go
// LSMEngine: the core storage engine orchestrating all components.
//
// Write path:
//   1. Write to WAL (fsync) → durability guaranteed
//   2. Write to active MemTable → in-memory visibility
//   3. If MemTable full → flush to SSTable (L0) + rotate WAL
//
// Read path:
//   1. Check active MemTable
//   2. Check immutable MemTable (if flush in progress)
//   3. Check L0 SSTables (newest first) with Bloom filters
//   4. Check L1, L2 SSTables

package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourusername/kvstore/internal/compaction"
	"github.com/yourusername/kvstore/internal/memtable"
	"github.com/yourusername/kvstore/internal/sstable"
	"github.com/yourusername/kvstore/internal/wal"
)

// Config holds engine configuration.
type Config struct {
	DataDir      string
	MemTableSize int64 // default 4MB
	WALSync      bool  // fsync on every write (default true)
}

// Stats holds engine performance counters.
type Stats struct {
	Writes       atomic.Int64
	Reads        atomic.Int64
	Deletes      atomic.Int64
	MemHits      atomic.Int64
	SSTHits      atomic.Int64
	BloomMisses  atomic.Int64 // reads blocked by bloom (saved disk I/O)
	FlushCount   atomic.Int64
	BytesWritten atomic.Int64
}

// Engine is the main LSM-Tree storage engine.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	active  *memtable.MemTable   // mutable MemTable (current writes)
	frozen  *memtable.MemTable   // immutable MemTable being flushed (may be nil)
	wal     *wal.WAL
	compact *compaction.Manager

	// SSTable readers (L0 newest→oldest, then L1, L2)
	readers   []*sstable.Reader
	readersMu sync.RWMutex

	stats  Stats
	closed atomic.Bool
}

// Open opens or creates an LSM engine at the given data directory.
// Performs crash recovery by replaying the WAL if it exists.
func Open(cfg Config) (*Engine, error) {
	if cfg.MemTableSize <= 0 {
		cfg.MemTableSize = memtable.DefaultMaxSize
	}
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("engine: mkdir %s: %w", cfg.DataDir, err)
	}

	e := &Engine{cfg: cfg}
	e.active = memtable.New(cfg.MemTableSize)

	// ── WAL open/recovery ──────────────────────────────────────────────────────
	walPath := filepath.Join(cfg.DataDir, "current.wal")
	w, err  := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("engine: wal open: %w", err)
	}
	e.wal = w

	// Replay WAL → rebuild MemTable (crash recovery)
	records, err := wal.Replay(walPath)
	if err != nil {
		log.Printf("engine: WAL replay error (partial): %v", err)
	}
	if len(records) > 0 {
		log.Printf("engine: replaying %d WAL records (crash recovery)", len(records))
		for _, rec := range records {
			switch rec.Type {
			case wal.RecordTypePut:
				e.active.Put(rec.Key, rec.Value)
			case wal.RecordTypeDelete:
				e.active.Delete(rec.Key)
			}
		}
		log.Printf("engine: crash recovery complete — %d records replayed", len(records))
	}

	// ── Load existing SSTables ─────────────────────────────────────────────────
	e.compact = compaction.NewManager(cfg.DataDir, e.onCompaction)
	if err := e.loadSSTables(); err != nil {
		log.Printf("engine: load SSTables: %v", err)
	}
	e.compact.Start()

	log.Printf("engine: opened at %s (MemTable=%dMB, WAL=%s)",
		cfg.DataDir, cfg.MemTableSize>>20, walPath)
	return e, nil
}

// Put stores a key-value pair. Durable immediately (WAL fsync).
func (e *Engine) Put(key, value []byte) error {
	if e.closed.Load() {
		return fmt.Errorf("engine: closed")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Write to WAL (fsync — durable before touching MemTable)
	seq := uint64(time.Now().UnixNano())
	if err := e.wal.Append(seq, key, value); err != nil {
		return fmt.Errorf("engine: wal append: %w", err)
	}

	// 2. Write to MemTable
	e.active.Put(key, value)

	e.stats.Writes.Add(1)
	e.stats.BytesWritten.Add(int64(len(key) + len(value)))

	// 3. Check if flush needed (non-blocking: fires in background goroutine)
	if e.active.SizeShouldFlush() {
		go e.flushMemTable()
	}
	return nil
}

// Get retrieves a key. Returns (value, found, error).
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	if e.closed.Load() {
		return nil, false, fmt.Errorf("engine: closed")
	}
	e.stats.Reads.Add(1)
	e.mu.RLock()
	active  := e.active
	frozen  := e.frozen
	e.mu.RUnlock()

	// 1. Active MemTable (most recent writes)
	if entry, ok := active.Get(key); ok {
		e.stats.MemHits.Add(1)
		if entry.Tombstone {
			return nil, false, nil // deleted
		}
		return entry.Value, true, nil
	}

	// 2. Frozen MemTable (currently being flushed)
	if frozen != nil {
		if entry, ok := frozen.Get(key); ok {
			e.stats.MemHits.Add(1)
			if entry.Tombstone {
				return nil, false, nil
			}
			return entry.Value, true, nil
		}
	}

	// 3. SSTables (newest L0 first, then L1, L2)
	e.readersMu.RLock()
	readers := append([]*sstable.Reader{}, e.readers...)
	e.readersMu.RUnlock()

	for _, r := range readers {
		entry, found, err := r.Get(key)
		if err != nil {
			log.Printf("engine: get error in %s: %v", r.Path(), err)
			continue
		}
		if found {
			e.stats.SSTHits.Add(1)
			if entry.Tombstone {
				return nil, false, nil
			}
			return entry.Value, true, nil
		}
	}
	return nil, false, nil
}

// Delete logically removes a key by writing a tombstone.
func (e *Engine) Delete(key []byte) error {
	if e.closed.Load() {
		return fmt.Errorf("engine: closed")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := uint64(time.Now().UnixNano())
	if err := e.wal.AppendDelete(seq, key); err != nil {
		return fmt.Errorf("engine: wal delete: %w", err)
	}
	e.active.Delete(key)
	e.stats.Deletes.Add(1)

	if e.active.SizeShouldFlush() {
		go e.flushMemTable()
	}
	return nil
}

// Scan returns all keys in [startKey, endKey] range across all levels.
func (e *Engine) Scan(startKey, endKey []byte) ([]sstable.Entry, error) {
	e.mu.RLock()
	active := e.active
	frozen := e.frozen
	e.mu.RUnlock()

	// Merge all sources
	seen := make(map[string]sstable.Entry)

	// MemTables
	for _, mt := range []*memtable.MemTable{active, frozen} {
		if mt == nil { continue }
		for _, me := range mt.Scan(startKey, endKey) {
			k := string(me.Key)
			if existing, ok := seen[k]; !ok || me.SeqNum > existing.SeqNum {
				seen[k] = sstable.Entry{
					Key: me.Key, Value: me.Value,
					Tombstone: me.Tombstone, SeqNum: me.SeqNum,
				}
			}
		}
	}

	// SSTables
	e.readersMu.RLock()
	readers := append([]*sstable.Reader{}, e.readers...)
	e.readersMu.RUnlock()

	for _, r := range readers {
		entries, err := r.Scan(startKey, endKey)
		if err != nil { continue }
		for _, se := range entries {
			k := string(se.Key)
			if existing, ok := seen[k]; !ok || se.SeqNum > existing.SeqNum {
				seen[k] = se
			}
		}
	}

	// Collect, sort, filter tombstones
	result := make([]sstable.Entry, 0, len(seen))
	for _, e := range seen {
		if !e.Tombstone {
			result = append(result, e)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return compareBytes(result[i].Key, result[j].Key) < 0
	})
	return result, nil
}

// Stats returns a snapshot of engine statistics.
func (e *Engine) GetStats() map[string]int64 {
	reads, bloomHits := int64(0), int64(0)
	e.readersMu.RLock()
	for _, r := range e.readers {
		rc, bh := r.Stats()
		reads += rc; bloomHits += bh
	}
	e.readersMu.RUnlock()

	return map[string]int64{
		"writes":        e.stats.Writes.Load(),
		"reads":         e.stats.Reads.Load(),
		"deletes":       e.stats.Deletes.Load(),
		"mem_hits":      e.stats.MemHits.Load(),
		"sst_hits":      e.stats.SSTHits.Load(),
		"bloom_misses":  bloomHits,
		"flush_count":   e.stats.FlushCount.Load(),
		"bytes_written": e.stats.BytesWritten.Load(),
		"bytes_compacted": e.compact.BytesCompacted(),
		"memtable_size": e.active.ApproximateSize(),
		"sstable_count": int64(len(e.readers)),
	}
}

// Close gracefully shuts down the engine.
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Flush any remaining MemTable data
	e.mu.Lock()
	if e.active.Count() > 0 {
		e.mu.Unlock()
		e.flushMemTable()
	} else {
		e.mu.Unlock()
	}
	e.compact.Stop()
	if err := e.wal.Close(); err != nil {
		log.Printf("engine: WAL close: %v", err)
	}
	log.Println("engine: closed cleanly")
	return nil
}

// ─── MemTable flush ────────────────────────────────────────────────────────────

func (e *Engine) flushMemTable() {
	e.mu.Lock()
	if e.frozen != nil {
		e.mu.Unlock()
		return // another flush already in progress
	}
	// Freeze the active MemTable, create a fresh one
	toFlush := e.active
	e.frozen = toFlush
	e.active = memtable.New(e.cfg.MemTableSize)
	e.mu.Unlock()

	// Write frozen MemTable to SSTable (background, no mutex held)
	entries := toFlush.Snapshot()
	if len(entries) == 0 {
		e.mu.Lock(); e.frozen = nil; e.mu.Unlock()
		return
	}

	sstPath := filepath.Join(e.cfg.DataDir,
		fmt.Sprintf("l0_%d.sst", time.Now().UnixNano()))
	writer := sstable.NewWriter(sstPath)
	for _, me := range entries {
		writer.Add(sstable.Entry{
			Key:       me.Key,
			Value:     me.Value,
			Tombstone: me.Tombstone,
			SeqNum:    me.SeqNum,
		})
	}
	size, err := writer.Flush()
	if err != nil {
		log.Printf("engine: flush error: %v", err)
		e.mu.Lock(); e.frozen = nil; e.mu.Unlock()
		return
	}

	// Write WAL checkpoint and rotate
	_ = e.wal.AppendCheckpoint(uint64(time.Now().UnixNano()))

	// Open the new SSTable for reads
	reader, err := sstable.OpenReader(sstPath)
	if err != nil {
		log.Printf("engine: open flushed SSTable: %v", err)
	} else {
		e.readersMu.Lock()
		// Prepend (L0 newest first)
		e.readers = append([]*sstable.Reader{reader}, e.readers...)
		e.readersMu.Unlock()
	}

	// Register with compaction manager
	e.compact.AddL0File(sstPath)

	e.stats.FlushCount.Add(1)
	log.Printf("engine: flushed MemTable → %s (%d entries, %d bytes)",
		sstPath, len(entries), size)

	// Clear frozen MemTable
	e.mu.Lock()
	e.frozen = nil
	e.mu.Unlock()
}

// ─── SSTable loading on startup ───────────────────────────────────────────────

func (e *Engine) loadSSTables() error {
	patterns := []string{
		filepath.Join(e.cfg.DataDir, "l0_*.sst"),
		filepath.Join(e.cfg.DataDir, "l1_*.sst"),
		filepath.Join(e.cfg.DataDir, "l2_*.sst"),
	}
	var allFiles []string
	for _, p := range patterns {
		files, _ := filepath.Glob(p)
		allFiles = append(allFiles, files...)
	}
	// Sort by name (timestamp-encoded = newest last)
	sort.Strings(allFiles)

	for _, f := range allFiles {
		r, err := sstable.OpenReader(f)
		if err != nil {
			log.Printf("engine: skip corrupt SSTable %s: %v", f, err)
			continue
		}
		e.readers = append([]*sstable.Reader{r}, e.readers...) // newest first
		log.Printf("engine: loaded SSTable %s (%d bytes)", f, r.Size())
	}
	return nil
}

// onCompaction is called by the compaction manager after a compaction.
func (e *Engine) onCompaction(removed, added []string) {
	removedSet := make(map[string]bool)
	for _, f := range removed {
		removedSet[f] = true
	}
	e.readersMu.Lock()
	// Remove old readers
	newReaders := make([]*sstable.Reader, 0, len(e.readers))
	for _, r := range e.readers {
		if !removedSet[r.Path()] {
			newReaders = append(newReaders, r)
		}
	}
	// Add new readers (added files)
	for _, f := range added {
		if r, err := sstable.OpenReader(f); err == nil {
			newReaders = append(newReaders, r)
		}
	}
	e.readers = newReaders
	e.readersMu.Unlock()
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
