// internal/memtable/memtable.go
// MemTable: in-memory sorted key-value store backed by a skip list.
// - Concurrent readers (sync.RWMutex.RLock)
// - Single writer (sync.RWMutex.Lock)
// - Flushed to an immutable SSTable when size exceeds threshold.
// - Tombstones: deleted keys are marked, not removed (actual deletion at compaction).

package memtable

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

const (
	// DefaultMaxSize: flush to SSTable when MemTable exceeds this (4MB)
	DefaultMaxSize = 4 * 1024 * 1024

	skipListMaxLevel = 12   // max number of forward pointers per node
	skipListP        = 0.25 // probability of adding a level
)

// Entry is a key-value record with metadata.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool   // true = this key was deleted
	SeqNum    uint64 // monotonically increasing sequence number
}

// skipNode is a node in the skip list.
type skipNode struct {
	entry   Entry
	forward []*skipNode
}

// MemTable is a concurrent, sorted in-memory key-value table.
type MemTable struct {
	mu      sync.RWMutex
	head    *skipNode   // sentinel head (no data, max-level forward pointers)
	level   int         // current max level in use
	size    int64       // approximate byte size (atomic)
	count   int64       // number of live entries (atomic)
	maxSize int64       // flush threshold
	seqGen  atomic.Uint64 // sequence number generator
}

// New creates a new MemTable with the given max size in bytes.
func New(maxSizeBytes int64) *MemTable {
	if maxSizeBytes <= 0 {
		maxSizeBytes = DefaultMaxSize
	}
	head := &skipNode{forward: make([]*skipNode, skipListMaxLevel)}
	return &MemTable{
		head:    head,
		level:   1,
		maxSize: maxSizeBytes,
	}
}

// Put inserts or updates a key. Returns the sequence number assigned.
func (m *MemTable) Put(key, value []byte) uint64 {
	seq := m.seqGen.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.put(key, value, false, seq)
	return seq
}

// Delete inserts a tombstone for key (logical delete).
func (m *MemTable) Delete(key []byte) uint64 {
	seq := m.seqGen.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.put(key, nil, true, seq)
	return seq
}

// Get looks up a key. Returns (entry, found).
// Thread-safe for concurrent reads.
func (m *MemTable) Get(key []byte) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node := m.findNode(key)
	if node == nil {
		return Entry{}, false
	}
	// Return a copy so callers can read safely after RUnlock
	e := Entry{
		Key:       append([]byte{}, node.entry.Key...),
		Value:     append([]byte{}, node.entry.Value...),
		Tombstone: node.entry.Tombstone,
		SeqNum:    node.entry.SeqNum,
	}
	return e, true
}

// Scan returns all entries in [startKey, endKey] order (inclusive).
// endKey=nil means scan to the end.
func (m *MemTable) Scan(startKey, endKey []byte) []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []Entry
	cur := m.head.forward[0]
	started := (startKey == nil)

	for cur != nil {
		cmp := compareBytes(cur.entry.Key, startKey)
		if !started && cmp < 0 {
			cur = cur.forward[0]
			continue
		}
		started = true
		if endKey != nil && compareBytes(cur.entry.Key, endKey) > 0 {
			break
		}
		entries = append(entries, Entry{
			Key:       append([]byte{}, cur.entry.Key...),
			Value:     append([]byte{}, cur.entry.Value...),
			Tombstone: cur.entry.Tombstone,
			SeqNum:    cur.entry.SeqNum,
		})
		cur = cur.forward[0]
	}
	return entries
}

// Snapshot returns all entries sorted by key (for SSTable flush).
func (m *MemTable) Snapshot() []Entry {
	return m.Scan(nil, nil)
}

// SizeShouldFlush returns true when the MemTable exceeds its size threshold.
func (m *MemTable) SizeShouldFlush() bool {
	return atomic.LoadInt64(&m.size) >= m.maxSize
}

// ApproximateSize returns the estimated byte size.
func (m *MemTable) ApproximateSize() int64 {
	return atomic.LoadInt64(&m.size)
}

// Count returns the number of entries (including tombstones).
func (m *MemTable) Count() int64 {
	return atomic.LoadInt64(&m.count)
}

// ─── Internal skip list operations ────────────────────────────────────────────

func (m *MemTable) put(key, value []byte, tombstone bool, seq uint64) {
	update := make([]*skipNode, skipListMaxLevel)
	cur := m.head
	for i := m.level - 1; i >= 0; i-- {
		for cur.forward[i] != nil && compareBytes(cur.forward[i].entry.Key, key) < 0 {
			cur = cur.forward[i]
		}
		update[i] = cur
	}
	// If key already exists at level 0, update it in place
	if next := update[0].forward[0]; next != nil && compareBytes(next.entry.Key, key) == 0 {
		oldSize := int64(len(next.entry.Key) + len(next.entry.Value) + 32)
		next.entry.Value     = append([]byte{}, value...)
		next.entry.Tombstone = tombstone
		next.entry.SeqNum    = seq
		newSize := int64(len(next.entry.Key) + len(next.entry.Value) + 32)
		atomic.AddInt64(&m.size, newSize-oldSize)
		return
	}
	// New node: pick a random level
	newLevel := m.randomLevel()
	if newLevel > m.level {
		for i := m.level; i < newLevel; i++ {
			update[i] = m.head
		}
		m.level = newLevel
	}
	node := &skipNode{
		entry: Entry{
			Key:       append([]byte{}, key...),
			Value:     append([]byte{}, value...),
			Tombstone: tombstone,
			SeqNum:    seq,
		},
		forward: make([]*skipNode, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		node.forward[i]    = update[i].forward[i]
		update[i].forward[i] = node
	}
	nodeSize := int64(len(key) + len(value) + 32 + newLevel*8)
	atomic.AddInt64(&m.size, nodeSize)
	atomic.AddInt64(&m.count, 1)
}

func (m *MemTable) findNode(key []byte) *skipNode {
	cur := m.head
	for i := m.level - 1; i >= 0; i-- {
		for cur.forward[i] != nil && compareBytes(cur.forward[i].entry.Key, key) < 0 {
			cur = cur.forward[i]
		}
	}
	if next := cur.forward[0]; next != nil && compareBytes(next.entry.Key, key) == 0 {
		return next
	}
	return nil
}

func (m *MemTable) randomLevel() int {
	level := 1
	for level < skipListMaxLevel && rand.Float64() < skipListP {
		level++
	}
	return level
}

func compareBytes(a, b []byte) int {
	la, lb := len(a), len(b)
	min := la
	if lb < min {
		min = lb
	}
	for i := 0; i < min; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}
