// internal/memtable/memtable_test.go
package memtable_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yourusername/kvstore/internal/memtable"
)

func TestMemTable_PutAndGet(t *testing.T) {
	m := memtable.New(1 << 20)
	m.Put([]byte("apple"), []byte("red"))
	m.Put([]byte("banana"), []byte("yellow"))
	m.Put([]byte("cherry"), []byte("red"))

	e, ok := m.Get([]byte("banana"))
	require.True(t, ok)
	assert.Equal(t, "yellow", string(e.Value))

	_, ok = m.Get([]byte("mango"))
	assert.False(t, ok)
}

func TestMemTable_Update(t *testing.T) {
	m := memtable.New(1 << 20)
	m.Put([]byte("k"), []byte("v1"))
	m.Put([]byte("k"), []byte("v2"))
	e, ok := m.Get([]byte("k"))
	require.True(t, ok)
	assert.Equal(t, "v2", string(e.Value))
}

func TestMemTable_Delete_Tombstone(t *testing.T) {
	m := memtable.New(1 << 20)
	m.Put([]byte("k"), []byte("v"))
	m.Delete([]byte("k"))
	e, ok := m.Get([]byte("k"))
	require.True(t, ok, "tombstone entry should still be found")
	assert.True(t, e.Tombstone)
}

func TestMemTable_SortedScan(t *testing.T) {
	m := memtable.New(1 << 20)
	keys := []string{"mango", "apple", "cherry", "banana", "date"}
	for _, k := range keys {
		m.Put([]byte(k), []byte(k+"-val"))
	}
	entries := m.Scan(nil, nil)
	require.Len(t, entries, 5)
	// Must be in sorted order
	for i := 1; i < len(entries); i++ {
		assert.LessOrEqual(t, string(entries[i-1].Key), string(entries[i].Key))
	}
}

func TestMemTable_ConcurrentReads(t *testing.T) {
	m := memtable.New(1 << 20)
	N := 1000
	for i := 0; i < N; i++ {
		m.Put([]byte(fmt.Sprintf("key:%06d", i)), []byte(fmt.Sprintf("val:%06d", i)))
	}

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i++ {
				key := []byte(fmt.Sprintf("key:%06d", i))
				_, _ = m.Get(key)
			}
		}()
	}
	wg.Wait() // no data races
}

func TestMemTable_SizeTracking(t *testing.T) {
	m := memtable.New(4 << 20)
	assert.Equal(t, int64(0), m.ApproximateSize())
	m.Put([]byte("k1"), []byte("v1"))
	assert.Greater(t, m.ApproximateSize(), int64(0))
}

func TestMemTable_FlushThreshold(t *testing.T) {
	// 1KB threshold
	m := memtable.New(1024)
	for i := 0; i < 100; i++ {
		m.Put([]byte(fmt.Sprintf("key:%06d", i)), make([]byte, 20))
	}
	assert.True(t, m.SizeShouldFlush(), "should need flush after filling past threshold")
}
