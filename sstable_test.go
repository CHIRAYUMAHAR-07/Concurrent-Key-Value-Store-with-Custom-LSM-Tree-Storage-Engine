// internal/sstable/sstable_test.go
package sstable_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yourusername/kvstore/internal/sstable"
)

func TestSSTable_WriteAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "sst_test_*.sst")
	require.NoError(t, err)
	f.Close()
	defer os.Remove(f.Name())

	// Write
	w := sstable.NewWriter(f.Name())
	entries := []sstable.Entry{
		{Key: []byte("apple"),  Value: []byte("red"),    SeqNum: 1},
		{Key: []byte("banana"), Value: []byte("yellow"), SeqNum: 2},
		{Key: []byte("cherry"), Value: []byte("dark"),   SeqNum: 3},
	}
	for _, e := range entries {
		w.Add(e)
	}
	size, err := w.Flush()
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))

	// Read
	r, err := sstable.OpenReader(f.Name())
	require.NoError(t, err)

	for _, want := range entries {
		got, found, err := r.Get(want.Key)
		require.NoError(t, err)
		require.True(t, found, "key %s not found", string(want.Key))
		assert.Equal(t, string(want.Value), string(got.Value))
		assert.Equal(t, want.SeqNum, got.SeqNum)
	}

	// Negative lookup
	_, found, err := r.Get([]byte("mango"))
	require.NoError(t, err)
	assert.False(t, found, "mango should not be found")
}

func TestSSTable_BloomFilter_ReducesDiskReads(t *testing.T) {
	f, _ := os.CreateTemp("", "sst_bloom_*.sst")
	f.Close()
	defer os.Remove(f.Name())

	// Write 1000 keys
	w := sstable.NewWriter(f.Name())
	for i := 0; i < 1000; i++ {
		w.Add(sstable.Entry{
			Key:   []byte(fmt.Sprintf("exists:key:%08d", i)),
			Value: []byte("value"),
			SeqNum: uint64(i),
		})
	}
	_, err := w.Flush()
	require.NoError(t, err)

	r, err := sstable.OpenReader(f.Name())
	require.NoError(t, err)

	// 1M negative lookups
	blocked := 0
	total   := 10_000 // reduced for unit test speed
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("nonexistent:%016d", i))
		_, found, err := r.Get(key)
		require.NoError(t, err)
		if !found {
			// Count how many were blocked by bloom vs scanned
		}
		_ = found
		blocked++ // in practice all should be blocked by bloom
	}

	_, bloomHits := r.Stats()
	pct := float64(bloomHits) / float64(total) * 100
	t.Logf("Bloom filter blocked %.1f%% of negative lookups (target: ≥90%%)", pct)
	assert.GreaterOrEqual(t, pct, 90.0,
		"Bloom filter should block ≥90%% of negative lookups (got %.1f%%)", pct)
}

func TestSSTable_Tombstone(t *testing.T) {
	f, _ := os.CreateTemp("", "sst_tomb_*.sst")
	f.Close()
	defer os.Remove(f.Name())

	w := sstable.NewWriter(f.Name())
	w.Add(sstable.Entry{Key: []byte("k"), Value: nil, Tombstone: true, SeqNum: 5})
	_, err := w.Flush()
	require.NoError(t, err)

	r, err := sstable.OpenReader(f.Name())
	require.NoError(t, err)

	e, found, err := r.Get([]byte("k"))
	require.NoError(t, err)
	require.True(t, found, "tombstone should be found in SSTable")
	assert.True(t, e.Tombstone)
}

func TestSSTable_ScanRange(t *testing.T) {
	f, _ := os.CreateTemp("", "sst_scan_*.sst")
	f.Close()
	defer os.Remove(f.Name())

	w := sstable.NewWriter(f.Name())
	for i := 0; i < 100; i++ {
		w.Add(sstable.Entry{
			Key:   []byte(fmt.Sprintf("key:%03d", i)),
			Value: []byte("v"),
			SeqNum: uint64(i),
		})
	}
	w.Flush()

	r, err := sstable.OpenReader(f.Name())
	require.NoError(t, err)

	entries, err := r.Scan([]byte("key:010"), []byte("key:019"))
	require.NoError(t, err)
	assert.Len(t, entries, 10, "scan should return keys 010–019")
}

func TestSSTable_LargeFile(t *testing.T) {
	if testing.Short() { t.Skip() }
	f, _ := os.CreateTemp("", "sst_large_*.sst")
	f.Close()
	defer os.Remove(f.Name())

	N := 50_000
	w := sstable.NewWriter(f.Name())
	for i := 0; i < N; i++ {
		w.Add(sstable.Entry{
			Key:   []byte(fmt.Sprintf("key:%010d", i)),
			Value: make([]byte, 64),
			SeqNum: uint64(i),
		})
	}
	size, err := w.Flush()
	require.NoError(t, err)
	t.Logf("50K entries: %.1f MB", float64(size)/(1<<20))

	r, err := sstable.OpenReader(f.Name())
	require.NoError(t, err)

	// Spot check
	for _, i := range []int{0, 1000, 25000, 49999} {
		key := []byte(fmt.Sprintf("key:%010d", i))
		_, found, err := r.Get(key)
		require.NoError(t, err)
		assert.True(t, found, "key %s not found in large SSTable", string(key))
	}
}
