// internal/wal/wal_test.go
package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yourusername/kvstore/internal/wal"
)

func TestWAL_WriteAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := wal.Open(path)
	require.NoError(t, err)

	writes := []struct{ key, val string }{
		{"alpha", "value1"},
		{"beta",  "value2"},
		{"gamma", "value3"},
	}
	for i, kv := range writes {
		require.NoError(t, w.Append(uint64(i), []byte(kv.key), []byte(kv.val)))
	}
	require.NoError(t, w.Close())

	// Replay
	records, err := wal.Replay(path)
	require.NoError(t, err)
	assert.Len(t, records, 3)
	for i, rec := range records {
		assert.Equal(t, writes[i].key, string(rec.Key))
		assert.Equal(t, writes[i].val, string(rec.Value))
		assert.Equal(t, wal.RecordTypePut, rec.Type)
	}
}

func TestWAL_CRCDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")

	w, err := wal.Open(path)
	require.NoError(t, err)
	require.NoError(t, w.Append(1, []byte("key"), []byte("value")))
	require.NoError(t, w.Close())

	// Corrupt a byte in the middle of the file
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	if len(data) > 10 {
		data[10] ^= 0xFF // flip bits → CRC should detect this
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	records, err := wal.Replay(path)
	// Should recover gracefully (stop at corrupt record, no error)
	assert.NoError(t, err, "corruption should be silently stopped, not panicked")
	// Corrupted record should NOT appear
	assert.Len(t, records, 0, "corrupted record should be dropped")
}

func TestWAL_TombstoneRecords(t *testing.T) {
	dir  := t.TempDir()
	path := filepath.Join(dir, "delete.wal")
	w, err := wal.Open(path)
	require.NoError(t, err)
	require.NoError(t, w.Append(1,       []byte("k1"), []byte("v1")))
	require.NoError(t, w.AppendDelete(2, []byte("k1")))
	require.NoError(t, w.Close())

	records, err := wal.Replay(path)
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, wal.RecordTypePut,    records[0].Type)
	assert.Equal(t, wal.RecordTypeDelete, records[1].Type)
}

func TestWAL_MissingFileIsOK(t *testing.T) {
	records, err := wal.Replay("/nonexistent/path/test.wal")
	assert.NoError(t, err)
	assert.Empty(t, records)
}

func TestWAL_SizeTracking(t *testing.T) {
	dir  := t.TempDir()
	path := filepath.Join(dir, "size.wal")
	w, err := wal.Open(path)
	require.NoError(t, err)
	defer w.Close()

	for i := 0; i < 100; i++ {
		key := []byte("key")
		val := make([]byte, 64)
		require.NoError(t, w.Append(uint64(i), key, val))
	}
	assert.Greater(t, w.Size(), int64(0))
}
