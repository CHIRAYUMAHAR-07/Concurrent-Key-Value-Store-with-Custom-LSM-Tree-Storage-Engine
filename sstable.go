// internal/sstable/sstable.go
// SSTable: immutable sorted-string table on disk.
// Written by Go (with optional C++ encoder via cgo), read via memory-mapped I/O.
//
// File layout (little-endian):
//   [Magic: 8 bytes = 0x4C534D53535441B]
//   [Data Records: sorted KV pairs with CRC32 per record]
//   [Index Block: sparse index, every 16th key → byte offset]
//   [Bloom Filter: serialized C++ BloomFilter]
//   [Footer: 32 bytes — offsets + magic_end + CRC]

package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
)

const (
	MagicHeader  = uint64(0x4C534D53535441B)  // "LSMSSTAB"
	MagicFooter  = uint64(0x454E44464F4F5455) // "ENDFOOTU"
	IndexInterval = 16 // sparse index: every 16th key
)

// Entry is a decoded record from an SSTable.
type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	SeqNum    uint64
}

// IndexEntry maps a key to a byte offset in the data section.
type IndexEntry struct {
	Key    []byte
	Offset int64
}

// Reader reads from a single SSTable file.
type Reader struct {
	path        string
	data        []byte     // memory-mapped or read-all buffer
	dataOffset  int64
	dataLen     int64
	indexOffset int64
	bloomOffset int64
	index       []IndexEntry
	bloom       bloomReader // lightweight bit-array reader (Go, no cgo needed for reads)
	size        int64
	readCount   atomic.Int64 // stats
	bloomHits   atomic.Int64 // stats: how many reads bypassed by bloom
}

// bloomReader is a pure-Go bloom filter reader (avoids cgo during hot read path).
type bloomReader struct {
	bits      []byte
	numBits   uint64
	numHashes int
}

func (br *bloomReader) mayContain(key []byte) bool {
	if br.numBits == 0 {
		return true // no bloom filter loaded → conservative
	}
	h1 := murmurGo(key, 0xdeadbeef12345678)
	h2 := murmurGo(key, 0xcafebabe87654321)
	for i := 0; i < br.numHashes; i++ {
		bit := (h1 + uint64(i)*h2) % br.numBits
		if br.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false // definitely absent
		}
	}
	return true
}

// Writer builds and flushes an SSTable from sorted entries.
type Writer struct {
	path    string
	entries []Entry
}

// NewWriter creates a writer for the given file path.
func NewWriter(path string) *Writer {
	return &Writer{path: path}
}

// Add appends a sorted entry. Entries MUST be added in sorted key order.
func (w *Writer) Add(e Entry) {
	w.entries = append(w.entries, Entry{
		Key:       append([]byte{}, e.Key...),
		Value:     append([]byte{}, e.Value...),
		Tombstone: e.Tombstone,
		SeqNum:    e.SeqNum,
	})
}

// Flush writes all entries to disk and builds the Bloom filter + index.
// Returns the number of bytes written.
func (w *Writer) Flush() (int64, error) {
	if err := os.MkdirAll(filepath.Dir(w.path), 0755); err != nil {
		return 0, fmt.Errorf("sstable: mkdir: %w", err)
	}
	f, err := os.Create(w.path)
	if err != nil {
		return 0, fmt.Errorf("sstable: create %s: %w", w.path, err)
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 1<<20) // 1MB write buffer

	// ── Magic ──────────────────────────────────────────────────────────────────
	if err := writeU64(bw, MagicHeader); err != nil {
		return 0, err
	}

	// ── Build Bloom filter in-memory (pure Go) ────────────────────────────────
	bloom := buildBloom(w.entries)

	// ── Data section ──────────────────────────────────────────────────────────
	dataOffset := int64(8)
	var indexEntries []IndexEntry
	pos := dataOffset

	for i, e := range w.entries {
		if i%IndexInterval == 0 {
			indexEntries = append(indexEntries, IndexEntry{
				Key:    append([]byte{}, e.Key...),
				Offset: pos,
			})
		}
		n, err := writeEntry(bw, e)
		if err != nil {
			return 0, fmt.Errorf("sstable: write entry %d: %w", i, err)
		}
		pos += n
	}
	dataLen := pos - dataOffset

	// ── Index section ─────────────────────────────────────────────────────────
	indexOffset := pos
	if err := writeU64(bw, uint64(len(indexEntries))); err != nil {
		return 0, err
	}
	pos += 8
	for _, ie := range indexEntries {
		n, err := writeIndexEntry(bw, ie)
		if err != nil {
			return 0, err
		}
		pos += n
	}

	// ── Bloom filter section ──────────────────────────────────────────────────
	bloomOffset := pos
	bloomBytes  := serializeBloom(bloom)
	if err := writeU64(bw, uint64(len(bloomBytes))); err != nil {
		return 0, err
	}
	if _, err := bw.Write(bloomBytes); err != nil {
		return 0, err
	}
	pos += 8 + int64(len(bloomBytes))

	// ── Footer ────────────────────────────────────────────────────────────────
	footerBuf := make([]byte, 32)
	binary.LittleEndian.PutUint64(footerBuf[0:],  uint64(dataOffset))
	binary.LittleEndian.PutUint64(footerBuf[8:],  uint64(dataLen))
	binary.LittleEndian.PutUint64(footerBuf[16:], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footerBuf[24:], uint64(bloomOffset))
	// Overwrite last 12 bytes: magic_end (8) + crc (4)
	binary.LittleEndian.PutUint64(footerBuf[0:], MagicFooter)
	c := crc32.ChecksumIEEE(footerBuf[:28])
	binary.LittleEndian.PutUint32(footerBuf[28:], c)
	if _, err := bw.Write(footerBuf); err != nil {
		return 0, err
	}
	// Rewrite footer properly
	footer := make([]byte, 40) // 5×8 bytes
	binary.LittleEndian.PutUint64(footer[0:],  uint64(dataOffset))
	binary.LittleEndian.PutUint64(footer[8:],  uint64(dataLen))
	binary.LittleEndian.PutUint64(footer[16:], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[24:], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[32:], MagicFooter)
	footerCRC := crc32.ChecksumIEEE(footer[:36])
	footerFull := make([]byte, 40)
	copy(footerFull, footer)
	binary.LittleEndian.PutUint32(footerFull[36:], footerCRC)
	if _, err := bw.Write(footerFull); err != nil {
		return 0, err
	}

	if err := bw.Flush(); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil { // fsync → durable
		return 0, err
	}
	stat, _ := f.Stat()
	return stat.Size(), nil
}

// OpenReader opens an SSTable for reading.
func OpenReader(path string) (*Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: read %s: %w", path, err)
	}
	r := &Reader{path: path, data: data, size: int64(len(data))}
	if err := r.parseFooter(); err != nil {
		return nil, fmt.Errorf("sstable: parse footer %s: %w", path, err)
	}
	if err := r.loadIndex(); err != nil {
		return nil, fmt.Errorf("sstable: load index %s: %w", path, err)
	}
	if err := r.loadBloom(); err != nil {
		// Bloom load failure is non-fatal — fall back to always-probe
		r.bloom = bloomReader{}
	}
	return r, nil
}

// Get looks up a single key. Returns (entry, found, err).
// First checks Bloom filter, then binary-searches the sparse index,
// then linearly scans the block.
func (r *Reader) Get(key []byte) (Entry, bool, error) {
	r.readCount.Add(1)

	// ── Bloom filter check ────────────────────────────────────────────────────
	if !r.bloom.mayContain(key) {
		r.bloomHits.Add(1) // Bloom filter saved a disk read
		return Entry{}, false, nil
	}

	// ── Find block start via sparse index ─────────────────────────────────────
	blockStart := r.dataOffset
	for i := len(r.index) - 1; i >= 0; i-- {
		if compareBytes(r.index[i].Key, key) <= 0 {
			blockStart = r.index[i].Offset
			break
		}
	}

	// ── Linear scan from block start ──────────────────────────────────────────
	pos := int(blockStart)
	dataEnd := int(r.dataOffset + r.dataLen)

	for pos < dataEnd {
		e, n, err := readEntryAt(r.data, pos)
		if err != nil {
			return Entry{}, false, fmt.Errorf("sstable get: %w", err)
		}
		pos += n
		cmp := compareBytes(e.Key, key)
		if cmp == 0 {
			return e, true, nil
		}
		if cmp > 0 {
			break // gone past key → not found
		}
	}
	return Entry{}, false, nil
}

// Scan returns all entries in [start, end] key range.
func (r *Reader) Scan(start, end []byte) ([]Entry, error) {
	var entries []Entry
	blockStart := r.dataOffset
	if start != nil {
		for i := len(r.index) - 1; i >= 0; i-- {
			if compareBytes(r.index[i].Key, start) <= 0 {
				blockStart = r.index[i].Offset
				break
			}
		}
	}
	pos := int(blockStart)
	dataEnd := int(r.dataOffset + r.dataLen)
	for pos < dataEnd {
		e, n, err := readEntryAt(r.data, pos)
		if err != nil {
			return nil, err
		}
		pos += n
		if start != nil && compareBytes(e.Key, start) < 0 {
			continue
		}
		if end != nil && compareBytes(e.Key, end) > 0 {
			break
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Iterator returns all entries in sorted order.
func (r *Reader) Iterator() ([]Entry, error) {
	return r.Scan(nil, nil)
}

// Stats returns read statistics.
func (r *Reader) Stats() (reads, bloomHits int64) {
	return r.readCount.Load(), r.bloomHits.Load()
}

// Path returns the file path.
func (r *Reader) Path() string { return r.path }

// Size returns the file size.
func (r *Reader) Size() int64 { return r.size }

// ─── Footer + Index parsing ────────────────────────────────────────────────────

func (r *Reader) parseFooter() error {
	if len(r.data) < 40 {
		return fmt.Errorf("file too small (%d bytes)", len(r.data))
	}
	footer := r.data[len(r.data)-40:]
	r.dataOffset  = int64(binary.LittleEndian.Uint64(footer[0:]))
	r.dataLen     = int64(binary.LittleEndian.Uint64(footer[8:]))
	r.indexOffset = int64(binary.LittleEndian.Uint64(footer[16:]))
	r.bloomOffset = int64(binary.LittleEndian.Uint64(footer[24:]))
	return nil
}

func (r *Reader) loadIndex() error {
	if r.indexOffset >= int64(len(r.data)) {
		return nil
	}
	pos := int(r.indexOffset)
	if pos+8 > len(r.data) {
		return fmt.Errorf("index: out of bounds")
	}
	count := binary.LittleEndian.Uint64(r.data[pos:])
	pos += 8
	r.index = make([]IndexEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		if pos+4 > len(r.data) {
			break
		}
		kl := int(binary.LittleEndian.Uint32(r.data[pos:]))
		pos += 4
		if pos+kl+8 > len(r.data) {
			break
		}
		key := append([]byte{}, r.data[pos:pos+kl]...)
		pos += kl
		off := int64(binary.LittleEndian.Uint64(r.data[pos:]))
		pos += 8
		r.index = append(r.index, IndexEntry{Key: key, Offset: off})
	}
	// Ensure index is sorted (it should be, but defensive sort)
	sort.Slice(r.index, func(i, j int) bool {
		return compareBytes(r.index[i].Key, r.index[j].Key) < 0
	})
	return nil
}

func (r *Reader) loadBloom() error {
	if r.bloomOffset >= int64(len(r.data)) {
		return fmt.Errorf("bloom: offset out of bounds")
	}
	pos := int(r.bloomOffset)
	if pos+8 > len(r.data) {
		return fmt.Errorf("bloom: no length prefix")
	}
	bloomLen := int(binary.LittleEndian.Uint64(r.data[pos:]))
	pos += 8
	if bloomLen == 0 || pos+bloomLen > len(r.data) {
		return fmt.Errorf("bloom: invalid length %d", bloomLen)
	}
	bloomData := r.data[pos : pos+bloomLen]
	if len(bloomData) < 12 {
		return fmt.Errorf("bloom: data too short")
	}
	numBits  := binary.LittleEndian.Uint64(bloomData[0:])
	numHashes := int(binary.LittleEndian.Uint32(bloomData[8:]))
	r.bloom = bloomReader{
		bits:      bloomData[12:],
		numBits:   numBits,
		numHashes: numHashes,
	}
	return nil
}

// ─── Entry binary encoding ─────────────────────────────────────────────────────

// writeEntry encodes one Entry and returns bytes written.
func writeEntry(w io.Writer, e Entry) (int64, error) {
	// Build payload: [kl:4][key][vl:4][val][flags:1][seq:8]
	kl := uint32(len(e.Key))
	vl := uint32(len(e.Value))
	payload := make([]byte, 4+int(kl)+4+int(vl)+1+8)
	off := 0
	binary.LittleEndian.PutUint32(payload[off:], kl); off += 4
	copy(payload[off:], e.Key); off += int(kl)
	binary.LittleEndian.PutUint32(payload[off:], vl); off += 4
	copy(payload[off:], e.Value); off += int(vl)
	if e.Tombstone {
		payload[off] = 1
	}
	off++
	binary.LittleEndian.PutUint64(payload[off:], e.SeqNum)

	csum := crc32.ChecksumIEEE(payload)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], csum)

	n := int64(len(crcBuf) + len(payload))
	if _, err := w.Write(crcBuf[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}
	return n, nil
}

// readEntryAt decodes an Entry from buf at position pos.
// Returns (entry, bytes_consumed, error).
func readEntryAt(buf []byte, pos int) (Entry, int, error) {
	start := pos
	if pos+4 > len(buf) {
		return Entry{}, 0, io.EOF
	}
	storedCRC := binary.LittleEndian.Uint32(buf[pos:])
	pos += 4
	payloadStart := pos

	if pos+4 > len(buf) { return Entry{}, 0, io.ErrUnexpectedEOF }
	kl := int(binary.LittleEndian.Uint32(buf[pos:])); pos += 4
	if pos+kl > len(buf) { return Entry{}, 0, io.ErrUnexpectedEOF }
	key := append([]byte{}, buf[pos:pos+kl]...); pos += kl

	if pos+4 > len(buf) { return Entry{}, 0, io.ErrUnexpectedEOF }
	vl := int(binary.LittleEndian.Uint32(buf[pos:])); pos += 4
	if pos+vl > len(buf) { return Entry{}, 0, io.ErrUnexpectedEOF }
	value := append([]byte{}, buf[pos:pos+vl]...); pos += vl

	if pos+9 > len(buf) { return Entry{}, 0, io.ErrUnexpectedEOF }
	tombstone := buf[pos] != 0; pos++
	seq := binary.LittleEndian.Uint64(buf[pos:]); pos += 8

	// CRC check
	if crc32.ChecksumIEEE(buf[payloadStart:pos]) != storedCRC {
		return Entry{}, 0, fmt.Errorf("sstable: CRC mismatch at offset %d", start)
	}
	return Entry{Key: key, Value: value, Tombstone: tombstone, SeqNum: seq}, pos - start, nil
}

func writeIndexEntry(w io.Writer, ie IndexEntry) (int64, error) {
	kl := uint32(len(ie.Key))
	buf := make([]byte, 4+int(kl)+8)
	binary.LittleEndian.PutUint32(buf[0:], kl)
	copy(buf[4:], ie.Key)
	binary.LittleEndian.PutUint64(buf[4+int(kl):], uint64(ie.Offset))
	_, err := w.Write(buf)
	return int64(len(buf)), err
}

func writeU64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

// ─── Pure-Go Bloom filter builder ─────────────────────────────────────────────

type goBloom struct {
	bits      []byte
	numBits   uint64
	numHashes int
}

func buildBloom(entries []Entry) goBloom {
	n := len(entries)
	if n == 0 { n = 1 }
	// Optimal bits for 1% FPR: -n * ln(0.01) / (ln(2)^2) ≈ 9.59 * n
	numBits := uint64(float64(n) * 9.6)
	if numBits < 64 { numBits = 64 }
	numHashes := 10
	bits := make([]byte, (numBits+7)/8)
	for _, e := range entries {
		h1 := murmurGo(e.Key, 0xdeadbeef12345678)
		h2 := murmurGo(e.Key, 0xcafebabe87654321)
		for i := 0; i < numHashes; i++ {
			bit := (h1 + uint64(i)*h2) % numBits
			bits[bit/8] |= 1 << (bit % 8)
		}
	}
	return goBloom{bits: bits, numBits: numBits, numHashes: numHashes}
}

func serializeBloom(b goBloom) []byte {
	out := make([]byte, 12+len(b.bits))
	binary.LittleEndian.PutUint64(out[0:], b.numBits)
	binary.LittleEndian.PutUint32(out[8:], uint32(b.numHashes))
	copy(out[12:], b.bits)
	return out
}

// ─── MurmurHash3 (Go version for read-path hot loop) ─────────────────────────
func murmurGo(key []byte, seed uint64) uint64 {
	h := seed
	for i := 0; i+8 <= len(key); i += 8 {
		k := binary.LittleEndian.Uint64(key[i:])
		k *= 0xff51afd7ed558ccdULL
		k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53; k ^= k >> 33
		h ^= k; h = h<<27 | h>>37; h = h*5 + 0x52dce729
	}
	// tail
	var tail uint64
	rem := key[len(key)&^7:]
	for i := len(rem) - 1; i >= 0; i-- {
		tail = tail<<8 | uint64(rem[i])
	}
	if len(rem) > 0 {
		tail *= 0xff51afd7ed558ccd; tail ^= tail >> 33
		tail *= 0xc4ceb9fe1a85ec53; tail ^= tail >> 33
		h ^= tail
	}
	h ^= uint64(len(key))
	h ^= h >> 33; h *= 0xff51afd7ed558ccd; h ^= h >> 33
	return h
}

const (
	x0xff51afd7ed558ccdULL = uint64(0xff51afd7ed558ccd)
	x0xc4ceb9fe1a85ec53   = uint64(0xc4ceb9fe1a85ec53)
)

func compareBytes(a, b []byte) int {
	la, lb := len(a), len(b)
	min := la; if lb < min { min = lb }
	for i := 0; i < min; i++ {
		if a[i] < b[i] { return -1 }
		if a[i] > b[i] { return 1 }
	}
	if la < lb { return -1 }
	if la > lb { return 1 }
	return 0
}
