package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"sync"
)

// ======================================================================
// Bloom Filter Implementation
// ======================================================================

// BloomFilter represents a simple Bloom filter.
type BloomFilter struct {
	m      uint64 // number of bits
	k      uint32 // number of hash functions
	bitset []byte // underlying bitset
	n      uint64 // number of items added
}

func optimalBloomParams(expectedItems int, falsePositiveRate float64) (m uint64, k uint32, mBytes uint64) {
	if expectedItems <= 0 {
		panic("expectedItems must be > 0")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		panic("falsePositiveRate must be between 0 and 1 (non-inclusive)")
	}
	n := float64(expectedItems)
	// Compute optimal number of bits (m) using:
	//   m = - (n * ln(p)) / (ln2)^2
	mFloat := -n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
	// Convert m to a full number of bytes.
	mBytes = uint64(math.Ceil(mFloat / 8))
	m = mBytes * 8
	// Compute the optimal number of hash functions:
	//   k = (m/n) * ln2
	k = uint32(math.Ceil((float64(m) / n) * math.Ln2))
	return
}

// NewBloomFilter calculates an optimum bit–array size (m) and number of hash functions (k)
// using the formulas:
//
//	m = - (n * ln(p)) / (ln2)^2
//	k = (m/n) * ln2
//
// It rounds m up to a whole number of bytes.
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	m, k, mBytes := optimalBloomParams(expectedItems, falsePositiveRate)
	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, mBytes),
		n:      0,
	}
}

// Add inserts a key (as a byte slice) into the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < bf.k; i++ {
		combined := h1 + uint64(i)*h2
		bit := combined % bf.m
		byteIndex := bit / 8
		bitIndex := bit % 8
		bf.bitset[byteIndex] |= 1 << bitIndex
	}
	bf.n++
}

// MightContain returns true if the key might be in the filter (or false if definitely not).
func (bf *BloomFilter) MightContain(key []byte) bool {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < bf.k; i++ {
		combined := h1 + uint64(i)*h2
		bit := combined % bf.m
		byteIndex := bit / 8
		bitIndex := bit % 8
		if (bf.bitset[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	return true
}

// hashKey returns two 64–bit hash values for the given key using FNV–1a.
// (The second hash is computed on the reversed key.)
func hashKey(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)
	sum1 := h1.Sum64()
	h2 := fnv.New64a()
	revKey := make([]byte, len(key))
	for i, b := range key {
		revKey[len(key)-1-i] = b
	}
	h2.Write(revKey)
	sum2 := h2.Sum64()
	return sum1, sum2
}

// ======================================================================
// SSTable Implementation
// ======================================================================

// Our file format is laid out as follows:
//
//   [Header "SST1" (4 bytes)]
//   [Data Region: a series of encoded rows]
//   [Index Region: a uint32 count followed by for each entry:
//         uint32 key length, key bytes, int64 file offset]
//   [Bloom Filter Region: serialized BloomFilter fields:
//         uint64 m, uint32 k, uint64 n,
//         uint32 bitset length, then bitset bytes]
//   [Footer: int64 indexOffset, int64 bfOffset, 4 bytes magic "SSTF"]
//
// Each row is encoded as:
//     [key: uint32 length + key bytes]
//     [number of columns: uint32]
//     For each column:
//         [column name: uint32 length + bytes]
//         [column value: uint32 length + bytes]

const (
	headerMagic = "SST1"
	footerMagic = "SSTF"
)

// IndexEntry represents one sparse index entry.
type IndexEntry struct {
	Key    string
	Offset int64
}

// Row represents a wide column row.
type Row struct {
	Key     string
	Columns map[string]string
}

// SSTable holds in–memory metadata for a single SSTable file.
type SSTable struct {
	filePath    string
	index       []IndexEntry
	bloom       *BloomFilter
	dataOffset  int64 // start of data region (immediately after header)
	indexOffset int64 // where index region starts
	bfOffset    int64 // where Bloom filter region starts
	bufPool     sync.Pool
}

// WriteSSTable writes a new SSTable file containing the provided rows.
// The rows must be sorted by key. The sparseInterval determines how many rows
// between index entries. expectedItems and falsePositiveRate are used to size the Bloom filter.
func WriteSSTable(filePath string, rows []Row, sparseInterval int, expectedItems int, falsePositiveRate float64) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header.
	if _, err := f.Write([]byte(headerMagic)); err != nil {
		return err
	}
	dataStart := int64(len(headerMagic))

	// Prepare the Bloom filter.
	bf := NewBloomFilter(expectedItems, falsePositiveRate)

	// Write data region.
	indexEntries := []IndexEntry{}
	for i, row := range rows {
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// Record an index entry every sparseInterval rows.
		if i%sparseInterval == 0 {
			indexEntries = append(indexEntries, IndexEntry{
				Key:    row.Key,
				Offset: offset,
			})
		}
		// Add key to Bloom filter.
		bf.Add([]byte(row.Key))
		// Write the encoded row.
		if err := writeRow(f, row); err != nil {
			return err
		}
	}

	// Mark where the index region begins.
	indexOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// Write index region.
	if err := binary.Write(f, binary.LittleEndian, uint32(len(indexEntries))); err != nil {
		return err
	}
	for _, entry := range indexEntries {
		if err := writeString(f, entry.Key); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.Offset); err != nil {
			return err
		}
	}

	// Mark where the Bloom filter region begins.
	bfOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// Write Bloom filter region.
	if err := binary.Write(f, binary.LittleEndian, bf.m); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, bf.k); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, bf.n); err != nil {
		return err
	}
	// Write the length of the bitset and then the bitset bytes.
	bitsetLen := uint32(len(bf.bitset))
	if err := binary.Write(f, binary.LittleEndian, bitsetLen); err != nil {
		return err
	}
	if _, err := f.Write(bf.bitset); err != nil {
		return err
	}

	// Write footer: indexOffset, bfOffset, and footer magic.
	if err := binary.Write(f, binary.LittleEndian, indexOffset); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, bfOffset); err != nil {
		return err
	}
	if _, err := f.Write([]byte(footerMagic)); err != nil {
		return err
	}
	// For debugging, you might print out the file layout offsets.
	_ = dataStart
	return nil
}

// writeRow encodes a Row and writes it to w.
func writeRow(w io.Writer, row Row) error {
	// Write the row key.
	if err := writeString(w, row.Key); err != nil {
		return err
	}
	// Write the number of columns.
	if err := binary.Write(w, binary.LittleEndian, uint32(len(row.Columns))); err != nil {
		return err
	}
	// Write each column (name and value).
	for col, val := range row.Columns {
		if err := writeString(w, col); err != nil {
			return err
		}
		if err := writeString(w, val); err != nil {
			return err
		}
	}
	return nil
}

// writeString writes a uint32 length followed by the string bytes.
func writeString(w io.Writer, s string) error {
	strBytes := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(strBytes))); err != nil {
		return err
	}
	if _, err := w.Write(strBytes); err != nil {
		return err
	}
	return nil
}

// readString reads a length-prefixed string from r.
func readString(r io.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// readRow decodes and returns a Row from r.
func readRow(r io.Reader) (Row, error) {
	key, err := readString(r)
	if err != nil {
		return Row{}, err
	}
	var numCols uint32
	if err := binary.Read(r, binary.LittleEndian, &numCols); err != nil {
		return Row{}, err
	}
	cols := make(map[string]string, numCols)
	for i := uint32(0); i < numCols; i++ {
		colName, err := readString(r)
		if err != nil {
			return Row{}, err
		}
		colVal, err := readString(r)
		if err != nil {
			return Row{}, err
		}
		cols[colName] = colVal
	}
	return Row{
		Key:     key,
		Columns: cols,
	}, nil
}

// LoadSSTable reads an SSTable file and reconstructs its in–memory index and Bloom filter.
func LoadSSTable(filePath string) (*SSTable, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Get the file size.
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()

	// The footer is 8 (indexOffset) + 8 (bfOffset) + 4 (magic) = 20 bytes.
	if fileSize < 20 {
		return nil, errors.New("file too small to be a valid SSTable")
	}
	footerBuf := make([]byte, 20)
	if _, err := f.ReadAt(footerBuf, fileSize-20); err != nil {
		return nil, err
	}
	rFooter := bytes.NewReader(footerBuf)
	var indexOffset int64
	var bfOffset int64
	if err := binary.Read(rFooter, binary.LittleEndian, &indexOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(rFooter, binary.LittleEndian, &bfOffset); err != nil {
		return nil, err
	}
	magic := make([]byte, 4)
	if _, err := rFooter.Read(magic); err != nil {
		return nil, err
	}
	if string(magic) != footerMagic {
		return nil, errors.New("invalid footer magic")
	}

	// Read the index region.
	if _, err := f.Seek(indexOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var indexCount uint32
	if err := binary.Read(f, binary.LittleEndian, &indexCount); err != nil {
		return nil, err
	}
	indexEntries := make([]IndexEntry, indexCount)
	for i := uint32(0); i < indexCount; i++ {
		key, err := readString(f)
		if err != nil {
			return nil, err
		}
		var offset int64
		if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}
		indexEntries[i] = IndexEntry{Key: key, Offset: offset}
	}

	// Read the Bloom filter region.
	if _, err := f.Seek(bfOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var m uint64
	var k uint32
	var n uint64
	if err := binary.Read(f, binary.LittleEndian, &m); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.LittleEndian, &k); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	var bitsetLen uint32
	if err := binary.Read(f, binary.LittleEndian, &bitsetLen); err != nil {
		return nil, err
	}
	bitset := make([]byte, bitsetLen)
	if _, err := io.ReadFull(f, bitset); err != nil {
		return nil, err
	}

	sst := &SSTable{
		filePath:    filePath,
		index:       indexEntries,
		bloom:       &BloomFilter{m: m, k: k, n: n, bitset: bitset},
		dataOffset:  int64(len(headerMagic)),
		indexOffset: indexOffset,
		bfOffset:    bfOffset,
		bufPool: sync.Pool{
			New: func() interface{} { return new(bytes.Buffer) },
		},
	}
	return sst, nil
}

// Get retrieves the row with the given key from the SSTable.
// It first consults the Bloom filter; if positive, it uses the sparse index
// to seek into the data region and then scans sequentially.
func (s *SSTable) Get(key string) (*Row, error) {
	// Check Bloom filter.
	if !s.bloom.MightContain([]byte(key)) {
		return nil, errors.New("key not found (bloom filter negative)")
	}
	// Use sparse index to find a starting file offset.
	startOffset := s.dataOffset
	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key > key
	})
	if i > 0 {
		startOffset = s.index[i-1].Offset
	}
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return nil, err
	}
	// Sequentially scan until we find the key or pass it.
	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if pos >= s.indexOffset {
			break
		}
		row, err := readRow(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if row.Key == key {
			return &row, nil
		}
		if row.Key > key {
			break
		}
	}
	return nil, errors.New("key not found")
}

// ReadAllRows reads and returns all rows from the data region.
func (s *SSTable) ReadAllRows() ([]Row, error) {
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(s.dataOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var rows []Row
	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if pos >= s.indexOffset {
			break
		}
		row, err := readRow(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ======================================================================
// Compaction Manager
// ======================================================================

// CompactionManager holds multiple levels of SSTables.
// For simplicity, level 0 is an array of SSTables; when there are too many,
// they are merged into a new SSTable (level 1).
type CompactionManager struct {
	levels    [][]*SSTable
	threshold int // maximum number of SSTables per level before compaction triggers
}

// NewCompactionManager creates a new compaction manager.
func NewCompactionManager(threshold int) *CompactionManager {
	return &CompactionManager{
		levels:    make([][]*SSTable, 1), // start with level 0
		threshold: threshold,
	}
}

// AddSSTable adds an SSTable to level 0 and triggers compaction if needed.
func (cm *CompactionManager) AddSSTable(sst *SSTable) error {
	cm.levels[0] = append(cm.levels[0], sst)
	if len(cm.levels[0]) >= cm.threshold {
		// Compact level 0 into a merged SSTable.
		merged, err := mergeSSTables(cm.levels[0])
		if err != nil {
			return err
		}
		// Clear level 0 and add the merged SSTable to level 1.
		cm.levels[0] = nil
		if len(cm.levels) < 2 {
			cm.levels = append(cm.levels, []*SSTable{})
		}
		cm.levels[1] = append(cm.levels[1], merged)
	}
	return nil
}

// mergeSSTables merges multiple SSTables into one.
// (For simplicity, it loads all rows into memory, merges and de–duplicates them, then writes a new SSTable.)
func mergeSSTables(ssts []*SSTable) (*SSTable, error) {
	var allRows []Row
	for _, sst := range ssts {
		rows, err := sst.ReadAllRows()
		if err != nil {
			return nil, err
		}
		allRows = append(allRows, rows...)
	}
	// Sort rows by key.
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].Key < allRows[j].Key
	})
	// Deduplicate rows (if keys repeat, keep the last occurrence).
	var dedup []Row
	seen := make(map[string]bool)
	for i := len(allRows) - 1; i >= 0; i-- {
		if !seen[allRows[i].Key] {
			seen[allRows[i].Key] = true
			dedup = append([]Row{allRows[i]}, dedup...)
		}
	}
	// Write the merged SSTable.
	newFilePath := fmt.Sprintf("sstable_merged_%d.sst", os.Getpid())
	if err := WriteSSTable(newFilePath, dedup, 10, len(dedup), 0.01); err != nil {
		return nil, err
	}
	return LoadSSTable(newFilePath)
}

// ======================================================================
// Demo (main)
// ======================================================================

func main() {
	// Create some sample rows.
	rows := []Row{
		{Key: "apple", Columns: map[string]string{"color": "red", "taste": "sweet"}},
		{Key: "banana", Columns: map[string]string{"color": "yellow", "taste": "sweet"}},
		{Key: "carrot", Columns: map[string]string{"color": "orange", "taste": "earthy"}},
		{Key: "date", Columns: map[string]string{"color": "brown", "taste": "sweet"}},
		{Key: "eggplant", Columns: map[string]string{"color": "purple", "taste": "bitter"}},
	}
	// Ensure rows are sorted by key.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Key < rows[j].Key
	})

	// Write an SSTable file.
	filePath := "sstable_demo.sst"
	if err := WriteSSTable(filePath, rows, 2, len(rows), 0.01); err != nil {
		log.Fatalf("WriteSSTable error: %v", err)
	}
	fmt.Println("SSTable written to", filePath)

	// Load the SSTable.
	sst, err := LoadSSTable(filePath)
	if err != nil {
		log.Fatalf("LoadSSTable error: %v", err)
	}

	// Retrieve a key.
	keyToGet := "carrot"
	row, err := sst.Get(keyToGet)
	if err != nil {
		fmt.Println("Get error:", err)
	} else {
		fmt.Printf("Retrieved row for key %q: %+v\n", keyToGet, row)
	}

	// Demonstrate the compaction manager.
	cm := NewCompactionManager(2)
	// Add the same SSTable twice to trigger compaction.
	if err := cm.AddSSTable(sst); err != nil {
		log.Fatalf("Compaction error: %v", err)
	}
	if err := cm.AddSSTable(sst); err != nil {
		log.Fatalf("Compaction error: %v", err)
	}
	if len(cm.levels) > 1 && len(cm.levels[1]) > 0 {
		fmt.Println("Compaction occurred. New SSTable at level 1:")
		compacted := cm.levels[1][0]
		// Retrieve a key from the compacted SSTable.
		row, err = compacted.Get("banana")
		if err != nil {
			fmt.Println("Get error from compacted SSTable:", err)
		} else {
			fmt.Printf("Retrieved row for key %q from compacted SSTable: %+v\n", "banana", row)
		}
	}
}
