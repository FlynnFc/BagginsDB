package database

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	headerMagic = "SST1"
	footerMagic = "SSTF"
)

// Cell represents a single cell in a wide–column store.

// readBytesWithPrefix reads a length-prefixed byte slice.
func readBytesWithPrefix(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// writeCell writes a Cell to w.
func writeCell(w io.Writer, cell *Cell) error {
	// Write partition key.
	if err := writeBytesWithPrefix(w, cell.PartitionKey); err != nil {
		return err
	}
	// Write clustering values: first the count, then each value.
	count := uint32(len(cell.ClusteringValues))
	if err := binary.Write(w, binary.BigEndian, count); err != nil {
		return err
	}
	for _, cv := range cell.ClusteringValues {
		if err := writeBytesWithPrefix(w, cv); err != nil {
			return err
		}
	}
	// Write column name.
	if err := writeBytesWithPrefix(w, cell.ColumnName); err != nil {
		return err
	}
	// Write value.
	if err := writeBytesWithPrefix(w, cell.Value); err != nil {
		return err
	}
	return nil
}

// readCell reads a Cell from r.
func readCell(r io.Reader) (Cell, error) {
	var cell Cell
	var err error

	if cell.PartitionKey, err = readBytesWithPrefix(r); err != nil {
		return cell, err
	}
	// Read clustering values count.
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return cell, err
	}
	cell.ClusteringValues = make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		if cell.ClusteringValues[i], err = readBytesWithPrefix(r); err != nil {
			return cell, err
		}
	}
	if cell.ColumnName, err = readBytesWithPrefix(r); err != nil {
		return cell, err
	}
	if cell.Value, err = readBytesWithPrefix(r); err != nil {
		return cell, err
	}
	return cell, nil
}

// IndexEntry represents a sparse index entry.
type IndexEntry struct {
	Key    []byte // The composite key (as encoded by Cell.CompositeKey).
	Offset int64
}

// SSTable holds in–memory metadata for an SSTable file.
type SSTable struct {
	filePath    string
	index       []IndexEntry
	bloom       *bloom.BloomFilter
	dataOffset  int64 // start of data region (right after header)
	indexOffset int64 // where index region begins
	bfOffset    int64 // where Bloom filter region begins
	bufPool     sync.Pool
}

// WriteSSTable writes a new SSTable file with the provided cells.
// The cells must be sorted in increasing order by their composite key.
// Every sparseInterval-th cell is recorded in the sparse index.
// The Bloom filter is sized using expectedItems and falsePositiveRate.
func WriteSSTable(filePath string, cells []Cell, sparseInterval int, expectedItems int, falsePositiveRate float64) (*SSTable, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Write header.
	if _, err := f.Write([]byte(headerMagic)); err != nil {
		return nil, err
	}
	// dataStart marks the beginning of the data region.
	dataStart := int64(len(headerMagic))

	// Create a new Bloom filter using the library.
	bf := bloom.NewWithEstimates(uint(expectedItems), falsePositiveRate)

	var indexEntries []IndexEntry

	// Write data region.
	for i, cell := range cells {
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		// Ensure a non-zero sparseInterval.
		if sparseInterval == 0 {
			sparseInterval = 2
		}
		if i%sparseInterval == 0 {
			indexEntries = append(indexEntries, IndexEntry{
				Key:    cell.CompositeKey(),
				Offset: offset,
			})
		}
		// Add composite key to the Bloom filter.
		bf.Add(cell.CompositeKey())
		// Write the cell.
		if err := writeCell(f, &cell); err != nil {
			return nil, err
		}
	}

	// Mark where the index region begins.
	indexOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// Write index region: first the count, then each entry.
	if err := binary.Write(f, binary.LittleEndian, uint32(len(indexEntries))); err != nil {
		return nil, err
	}
	for _, entry := range indexEntries {
		if err := writeBytesWithPrefix(f, entry.Key); err != nil {
			return nil, err
		}
		if err := binary.Write(f, binary.LittleEndian, entry.Offset); err != nil {
			return nil, err
		}
	}

	// Mark where the Bloom filter region begins.
	bfOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// Serialize the Bloom filter using gob.
	var bfBuffer bytes.Buffer
	if err := gob.NewEncoder(&bfBuffer).Encode(bf); err != nil {
		return nil, err
	}
	bfBytes := bfBuffer.Bytes()
	// Write out the length of the serialized Bloom filter.
	if err := binary.Write(f, binary.LittleEndian, uint32(len(bfBytes))); err != nil {
		return nil, err
	}
	// Write the Bloom filter bytes.
	if _, err := f.Write(bfBytes); err != nil {
		return nil, err
	}

	// Write footer: indexOffset, bfOffset, and footer magic.
	if err := binary.Write(f, binary.LittleEndian, indexOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(f, binary.LittleEndian, bfOffset); err != nil {
		return nil, err
	}
	if _, err := f.Write([]byte(footerMagic)); err != nil {
		return nil, err
	}

	// dataStart is available for debugging if needed.
	_ = dataStart

	return &SSTable{
		filePath:    filePath,
		index:       indexEntries,
		bloom:       bf,
		dataOffset:  int64(len(headerMagic)),
		indexOffset: indexOffset,
		bfOffset:    bfOffset,
		bufPool: sync.Pool{
			New: func() interface{} { return new(bytes.Buffer) },
		},
	}, nil
}

// LoadSSTable loads an SSTable from disk, reconstructing its sparse index and Bloom filter.
func LoadSSTable(filePath string) (*SSTable, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fi.Size()
	// Footer is 8 (indexOffset) + 8 (bfOffset) + 4 (magic) = 20 bytes.
	if fileSize < 20 {
		return nil, errors.New("file too small to be a valid SSTable")
	}

	// Read footer.
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

	// Read index region.
	if _, err := f.Seek(indexOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var indexCount uint32
	if err := binary.Read(f, binary.LittleEndian, &indexCount); err != nil {
		return nil, err
	}
	indexEntries := make([]IndexEntry, indexCount)
	for i := uint32(0); i < indexCount; i++ {
		key, err := readBytesWithPrefix(f)
		if err != nil {
			return nil, err
		}
		var offset int64
		if err := binary.Read(f, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}
		indexEntries[i] = IndexEntry{Key: key, Offset: offset}
	}

	// Read Bloom filter region.
	if _, err := f.Seek(bfOffset, io.SeekStart); err != nil {
		return nil, err
	}
	// First, read the length of the serialized Bloom filter.
	var bfLength uint32
	if err := binary.Read(f, binary.LittleEndian, &bfLength); err != nil {
		return nil, err
	}
	bfBytes := make([]byte, bfLength)
	if _, err := io.ReadFull(f, bfBytes); err != nil {
		return nil, err
	}
	// Decode the Bloom filter using gob.
	bfBuffer := bytes.NewBuffer(bfBytes)
	var bf bloom.BloomFilter
	if err := gob.NewDecoder(bfBuffer).Decode(&bf); err != nil {
		return nil, err
	}

	sst := &SSTable{
		filePath:    filePath,
		index:       indexEntries,
		bloom:       &bf,
		dataOffset:  int64(len(headerMagic)),
		indexOffset: indexOffset,
		bfOffset:    bfOffset,
		bufPool: sync.Pool{
			New: func() interface{} { return new(bytes.Buffer) },
		},
	}
	return sst, nil
}

// Get retrieves a Cell using its composite key components.
// The caller provides the PartitionKey, ColumnName, and (optionally) ClusteringValues.
func (s *SSTable) Get(partitionKey, columnName []byte, clusteringValues ...[]byte) (*Cell, error) {
	// Compose the composite key.
	cell := Cell{
		PartitionKey:     partitionKey,
		ClusteringValues: clusteringValues,
		ColumnName:       columnName,
	}
	compositeKey := cell.CompositeKey()

	// Check Bloom filter.
	if !s.bloom.Test(compositeKey) {
		return nil, errors.New("key not found (bloom filter negative)")
	}

	// Use the sparse index to narrow the search window.
	// Binary search the sparse index for the first entry with a key greater than compositeKey.
	i := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].Key, compositeKey) > 0
	})

	var startOffset, endOffset int64
	if i == 0 {
		// If the first index entry is already greater than compositeKey, search from data start.
		startOffset = s.dataOffset
		endOffset = s.index[0].Offset
	} else if i < len(s.index) {
		// Otherwise, search from the previous index entry until the current index entry.
		startOffset = s.index[i-1].Offset
		endOffset = s.index[i].Offset
	} else {
		// If compositeKey is greater than all sparse index keys,
		// start at the last index entry and scan until the index region begins.
		startOffset = s.index[len(s.index)-1].Offset
		endOffset = s.indexOffset
	}

	// Open the file for scanning.
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Seek to the start offset.
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return nil, err
	}

	// Scan sequentially from startOffset to endOffset.
	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		// Stop scanning once we reach the end offset.
		if pos >= endOffset {
			break
		}
		candidate, err := readCell(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		candidateKey := candidate.CompositeKey()
		cmp := bytes.Compare(candidateKey, compositeKey)
		if cmp == 0 {
			return &candidate, nil
		} else if cmp > 0 {
			// Since the keys are sorted, if we've passed the target, we can abort.
			break
		}
	}
	return nil, errors.New("key not found")
}

// ReadAllCells reads all cells from the SSTable data region.
func (s *SSTable) ReadAllCells() ([]Cell, error) {
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(s.dataOffset, io.SeekStart); err != nil {
		return nil, err
	}
	var cells []Cell
	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		if pos >= s.indexOffset {
			break
		}
		cell, err := readCell(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		cells = append(cells, cell)
	}
	return cells, nil
}
