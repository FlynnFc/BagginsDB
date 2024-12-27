package database

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/flynnfc/bagginsdb/internal/truetime"

	"go.uber.org/zap"
)

/*
ColumnEntry represents a single “cell” or “column” in a -column model:

- PartitionKey: The main partition key (like “user_id”)
- ClusteringKeys: Zero or more values that define how columns/rows are sorted within the partition (like [“year”, “month”, “day”] or a timestamp, etc.)
- ColumnName: The “column” name within that row
- Value: The column value
- Timestamp: A truetime.Timestamp for conflict resolution, etc.
*/
type ColumnEntry struct {
	PartitionKey   []byte
	ClusteringKeys [][]byte
	ColumnName     []byte
	Value          []byte
	Timestamp      truetime.Timestamp
}

// EntryMetadata holds the minimal “index” info for an on-disk entry.
type EntryMetadata struct {
	PartitionKey   []byte
	ClusteringKeys [][]byte
	ColumnName     []byte
	FileOffset     int64
}

// SSTable represents one on-disk -column table.
type SSTable struct {
	file        *os.File
	logger      *zap.Logger
	bloomFilter *bloom.BloomFilter
	index       []EntryMetadata
}

// sstWriteEntry writes a ColumnEntry in the following format:
// [partitionKeyLen][partitionKeyBytes]
// [numClusteringKeys][clusteringKeyLen][clusteringKeyBytes] ... repeated ...
// [columnNameLen][columnNameBytes]
// [valueLen][valueBytes]
// [timestamp (int64)]
func sstWriteEntry(w io.Writer, entry ColumnEntry) error {
	// 1. Partition key
	if err := writeBytes(w, entry.PartitionKey); err != nil {
		return err
	}

	// 2. Clustering keys
	if err := binary.Write(w, binary.LittleEndian, int32(len(entry.ClusteringKeys))); err != nil {
		return err
	}
	for _, ck := range entry.ClusteringKeys {
		if err := writeBytes(w, ck); err != nil {
			return err
		}
	}

	// 3. Column name
	if err := writeBytes(w, entry.ColumnName); err != nil {
		return err
	}

	// 4. Value
	if err := writeBytes(w, entry.Value); err != nil {
		return err
	}

	// 5. Timestamp
	ts := entry.Timestamp.Latest.UnixNano()
	if err := binary.Write(w, binary.LittleEndian, ts); err != nil {
		return err
	}

	return nil
}

// sstReadEntry reads one ColumnEntry (in the format described above).
func sstReadEntry(r io.Reader) (ColumnEntry, error) {
	var entry ColumnEntry
	// partitionKey
	pk, err := readBytes(r)
	if err != nil {
		return entry, err
	}
	entry.PartitionKey = pk

	// clustering keys
	var ckCount int32
	if err := binary.Read(r, binary.LittleEndian, &ckCount); err != nil {
		return entry, err
	}
	entry.ClusteringKeys = make([][]byte, ckCount)
	for i := 0; i < int(ckCount); i++ {
		ck, err := readBytes(r)
		if err != nil {
			return entry, err
		}
		entry.ClusteringKeys[i] = ck
	}

	// column name
	colName, err := readBytes(r)
	if err != nil {
		return entry, err
	}
	entry.ColumnName = colName

	// value
	val, err := readBytes(r)
	if err != nil {
		return entry, err
	}
	entry.Value = val

	// timestamp
	var ts int64
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return entry, err
	}
	entry.Timestamp.Latest = time.Now()

	return entry, nil
}

// buildSSTable builds a new file from an already-sorted slice of ColumnEntry.
func buildSSTable(filePath string, entries []ColumnEntry, bloomSize uint, indexInterval int, logger *zap.Logger) (*SSTable, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)

	bf := bloom.New(bloomSize, 5)
	idx := make([]EntryMetadata, 0, len(entries)/indexInterval+1)

	for i, e := range entries {
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		if err := sstWriteEntry(writer, e); err != nil {
			return nil, err
		}
		if err := writer.Flush(); err != nil {
			return nil, err
		}

		// Add to bloom filter by the full composite key
		composite := compositeKey(e.PartitionKey, e.ClusteringKeys, e.ColumnName)
		bf.Add(composite)

		// If we’re at an interval or the first entry, store in index
		// (You can tune how you store the sparse index.)
		if i%indexInterval == 0 {
			idx = append(idx, EntryMetadata{
				PartitionKey:   e.PartitionKey,
				ClusteringKeys: e.ClusteringKeys,
				ColumnName:     e.ColumnName,
				FileOffset:     offset,
			})
		}
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}
	// Do these AFTER actually flushing the important data. These can be generated from the data if failed and needed :)
	baseIden := ExtractIdentifier(filePath)
	if err := WriteBloomFilter(bf, "bloom_"+baseIden); err != nil {
		return nil, err
	}

	if err := writeSSTableIndex("index_"+baseIden, idx); err != nil {
		return nil, err
	}

	return &SSTable{
		file:        file,
		logger:      logger,
		bloomFilter: bf,
		index:       idx,
	}, nil
}

// close closes the  sstable file.
func (wst *SSTable) close() error {
	return wst.file.Close()
}

// get attempts to find the *exact* (partitionKey, clusteringKeys, columnName). If you want range scans,
// you would write a separate method that scans from one composite to another.
func (wst *SSTable) get(partKey []byte, clustering [][]byte, colName []byte) ([]byte, error) {
	// 1. Bloom filter check
	cmp := compositeKey(partKey, clustering, colName)
	if wst.bloomFilter != nil && !wst.bloomFilter.Test(cmp) {
		return nil, nil
	}

	// 2. Find approximate location via binary search in the sparse index
	i := BinarySearch(wst.index, partKey, clustering, colName)
	if i < 0 {
		return nil, nil
	}

	// Because the index is sparse, we may need to scan forward from index[i].FileOffset
	// to find the exact entry in a short region.
	offset := wst.index[i].FileOffset
	if _, err := wst.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	// read forward until we pass the possible matching composite key or hit EOF
	reader := bufio.NewReader(wst.file)
	for {
		curOffset, _ := wst.file.Seek(0, io.SeekCurrent)
		entry, err := sstReadEntry(reader)
		if err == io.EOF {
			// no more data
			return nil, nil
		}
		if err != nil {
			wst.logger.Error("failed to read  entry", zap.Error(err))
			return nil, err
		}

		// If we jumped too far, or if partition/clustering is bigger, we can stop
		cmpEntry := compositeKey(entry.PartitionKey, entry.ClusteringKeys, entry.ColumnName)
		if bytes.Compare(cmpEntry, cmp) > 0 {
			// we've passed it
			return nil, nil
		}

		// if match
		if bytes.Equal(cmpEntry, cmp) {
			// found it
			return entry.Value, nil
		}

		// otherwise keep reading
		// but watch out for index intervals; if we’re beyond a small window, we might stop
		if (curOffset - offset) > 65536 { // e.g. stop scanning after 64KB
			// your real logic can vary
			return nil, nil
		}
	}
}

// BinarySearch does a binary search on the sparse index for the “closest” offset to
// the composite key. This doesn’t guarantee an exact match—only a region where you can
// sequentially read to find it or confirm missing.
func BinarySearch(index []EntryMetadata, pk []byte, clustering [][]byte, colName []byte) int {
	low, high := 0, len(index)-1
	cmp := compositeKey(pk, clustering, colName)
	for low <= high {
		mid := (low + high) / 2
		midCmp := compositeKey(index[mid].PartitionKey, index[mid].ClusteringKeys, index[mid].ColumnName)
		switch bytes.Compare(cmp, midCmp) {
		case -1:
			high = mid - 1
		case 1:
			low = mid + 1
		default:
			return mid
		}
	}
	// If not found exactly, low is the insertion point. We return low-1 or just low
	// so that we can start scanning from there.
	return max(0, high)
}

// compositeKey creates a single byte slice combining partitionKey, clusteringKeys, and columnName
// so that lexicographic ordering respects the -column ordering.
func compositeKey(partKey []byte, clustering [][]byte, colName []byte) []byte {
	// You can implement a more elaborate scheme. Here, we simply do:
	// partitionKey + 0x00 + each cluster + 0x01 ... + colName + 0x02
	// so that we can do a lex-based comparison.
	var buf bytes.Buffer
	buf.Write(partKey)
	buf.WriteByte(0x00)
	for _, ck := range clustering {
		buf.Write(ck)
		buf.WriteByte(0x01)
	}
	buf.Write(colName)
	buf.WriteByte(0x02)
	return buf.Bytes()
}

// Helpers for writing/reading length-delimited byte slices
func writeBytes(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.LittleEndian, int32(len(b))); err != nil {
		return err
	}
	if len(b) > 0 {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func readBytes(r io.Reader) ([]byte, error) {
	var length int32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, errors.New("negative length")
	}
	b := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
