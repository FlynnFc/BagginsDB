package database

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/flynnfc/bagginsdb/internal/truetime"
	"github.com/willf/bloom"
	"go.uber.org/zap"
)

type entryMetadata struct {
	Key        []byte
	FileOffset int64
}

// ssTable represents a single SSTable file.
type ssTable struct {
	file        *os.File
	logger      *zap.Logger
	bloomFilter *bloom.BloomFilter
	index       []entryMetadata // sparse index
}

// sstWriteEntry writes a key-value-timestamp record to w.
func sstWriteEntry(w io.Writer, key, value []byte, ts truetime.Timestamp) error {
	if err := binary.Write(w, binary.LittleEndian, int32(len(key))); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, int32(len(value))); err != nil {
		return err
	}
	if _, err := w.Write(value); err != nil {
		return err
	}
	timestamp := ts.Latest.UnixNano()
	if err := binary.Write(w, binary.LittleEndian, timestamp); err != nil {
		return err
	}
	return nil
}

// sstReadEntry reads a single record from r.
func sstReadEntry(r io.Reader) (key, value []byte, ts int64, err error) {
	var keyLen int32
	if err = binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return
	}
	if keyLen <= 0 || keyLen > 1024*1024 {
		err = errors.New("invalid key length")
		return
	}

	key = make([]byte, keyLen)
	if _, err = io.ReadFull(r, key); err != nil {
		return
	}

	var valueLen int32
	if err = binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return
	}
	if valueLen < 0 || valueLen > 1024*1024 {
		err = errors.New("invalid value length")
		return
	}

	value = make([]byte, valueLen)
	if _, err = io.ReadFull(r, value); err != nil {
		return
	}

	if err = binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return
	}

	return key, value, ts, nil
}

// buildSSTable creates a new SSTable file from a sorted list of key-value pairs.
func buildSSTable(filePath string, kvs []struct {
	Key []byte
	Val Value
}, bloomSize uint, indexInterval int, logger *zap.Logger) (*ssTable, error) {

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)

	bf := bloom.New(bloomSize, 5)
	index := make([]entryMetadata, 0, len(kvs)/indexInterval+1)

	for _, kv := range kvs {
		// Get the offset *before* writing
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		if err := sstWriteEntry(writer, kv.Key, kv.Val.Data, kv.Val.Timestamp); err != nil {
			return nil, err
		}

		// Flush the writer to ensure data is written to the file
		if err := writer.Flush(); err != nil {
			return nil, err
		}

		bf.Add(kv.Key)
		index = append(index, entryMetadata{Key: kv.Key, FileOffset: offset})
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	return &ssTable{
		file:        file,
		logger:      logger,
		bloomFilter: bf,
		index:       index,
	}, nil
}

// close closes the sstable file.
func (sst *ssTable) close() error {
	return sst.file.Close()
}

// get attempts to find a key in this sstable.
func (sst *ssTable) get(key []byte) ([]byte, error) {
	// Check bloom filter (unchanged)
	if sst.bloomFilter != nil && !sst.bloomFilter.Test(key) {
		return nil, nil
	}

	// Binary search the index
	i := binarySearch(sst.index, key)
	if i < 0 {
		// Key not found in the index
		return nil, nil
	}

	// Seek to the offset in the index
	offset := sst.index[i].FileOffset
	if _, err := sst.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read a single entry (assuming key is unique within the file)
	rKey, rVal, _, err := sstReadEntry(sst.file)
	if err != nil {
		sst.logger.Error("failed to read entry", zap.Error(err))
		return nil, err
	}

	// Compare keys (unchanged)
	if string(rKey) == string(key) {
		return rVal, nil
	}

	// Key not found (shouldn't happen as bloom filter and index should prevent this)
	return nil, errors.New("key not found (unexpected)")
}

// binarySearch implements a binary search on the index for the given key
func binarySearch(index []entryMetadata, key []byte) int {
	low := 0
	high := len(index) - 1
	for low <= high {
		mid := (low + high) / 2
		if compareKeys(key, index[mid].Key) < 0 {
			high = mid - 1
		} else if compareKeys(key, index[mid].Key) > 0 {
			low = mid + 1
		} else {
			return mid // Key found
		}
	}
	return -1 // Key not found
}

// compareKeys compares two byte slices lexicographically
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}
