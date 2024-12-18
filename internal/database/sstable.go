package database

import (
	"bufio"
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
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		if err := sstWriteEntry(writer, kv.Key, kv.Val.Data, kv.Val.Timestamp); err != nil {
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
	// Check bloom filter
	// If bloom filter is disabled for debugging, skip this step.
	if sst.bloomFilter != nil && !sst.bloomFilter.Test(key) {
		return nil, nil
	}

	// Seek to start of the file
	if _, err := sst.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(sst.file)
	for {
		rKey, rVal, _, err := sstReadEntry(reader)
		if err == io.EOF {
			// Reached end of file, key not found
			break
		}
		if err != nil {
			sst.logger.Error("failed to read entry", zap.Error(err))
			return nil, err
		}

		// Compare keys
		if string(rKey) == string(key) {
			return rVal, nil
		}
		// Because the file is sorted, once we pass the key lexically, we could break
		// But since this is a debug test, we'll read the entire file just to confirm
		// it's truly not there. Remove the lexical break if you want to fully confirm.
	}

	return nil, nil
}
