package database

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/flynnfc/bagginsdb/internal/truetime"
	"go.uber.org/zap"
)

// SSTableManager manages multiple SSTables, supports compactions.
type SSTableManager struct {
	mu            sync.RWMutex
	logger        *zap.Logger
	directory     string
	sstables      []*ssTable
	bloomSize     uint
	indexInterval int
}

// NewSSTableManager creates a new manager to handle SSTables.
func NewSSTableManager(dir string, bloomSize uint, indexInterval int, logger *zap.Logger) (*SSTableManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &SSTableManager{
		logger:        logger,
		directory:     dir,
		bloomSize:     bloomSize,
		indexInterval: indexInterval,
	}, nil
}

// FlushMemtable creates a new SSTable from the memtable and adds it to the manager.
func (mgr *SSTableManager) FlushMemtable(mem *memtable) error {
	var kvs []struct {
		Key []byte
		Val Value
	}
	for node := mem.skiplist.Front(); node != nil; node = node.Next() {
		k := node.key
		v := node.value
		kvs = append(kvs, struct {
			Key []byte
			Val Value
		}{Key: k, Val: v})
	}

	sort.Slice(kvs, func(i, j int) bool {
		return string(kvs[i].Key) < string(kvs[j].Key)
	})

	// Create a unique file name
	f, err := os.CreateTemp(mgr.directory, "sstable_")
	if err != nil {
		return err
	}
	fName := f.Name()
	f.Close()

	sst, err := buildSSTable(fName, kvs, mgr.bloomSize, mgr.indexInterval, mgr.logger)
	if err != nil {
		return err
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.sstables = append(mgr.sstables, sst)
	return nil
}

// Get finds the value for a key by checking SSTables in reverse order (newest first).
func (mgr *SSTableManager) Get(key []byte) ([]byte, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	for i := len(mgr.sstables) - 1; i >= 0; i-- {
		val, err := mgr.sstables[i].get(key)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	return nil, nil
}

// Compact merges all sstables into a single SSTable, deduplicating keys.
func (mgr *SSTableManager) Compact() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if len(mgr.sstables) <= 1 {
		return nil
	}

	// Read all entries
	var allEntries []struct {
		Key []byte
		Val Value
	}

	for _, sst := range mgr.sstables {
		if _, err := sst.file.Seek(0, 0); err != nil {
			return err
		}
		reader := sst.file
		for {
			key, val, ts, err := sstReadEntry(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			allEntries = append(allEntries, struct {
				Key []byte
				Val Value
			}{
				Key: key,
				Val: Value{Data: val, Timestamp: truetime.Timestamp{Latest: time.Unix(0, ts)}},
			})
		}
	}

	// Sort and deduplicate
	sort.Slice(allEntries, func(i, j int) bool {
		return string(allEntries[i].Key) < string(allEntries[j].Key)
	})

	deduped := deduplicateEntries(allEntries)

	// Create compacted SSTable
	fName := filepath.Join(mgr.directory, "sstable_compacted")
	sst, err := buildSSTable(fName, deduped, mgr.bloomSize, mgr.indexInterval, mgr.logger)
	if err != nil {
		return err
	}

	// Close and remove old
	for _, old := range mgr.sstables {
		old.close()
		os.Remove(old.file.Name())
	}

	mgr.sstables = []*ssTable{sst}
	return nil
}

// deduplicateEntries keeps only the latest value for each key
func deduplicateEntries(entries []struct {
	Key []byte
	Val Value
}) []struct {
	Key []byte
	Val Value
} {
	if len(entries) == 0 {
		return entries
	}
	result := make([]struct {
		Key []byte
		Val Value
	}, 0, len(entries))

	lastKey := entries[0].Key
	lastVal := entries[0].Val
	for i := 1; i < len(entries); i++ {
		if string(entries[i].Key) != string(lastKey) {
			// different key, append previous
			result = append(result, struct {
				Key []byte
				Val Value
			}{Key: lastKey, Val: lastVal})
			lastKey = entries[i].Key
			lastVal = entries[i].Val
		} else {
			// same key, choose the one with the latest timestamp
			// assuming entries are from oldest to newest sst, the last occurrence is the newest
			lastVal = entries[i].Val
		}
	}
	// append the last
	result = append(result, struct {
		Key []byte
		Val Value
	}{Key: lastKey, Val: lastVal})

	return result
}
