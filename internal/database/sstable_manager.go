package database

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"go.uber.org/zap"
)

// SSTableManager manages multiple SSTables (for compaction, etc.).
type SSTableManager struct {
	mu            sync.RWMutex
	logger        *zap.Logger
	directory     string
	sstables      []*SSTable
	bloomSize     uint
	indexInterval int
}

// NewSSTableManager creates a new manager.
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

// FlushMemtable would convert a  “memtable” (not shown) into an SSTable on disk.
// For demonstration, assume we have a slice of ColumnEntry that’s already sorted.
func (mgr *SSTableManager) FlushMemtable(entries []ColumnEntry) error {
	// Sort the entries by composite ordering (partitionKey, clusteringKeys, columnName).
	sort.Slice(entries, func(i, j int) bool {
		cmpI := compositeKey(entries[i].PartitionKey, entries[i].ClusteringKeys, entries[i].ColumnName)
		cmpJ := compositeKey(entries[j].PartitionKey, entries[j].ClusteringKeys, entries[j].ColumnName)
		return bytes.Compare(cmpI, cmpJ) < 0
	})

	// Create a unique file
	f, err := os.CreateTemp(mgr.directory, "sstable_")
	if err != nil {
		return err
	}
	fName := f.Name()
	_ = f.Close()

	sst, err := buildSSTable(fName, entries, mgr.bloomSize, mgr.indexInterval, mgr.logger)
	if err != nil {
		return err
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.sstables = append(mgr.sstables, sst)
	return nil
}

// Get retrieves a single “cell” identified by (partitionKey, clusteringKeys, columnName)
// checking from newest table first, similar to your original code.
func (mgr *SSTableManager) Get(partKey []byte, clustering [][]byte, colName []byte) ([]byte, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	for i := len(mgr.sstables) - 1; i >= 0; i-- {
		val, err := mgr.sstables[i].get(partKey, clustering, colName)
		if err != nil {
			return nil, err
		}
		if val != nil {
			return val, nil
		}
	}
	return nil, nil
}

// Compact merges all sstables into a single one, deduplicating by (partitionKey, clustering, columnName).
func (mgr *SSTableManager) Compact() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if len(mgr.sstables) <= 1 {
		return nil
	}

	// read all entries from all sstables
	var all []ColumnEntry
	for _, sst := range mgr.sstables {
		if _, err := sst.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		reader := bufio.NewReader(sst.file)
		for {
			entry, err := sstReadEntry(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			all = append(all, entry)
		}
	}

	// sort by composite key
	sort.Slice(all, func(i, j int) bool {
		cmpI := compositeKey(all[i].PartitionKey, all[i].ClusteringKeys, all[i].ColumnName)
		cmpJ := compositeKey(all[j].PartitionKey, all[j].ClusteringKeys, all[j].ColumnName)
		return bytes.Compare(cmpI, cmpJ) < 0
	})

	// deduplicate (keep the latest by timestamp)
	merged := deduplicateEntries(all)

	// create a new compacted table
	compPath := filepath.Join(mgr.directory, "wsstable_compacted")
	newSST, err := buildSSTable(compPath, merged, mgr.bloomSize, mgr.indexInterval, mgr.logger)
	if err != nil {
		return err
	}

	// close & remove old
	for _, old := range mgr.sstables {
		old.close()
		os.Remove(old.file.Name())
	}

	mgr.sstables = []*SSTable{newSST}
	return nil
}

// deduplicateEntries merges entries that share the same (partitionKey, clusteringKeys, columnName),
// keeping only the one with the newest Timestamp.
func deduplicateEntries(entries []ColumnEntry) []ColumnEntry {
	if len(entries) == 0 {
		return entries
	}

	result := make([]ColumnEntry, 0, len(entries))
	last := entries[0]

	for i := 1; i < len(entries); i++ {
		if sameCompositeKey(last, entries[i]) {
			// same “cell,” keep the newer timestamp
			if entries[i].Timestamp.Latest.After(last.Timestamp.Latest) {
				last = entries[i]
			}
		} else {
			// different key => push previous
			result = append(result, last)
			last = entries[i]
		}
	}
	// push final
	result = append(result, last)
	return result
}

func sameCompositeKey(a, b ColumnEntry) bool {
	// Compare partitionKey, then length of clusteringKeys, each clusteringKey, then columnName
	if !bytes.Equal(a.PartitionKey, b.PartitionKey) {
		return false
	}
	if len(a.ClusteringKeys) != len(b.ClusteringKeys) {
		return false
	}
	for i := 0; i < len(a.ClusteringKeys); i++ {
		if !bytes.Equal(a.ClusteringKeys[i], b.ClusteringKeys[i]) {
			return false
		}
	}
	return bytes.Equal(a.ColumnName, b.ColumnName)
}

// Close closes all open sstables
func (mgr *SSTableManager) Close() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, sst := range mgr.sstables {
		if err := sst.close(); err != nil {
			return err
		}
	}
	return nil
}
