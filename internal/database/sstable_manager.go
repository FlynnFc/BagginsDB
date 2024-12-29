package database

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
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

	// Load saved sstables
	mgr := &SSTableManager{
		logger:        logger,
		directory:     dir,
		bloomSize:     bloomSize,
		indexInterval: indexInterval,
	}

	// // TODO: Issue with loading index and bloom. Actually want this at the top of the sstable file.
	// if err := mgr.LoadSStables(); err != nil {
	// 	return mgr, err
	// }

	return mgr, nil
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
	compPath := filepath.Join(mgr.directory, "sstable_compacted")
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

func (mgr *SSTableManager) LoadSStables() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	files, err := os.ReadDir(mgr.directory)
	if err != nil {
		return err
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if !strings.HasPrefix(f.Name(), "sstable_") {
			continue
		}

		sst, err := openSSTable(filepath.Join(mgr.directory, f.Name()), mgr.logger)
		if err != nil {
			return err
		}
		mgr.sstables = append(mgr.sstables, sst)
	}

	return nil
}

func ExtractIdentifier(sstPath string) string {
	// Split the string by "/"
	parts := strings.Split(sstPath, "\\")
	if len(parts) < 2 {
		return "9999"
	}

	// Get the last part and check if it contains "sstable_"
	lastPart := parts[len(parts)-1]
	if strings.HasPrefix(lastPart, "sstable_") {
		return lastPart[len("sstable_"):] // Extract everything after "sstable_"
	}

	return "cheese"
}

func openSSTable(filePath string, logger *zap.Logger) (*SSTable, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	baseIden := ExtractIdentifier(filePath)
	idx, err := readSSTableIndex("sst/" + "index_" + baseIden)
	if err != nil {
		return nil, err
	}
	bf, err := readBloomFilter("sst/" + "bloom_" + baseIden)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		file:        file,
		logger:      logger,
		bloomFilter: bf,
		index:       idx,
	}, nil
}

func readSSTableIndex(filePath string) ([]EntryMetadata, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var idx []EntryMetadata

	for {
		var entry EntryMetadata

		// Read the PartitionKey
		var partitionKeyLength int32
		if err := binary.Read(reader, binary.BigEndian, &partitionKeyLength); err != nil {
			if err == io.EOF {
				break // End of file
			}
			return nil, fmt.Errorf("failed to read partition key length: %w", err)
		}
		partitionKey := make([]byte, partitionKeyLength)
		if _, err := io.ReadFull(reader, partitionKey); err != nil {
			return nil, fmt.Errorf("failed to read partition key: %w", err)
		}
		entry.PartitionKey = partitionKey

		// Read the number of ClusteringKeys
		var numClusteringKeys int32
		if err := binary.Read(reader, binary.BigEndian, &numClusteringKeys); err != nil {
			return nil, fmt.Errorf("failed to read number of clustering keys: %w", err)
		}
		clusteringKeys := make([][]byte, numClusteringKeys)
		for i := int32(0); i < numClusteringKeys; i++ {
			var clusteringKeyLength int32
			if err := binary.Read(reader, binary.BigEndian, &clusteringKeyLength); err != nil {
				return nil, fmt.Errorf("failed to read clustering key length: %w", err)
			}
			clusteringKey := make([]byte, clusteringKeyLength)
			if _, err := io.ReadFull(reader, clusteringKey); err != nil {
				return nil, fmt.Errorf("failed to read clustering key: %w", err)
			}
			clusteringKeys[i] = clusteringKey
		}
		entry.ClusteringKeys = clusteringKeys

		// Read the ColumnName
		var columnNameLength int32
		if err := binary.Read(reader, binary.BigEndian, &columnNameLength); err != nil {
			return nil, fmt.Errorf("failed to read column name length: %w", err)
		}
		columnName := make([]byte, columnNameLength)
		if _, err := io.ReadFull(reader, columnName); err != nil {
			return nil, fmt.Errorf("failed to read column name: %w", err)
		}
		entry.ColumnName = columnName

		// Read the FileOffset
		if err := binary.Read(reader, binary.BigEndian, &entry.FileOffset); err != nil {
			return nil, fmt.Errorf("failed to read file offset: %w", err)
		}

		// Append the entry to the index
		idx = append(idx, entry)
	}

	return idx, nil
}

func writeSSTableIndex(filePath string, idx []EntryMetadata) error {
	file, err := os.OpenFile("sst/"+filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, entry := range idx {
		if err := binary.Write(writer, binary.BigEndian, int32(len(entry.PartitionKey))); err != nil {
			return fmt.Errorf("failed to write partition key length: %w", err)
		}
		if _, err := writer.Write(entry.PartitionKey); err != nil {
			return fmt.Errorf("failed to write partition key: %w", err)
		}

		if err := binary.Write(writer, binary.BigEndian, int32(len(entry.ClusteringKeys))); err != nil {
			return fmt.Errorf("failed to write number of clustering keys: %w", err)
		}
		for _, key := range entry.ClusteringKeys {
			if err := binary.Write(writer, binary.BigEndian, int32(len(key))); err != nil {
				return fmt.Errorf("failed to write clustering key length: %w", err)
			}
			if _, err := writer.Write(key); err != nil {
				return fmt.Errorf("failed to write clustering key: %w", err)
			}
		}

		if err := binary.Write(writer, binary.BigEndian, int32(len(entry.ColumnName))); err != nil {
			return fmt.Errorf("failed to write column name length: %w", err)
		}
		if _, err := writer.Write(entry.ColumnName); err != nil {
			return fmt.Errorf("failed to write column name: %w", err)
		}

		if err := binary.Write(writer, binary.BigEndian, entry.FileOffset); err != nil {
			return fmt.Errorf("failed to write file offset: %w", err)
		}
	}

	return nil
}

func writeBloomFilter(filter *bloom.BloomFilter, fileName string) error {
	file, err := os.Create("sst/" + fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Serialize the bloom filter to a byte buffer
	data, err := filter.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to write bloom filter to buffer: %w", err)
	}

	buffer := bytes.NewBuffer(data)

	// Write the buffer to the file
	if _, err := file.Write(buffer.Bytes()); err != nil {
		return fmt.Errorf("failed to write buffer to file: %w", err)
	}

	return nil
}

func readBloomFilter(fileName string) (*bloom.BloomFilter, error) {
	// Open the file
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	buffer := make([]byte, info.Size())
	if _, err := file.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to read file content: %w", err)
	}

	filter := bloom.New(1, 1) // Create an empty filter to populate
	if err := filter.UnmarshalBinary(buffer); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter from buffer: %w", err)
	}

	return filter, nil
}

func readEntryMetadata(reader *bufio.Reader) (EntryMetadata, error) {
	var entry EntryMetadata
	if err := binary.Read(reader, binary.BigEndian, &entry.PartitionKey); err != nil {
		return EntryMetadata{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &entry.ClusteringKeys); err != nil {
		return EntryMetadata{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &entry.ColumnName); err != nil {
		return EntryMetadata{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &entry.FileOffset); err != nil {
		return EntryMetadata{}, err
	}
	return entry, nil
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

// Close all open sstables
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
