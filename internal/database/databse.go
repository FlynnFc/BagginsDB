package database

import (
	"sync"

	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/truetime"
	"go.uber.org/zap"
)

// DBConfig holds user-defined settings (similar to your Config).
type Config struct {
	Host             string
	MemTableSize     int
	ConsistencyLevel int
}

// Database is our -column DB that manages memtables & SSTables.
type Database struct {
	logger            *zap.Logger
	wal               logger.WAL
	config            Config
	memtable          *memtable
	oldMemtable       *memtable
	sstManager        *SSTableManager
	mu                sync.RWMutex
	clock             *truetime.TrueTime
	compactionMu      sync.Mutex
	flushMu           sync.Mutex
	compactionTrigger chan struct{}

	flushThreshold int // how many entries or how big the memtable is before flushing
}

// NewDatabase creates a new DB instance, sets up the memtable, sstable manager, etc.
func NewDatabase(l *zap.Logger, c Config) *Database {
	// 1. Start a TrueTime clock
	clock := truetime.NewTrueTime(l)
	// 2. Create the primary memtable
	memtable := NewMemtable()

	// 3. Build your  SSTable manager
	dir := "sst" // directory to store sst files
	bloomSize := uint(1000000)
	indexInterval := 10

	sstManager, err := NewSSTableManager(dir, bloomSize, indexInterval, l)
	if err != nil {
		l.Fatal("Failed to create SSTableManager", zap.Error(err))
		panic(err)
	}

	wal := logger.InitWAL("logs/wal.log")

	// 4. Construct the Database struct
	db := &Database{
		logger:            l,
		config:            c,
		memtable:          memtable,
		sstManager:        sstManager,
		clock:             clock,
		wal:               wal,
		compactionTrigger: make(chan struct{}, 1),
		flushThreshold:    1024 * 1024, // e.g. 100KB or 100k entries, up to you
	}

	db.startCompactionWorker()
	return db
}

func (db *Database) Put(partKey []byte, clustering [][]byte, colName []byte, value []byte) {
	// 1) Acquire the lock for a short time
	db.wal.Batch.Write(db.wal.Index, partKey)
	// db.wal.Info("Put", zap.ByteString("partKey", partKey), zap.ByteStrings("clustering", clustering), zap.ByteString("colName", colName), zap.ByteString("value", value))
	db.memtable.Put(ColumnEntry{
		PartitionKey:   partKey,
		ClusteringKeys: clustering,
		ColumnName:     colName,
		Value:          value,
		Timestamp:      db.clock.Now(),
	})

	needFlush := db.memtable.Len() > db.flushThreshold
	var old *memtable
	if needFlush {
		old = db.memtable
		db.memtable = NewMemtable()

		if len(db.sstManager.sstables) > 10 {
			select {
			case db.compactionTrigger <- struct{}{}:
			default:
			}
		}
	}

	if needFlush && old != nil {
		go func(m *memtable) {
			db.flushMu.Lock()
			defer db.flushMu.Unlock()

			entries := m.ToColumnEntries()
			if err := db.sstManager.FlushMemtable(entries); err != nil {
				db.logger.Error("Flush failed", zap.Error(err))
			} else {
				db.logger.Info("Flushed memtable", zap.Int("entries", len(entries)))
			}
		}(old)
	}
}

// Get retrieves one “cell” by (partitionKey, clusteringKeys, columnName).
func (db *Database) Get(partKey []byte, clustering [][]byte, colName []byte) []byte {
	// Check the active memtable
	val := db.memtable.Get(partKey, clustering, colName)
	if val == nil && db.oldMemtable != nil {
		// Check old memtable (if there was a recent flush)
		val = db.oldMemtable.Get(partKey, clustering, colName)
	}
	if val == nil {
		// Go to SSTables
		res, err := db.sstManager.Get(partKey, clustering, colName)
		if err != nil {
			db.logger.Error("Error reading from SSTableManager", zap.Error(err))
			return nil
		}
		val = res
	}
	return val
}

func (db *Database) startCompactionWorker() {
	go func() {
		for range db.compactionTrigger {
			db.compactionMu.Lock()
			if err := db.sstManager.Compact(); err != nil {
				db.logger.Error("Failed to compact SSTables", zap.Error(err))
			} else {
				db.logger.Info("Compaction completed", zap.Int("sstables", len(db.sstManager.sstables)))
			}
			db.compactionMu.Unlock()
		}
	}()
}

// Compact is a manual method to force a compaction
func (db *Database) Compact() {
	if err := db.sstManager.Compact(); err != nil {
		db.logger.Error("Failed to compact SSTables", zap.Error(err))
	}
}

func (db *Database) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Flush current memtable
	if db.memtable != nil {
		Entries := db.memtable.ToColumnEntries()
		if err := db.sstManager.FlushMemtable(Entries); err != nil {
			db.logger.Error("Failed to flush memtable on close", zap.Error(err))
		}
		db.memtable = nil
	}

	// Clean up old memtable if it exists
	if db.oldMemtable != nil {
		Entries := db.oldMemtable.ToColumnEntries()
		if err := db.sstManager.FlushMemtable(Entries); err != nil {
			db.logger.Error("Failed to flush old memtable on close", zap.Error(err))
		}
		db.oldMemtable = nil
	}

	// Close the manager
	if err := db.sstManager.Close(); err != nil {
		db.logger.Error("Failed to close SSTableManager", zap.Error(err))
	}
}
