package db

import (
	"fmt"
	"os"
	"sync"

	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/truetime"
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
	wal               *zap.Logger
	config            Config
	memtable          *memtable
	oldMemtable       *memtable
	sstManager        *sstableManager
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
	memtable := newMemtable()

	// 3. Build your  SSTable manager
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		l.Fatal("NODE_ID environment variable must be set")
		panic("NODE_ID environment variable must be set")
	}
	dir := fmt.Sprintf("%s/sst", nodeID) // directory to store sst files
	bloomSize := uint(1000000)
	indexInterval := uint(10)

	sstManager, err := newSSTableManager(dir, bloomSize, indexInterval, l)
	if err != nil {
		l.Fatal("Failed to create SSTableManager", zap.Error(err))
		panic(err)
	}

	wal := logger.InitWALLogger("wal/wal")

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

	return db
}

func (db *Database) Put(partKey []byte, clustering [][]byte, colName []byte, value []byte) {
	db.wal.Info("Put", zap.ByteString("partKey", partKey), zap.ByteStrings("clustering", clustering), zap.ByteString("colName", colName), zap.ByteString("value", value))
	db.memtable.Put(Cell{
		PartitionKey:     partKey,
		ClusteringValues: clustering,
		ColumnName:       colName,
		Value:            value,
	})

	needFlush := db.memtable.Len() > db.flushThreshold

	if needFlush {
		var old *memtable
		db.mu.Lock()
		old = db.memtable
		db.memtable = newMemtable()
		db.mu.Unlock()
		err := db.wal.Sync()
		if err != nil {
			db.logger.Error("Failed to sync WAL", zap.Error(err))
		}
		go func(m *memtable) {
			db.flushMu.Lock()
			defer db.flushMu.Unlock()

			entries := m.ToColumnEntries()
			if err := db.sstManager.CreateSSTable(entries); err != nil {
				db.logger.Error("Flush failed", zap.Error(err))
			} else {
				db.logger.Info("Flushed memtable", zap.Int("entries", len(entries)))
				// Delete old WAL/Mark it safe for deletion
				db.wal.Info("Checkpoint")
			}
		}(old)
	}
}

// Get retrieves one “cell” by (partitionKey, clusteringKeys, columnName).
func (db *Database) Get(partKey []byte, clustering [][]byte, colName []byte) ([]byte, error) {
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
			return nil, err
		}
		val = res
	}
	return val, nil
}

func (db *Database) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.logger.Sync(); err != nil {
		db.logger.Error("Failed to wal on close", zap.Error(err))
	}
	// Flush current memtable
	if db.memtable != nil {
		Entries := db.memtable.ToColumnEntries()
		if err := db.sstManager.CreateSSTable(Entries); err != nil {
			db.logger.Error("Failed to flush memtable on close", zap.Error(err))
		}
		db.memtable = nil
	}

	// Clean up old memtable if it exists
	if db.oldMemtable != nil {
		Entries := db.oldMemtable.ToColumnEntries()
		if err := db.sstManager.CreateSSTable(Entries); err != nil {
			db.logger.Error("Failed to flush old memtable on close", zap.Error(err))
		}
		db.oldMemtable = nil
	}
}
