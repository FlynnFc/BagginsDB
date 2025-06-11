package db

import (
	"fmt"
	"os"
	"sync"

	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/truetime"
	"go.uber.org/zap"
)

// Database is our underlying storage engine that manages memtables & SSTables.
// It is fully ACID compliant on it's own, but we would need extra work to make it distributed (See BagginsdbServer).
type Database struct {
	logger            *zap.Logger // Our standard logger. Writes both to file and stdout.
	wal               *zap.Logger // Write Ahead Log. Ensures we don't lose data on crashes.
	config            Config
	memtable          *memtable
	oldMemtable       *memtable       // Used during flushes.
	sstManager        *sstableManager // Handles compaction, reading, and writing SSTables.
	mu                sync.RWMutex
	clock             truetime.TrueTime // An attempt at simulating Google's true time clock to eliminate distributed locks.
	flushMu           sync.Mutex
	compactionTrigger chan struct{}

	flushThreshold int // how many entries or how big the memtable is before flushing.
}

// NewDatabase creates a new DB instance, sets up the memtable, sstable manager, etc.
// Anything that fails in this call will cause a panic. Something is seriously wrong if this fails to run.
func NewDatabase(l *zap.Logger, c Config) *Database {
	clock := truetime.NewNTPTime("time.google.com")

	memtable := newMemtable()

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		l.Fatal("NODE_ID environment variable must be set")
		panic("NODE_ID environment variable must be set")
	}
	dir := fmt.Sprintf("%s/sst", nodeID) // directory to store sst files.
	bloomSize := uint(1000000)
	indexInterval := uint(10)

	sstManager, err := newSSTableManager(dir, bloomSize, indexInterval, l)
	if err != nil {
		l.Fatal("Failed to create SSTableManager", zap.Error(err))
		panic(err)
	}

	wal := logger.InitWALLogger("wal/wal")

	db := &Database{
		logger:            l,
		config:            c,
		memtable:          memtable,
		sstManager:        sstManager,
		clock:             clock,
		wal:               wal,
		compactionTrigger: make(chan struct{}, 1),
		flushThreshold:    c.MemTableSize, // Number of entries before we flush.
	}

	return db
}

func (db *Database) Put(partKey []byte, clustering [][]byte, colName []byte, value []byte) {
	// Write to the write ahead log.
	db.wal.Info("Put", zap.ByteString("partKey", partKey), zap.ByteStrings("clustering", clustering), zap.ByteString("colName", colName), zap.ByteString("value", value))
	// Then write to the memtable.
	// This is a simple write to the memtable. We don't need to worry about the SSTables here.

	timestamp, err := db.clock.NowInterval()
	if err != nil {
		db.logger.Error("Failed to get timestamp", zap.Error(err))
	}
	db.memtable.Put(Cell{
		PartitionKey:     partKey,
		ClusteringValues: clustering,
		ColumnName:       colName,
		Value:            value,
		Timestamp:        timestamp,
	})

	needFlush := db.memtable.Len() > db.flushThreshold

	if needFlush {
		var old *memtable
		// Instantiate a new memtable so other writes can continue.
		db.mu.Lock()
		old = db.memtable
		db.memtable = newMemtable()
		db.mu.Unlock()
		// Flush our write ahead log.
		// This is a blocking call. We need to ensure that the WAL is synced before we flush the memtable.
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
	// Check the active memtable(s)
	val := db.memtable.Get(partKey, clustering, colName)
	if val == nil && db.oldMemtable != nil {
		// Check old memtable (if there was a recent flush)
		val = db.oldMemtable.Get(partKey, clustering, colName)
	}
	// If there's no luck there we can check the SSTables.
	if val == nil {
		res, err := db.sstManager.Get(partKey, clustering, colName)
		if err != nil {
			db.logger.Error("Error reading from SSTableManager", zap.Error(err))
			return nil, err
		}
		val = res
	}
	return val, nil
}

// Close flushes the memtable(s) and closes any open files/SStables
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
	// Finally ensure the WAL is synced.
	db.wal.Sync()
}
