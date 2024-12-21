package database

import (
	"sync"

	"github.com/flynnfc/bagginsdb/internal/truetime"
	"go.uber.org/zap"
)

type Config struct {
	Host             string
	MemTableSize     int
	ConsistencyLevel int
}

type Database struct {
	logger            *zap.Logger
	config            Config
	memtable          *memtable
	oldMemTable       *memtable
	sstManager        *SSTableManager
	clock             *truetime.TrueTime
	mu                sync.RWMutex
	compactionMu      sync.Mutex
	flushMu           sync.Mutex
	compactionTrigger chan struct{}
	flushTrigger      chan struct{}
	// You could store a threshold and intervals for compaction here if desired.
	flushThreshold int
}

func NewDatabase(l *zap.Logger, c Config) *Database {
	clock := truetime.NewTrueTime(l)
	clock.Run()
	memtable := NewMemtable()

	// Instead of making a file name, we’ll use a directory for SSTables.
	// The SSTableManager expects a directory where it can create multiple SSTable files.
	dir := "sst"
	bloomSize := uint(1000000) // Increase size
	indexInterval := 10        // Adjust as needed   // Create an index entry every 100 keys (tune as needed)

	sstManager, err := NewSSTableManager(dir, bloomSize, indexInterval, l)
	if err != nil {
		l.Fatal("Failed to create SSTableManager", zap.Error(err))
		panic(err)
	}

	db := &Database{
		logger:            l,
		config:            c,
		memtable:          memtable,
		sstManager:        sstManager,
		clock:             clock,
		compactionTrigger: make(chan struct{}, 1),
		flushThreshold:    1024 * 100, // 100KB
	}
	db.startCompactionWorker()
	return db
}

func (d *Database) Put(key []byte, value interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Insert into memtable
	d.memtable.Put(key, value.([]byte), d.clock.Now())

	// Check if we need to flush
	if d.memtable.skiplist.Len() > d.flushThreshold {
		d.oldMemTable = d.memtable
		d.memtable = NewMemtable()

		go func(oldMemTable *memtable) {
			d.flushMu.Lock()
			if err := d.sstManager.FlushMemtable(oldMemTable); err != nil {
				d.logger.Error("Failed to write memtable to SSTable", zap.Error(err))
			}
			d.flushMu.Unlock()
		}(d.oldMemTable)

		d.oldMemTable = nil

		// Notify background compaction if needed
		if len(d.sstManager.sstables) > 10 { // arbitrary condition
			select {
			case d.compactionTrigger <- struct{}{}: // Signal compaction worker
			default:
				// Avoid blocking if the compaction worker is already notified
			}
		}
	}
}

// Background compaction worker
func (d *Database) startCompactionWorker() {
	go func() {
		for range d.compactionTrigger {
			d.compactionMu.Lock()
			if err := d.sstManager.Compact(); err != nil {
				d.logger.Error("Failed to compact SSTables", zap.Error(err))
			}
			d.compactionMu.Unlock()
		}
	}()
}

func (d *Database) Get(key interface{}) interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check memtable first
	value := d.memtable.Get(key.([]byte))
	if value == nil {
		// If not in memtable, check SSTables
		val, err := d.sstManager.Get(key.([]byte))
		if err != nil {
			d.logger.Error("Error reading from SSTableManager", zap.Error(err))
			return nil
		}
		value = val
	}
	return value
}

// (Optional) Method to force a compaction if needed.
func (d *Database) Compact() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.sstManager.Compact(); err != nil {
		d.logger.Error("Failed to compact SSTables", zap.Error(err))
	}
}
