package database

import (
	"sync"

	"github.com/flynnfc/bagginsdb/internal/truetime"
	"github.com/huandu/skiplist"
)

// Value represents a value in the memtable with a timestamp.
type Value struct {
	Data      []byte
	Timestamp truetime.Timestamp
}

// memtable represents an in-memory table for an LSM tree.
type memtable struct {
	skiplist *skiplist.SkipList
	mu       sync.RWMutex
}

// NewMemtable creates a new memtable.
func NewMemtable() *memtable {
	list := skiplist.New(skiplist.Bytes)
	return &memtable{skiplist: list}
}

// Put inserts or updates a key-value pair in the memtable.
func (m *memtable) Put(key []byte, value []byte, ts truetime.Timestamp) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skiplist.Set(key, Value{Data: value, Timestamp: ts})
}

// Get retrieves the value for a given key from the memtable.
func (m *memtable) Get(key []byte) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node := m.skiplist.Get(key)
	if node == nil {
		return nil
	}
	return node.Value.(Value).Data
}

// Delete marks a key as deleted in the memtable.
func (m *memtable) Delete(key []byte, ts truetime.Timestamp) {
	m.skiplist.Set(key, Value{Data: nil, Timestamp: ts})
}
