package db

import (
	"bytes"
	"math/rand"
	"sync"
)

// Value represents the data stored in each skipList node.
type Value struct {
	Data      []byte
	Timestamp int64
}

// node is one element of the skipList.
type node struct {
	key   []byte // This could be a composite key in a wide-column context
	value Value

	next []*node // next pointers for each “level” in the skipList
}

// skipList is the data structure that holds nodes in sorted order.
type skipList struct {
	head  *node
	level int
	size  int
	mu    sync.RWMutex
}

// NewskipList creates an empty skipList with a head node.
func newSkipList() *skipList {
	// We typically fix a maximum level, for example 16 or 32.
	const maxLevel = 16
	head := &node{
		next: make([]*node, maxLevel),
	}
	return &skipList{
		head:  head,
		level: 1, // current highest level in use
	}
}

// randomLevel decides how tall a new node might be.
func (sl *skipList) randomLevel() int {
	level := 1
	// Probability factor for incrementing level
	const p = 0.25
	for level < len(sl.head.next) && rand.Float32() < p {
		level++
	}
	return level
}

// Get searches for a node by key and returns its Value.
func (sl *skipList) Get(key []byte) *Value {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	x := sl.head
	// Traverse from top level down
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && bytes.Equal(x.key, key) {
		return &x.value
	}
	return nil
}

// Set inserts or updates a key-value pair (storing composite keys if wide-column).
func (sl *skipList) Set(key []byte, val Value) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// We’ll keep track of nodes we passed along the way (for updating next pointers).
	update := make([]*node, len(sl.head.next))
	current := sl.head

	// Start from the highest level and move down
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}
	current = current.next[0]

	// If key already exists, update it if the new timestamp is newer.
	if current != nil && bytes.Equal(current.key, key) {
		if val.Timestamp > current.value.Timestamp {
			current.value = val
		}
		return
	}

	// Key not found: insert a new node
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		// If our node is taller than current skipList, update skipList level
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}
	newNode := &node{
		key:   key,
		value: val,
		next:  make([]*node, newLevel),
	}
	// Re-wire pointers at each level
	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
	sl.size++
}

// Front returns the first data node in the skipList (lowest level).
func (sl *skipList) Front() *node {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return sl.head.next[0]
}

// Len returns a rough count of nodes in the skipList. (Optional: you can maintain a counter.)
func (sl *skipList) Len() int {
	return sl.size
}

func (sl *skipList) Entries() []struct {
	Key []byte
	Val Value
} {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var result []struct {
		Key []byte
		Val Value
	}
	x := sl.head.next[0]
	for x != nil {
		result = append(result, struct {
			Key []byte
			Val Value
		}{
			Key: x.key,
			Val: x.value,
		})
		x = x.next[0]
	}
	return result
}
