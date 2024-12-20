package database

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const maxLevel = 32 // Maximum number of levels in the skiplist
const p = 0.25      // Probability for level increase

// Node represents an element in the skiplist
type Node struct {
	key   []byte
	value Value
	next  []*Node
}

// SkipList represents the skiplist
type SkipList struct {
	mutex   sync.RWMutex
	head    *Node
	height  int
	length  int
	randSrc rand.Source
}

// NewSkipList creates and initializes a new skiplist
func NewSkipList() *SkipList {
	return &SkipList{
		head: &Node{
			next: make([]*Node, maxLevel),
		},
		randSrc: rand.NewSource(time.Now().UnixNano()),
		height:  1,
		length:  0,
	}
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	level := 1
	for ; level < maxLevel && rand.New(sl.randSrc).Float64() < p; level++ {
	}
	return level
}

// Put inserts or updates a key-value pair in the skiplist
func (sl *SkipList) Set(key []byte, value Value) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	update := make([]*Node, maxLevel)
	current := sl.head

	for i := sl.height - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	// Check if the key exists
	if current.next[0] != nil && bytes.Equal(current.next[0].key, key) {
		current.next[0].value = value
		return
	}

	// Insert new node
	level := sl.randomLevel()
	if level > sl.height {
		for i := sl.height; i < level; i++ {
			update[i] = sl.head
		}
		sl.height = level
	}

	node := &Node{
		key:   key,
		value: value,
		next:  make([]*Node, level),
	}

	for i := 0; i < level; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	sl.length++
}

// Get retrieves the value for a given key
func (sl *SkipList) Get(key []byte) Value {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	current := sl.head
	for i := sl.height - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
	}

	current = current.next[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.value
	}
	return Value{}
}

// Delete removes a key-value pair from the skiplist
func (sl *SkipList) Delete(key []byte) bool {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	update := make([]*Node, maxLevel)
	current := sl.head

	for i := sl.height - 1; i >= 0; i-- {
		for current.next[i] != nil && bytes.Compare(current.next[i].key, key) < 0 {
			current = current.next[i]
		}
		update[i] = current
	}

	current = current.next[0]
	if current == nil || !bytes.Equal(current.key, key) {
		return false
	}

	for i := 0; i < sl.height; i++ {
		if update[i].next[i] != current {
			break
		}
		update[i].next[i] = current.next[i]
	}

	for sl.height > 1 && sl.head.next[sl.height-1] == nil {
		sl.height--
	}

	sl.length--
	return true
}

// Len returns the number of elements in the skiplist
func (sl *SkipList) Len() int {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	return sl.length
}

// Front returns the first element in the skiplist
func (sl *SkipList) Front() *Node {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	if sl.head.next[0] == nil {
		return nil
	}
	return sl.head.next[0]
}

// Next returns the next element in the skiplist for a given node
func (n *Node) Next() *Node {
	if n == nil || n.next[0] == nil {
		return nil
	}
	return n.next[0]
}
