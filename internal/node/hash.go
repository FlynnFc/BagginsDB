package node

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Hash func(data []byte) uint32

type HashRing struct {
	hash     Hash           // The hash function to use.
	replicas int            // Number of virtual nodes per actual node.
	keys     []int          // Sorted hash ring.
	hashMap  map[int]string // Mapping from virtual node hash to the real node.
	sync.RWMutex
}

func NewHashRing(replicas int, fn Hash) *HashRing {
	m := &HashRing{
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if fn != nil {
		m.hash = fn
	} else {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *HashRing) Add(nodes ...string) {
	m.Lock()
	defer m.Unlock()

	for _, node := range nodes {
		// Add virtual nodes.
		for i := 0; i < m.replicas; i++ {
			// Create a unique key for each replica.
			hashKey := int(m.hash([]byte(strconv.Itoa(i) + node)))
			m.keys = append(m.keys, hashKey)
			m.hashMap[hashKey] = node
		}
	}
	// Sort the keys to enable binary search.
	sort.Ints(m.keys)
}

// Get returns the closest node in the hash ring for the provided key.
func (m *HashRing) Get(key string) []string {
	m.RLock()
	defer m.RUnlock()

	if len(m.keys) == 0 {
		return nil
	}

	// Compute the hash of the key.
	hashKey := int(m.hash([]byte(key)))
	// Use binary search to find the first node with a hash >= hashKey.
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hashKey
	})

	// If we've gone past the end, wrap around to the first node.
	if idx == len(m.keys) {
		idx = 0
	}
	output := make([]string, m.replicas)
	for i := 0; i < m.replicas; i++ {
		output[i] = m.hashMap[m.keys[(idx+i)]]
	}
	return output
}
