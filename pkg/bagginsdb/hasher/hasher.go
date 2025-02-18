package hasher

import (
	"sync"

	"go.uber.org/zap"
)

// Hasher is an abstraction that allows us to add nodes to a hash ring and get the appropriate nodes for a given key.
// It uses consistent hashing to map keys to nodes.
// Hasher sits above the hashRing implementation and adds the ability for logging and a better user facing API
type Hasher struct {
	consistencyLevel int
	hashRing         *hashRing
	logger           *zap.Logger
	sync.RWMutex
}

func NewHasher(config *HasherConfig) *Hasher {
	return &Hasher{consistencyLevel: config.ConsistencyLevel, hashRing: NewHashRing(config.Replicas, config.HashFn), logger: config.Logger}
}

// For now I don't wanna handle hot config reloads
// func (c *Node) SetConfig(config *NodeConfig) {
// 	c.Lock()
// 	defer c.Unlock()
// 	c.consistencyLevel = config.consistencyLevel
// }

func (c *Hasher) AddNode(nodes ...string) {
	c.hashRing.Add(nodes...)
}

func (c *Hasher) GetNodes(key string) []string {
	// Forward request to the appropriate node.
	return c.hashRing.Get(key)
}
