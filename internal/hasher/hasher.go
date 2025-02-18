package hasher

import (
	"sync"

	"go.uber.org/zap"
)

type Hasher struct {
	consistencyLevel int
	hashRing         *HashRing
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

func (c *Hasher) GetHash(key string) []string {
	// Forward request to the appropriate node.
	return c.hashRing.Get(key)
}
