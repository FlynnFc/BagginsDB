package node

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"go.uber.org/zap"
)

type Node struct {
	consistencyLevel int
	hashRing         *HashRing
	logger           *zap.Logger
	sync.RWMutex
}

func NewNode(config *NodeConfig) *Node {
	return &Node{consistencyLevel: config.ConsistencyLevel, hashRing: NewHashRing(config.Replicas, config.HashFn), logger: config.Logger}
}

// For now I don't wanna handle hot config reloads
// func (c *Node) SetConfig(config *NodeConfig) {
// 	c.Lock()
// 	defer c.Unlock()
// 	c.consistencyLevel = config.consistencyLevel
// }

type Response struct {
	status int
	data   []byte
}

func (c *Node) HandleRequest(req *Request) {
	// Check what nodes are responsible for the request.
	c.RLock()
	nodes := c.hashRing.Get(req.key)
	c.RUnlock()
	responses := make([]*Response, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		// Check if the nodes are available.
		go func(node string, i int, wg *sync.WaitGroup) {
			isUp := c.Ping(node)
			if isUp {
				// Perform the request.
				res, err := c.ForwardRequest(req)
				if err != nil {
					c.logger.Error(fmt.Sprintf("Failed to forward request to node %s", node), zap.Error(err))
				}
				responses[i] = res
			}
			wg.Done()
		}(node, i, &wg)
	}
	wg.Wait()

	// Check the consistency level.
	switch c.consistencyLevel {
	case ONE:
		if responses[0].status == 200 {
			handleConsistencyResponse(string(responses[0].data), true, context.Background(), nil)
		} else {
			handleConsistencyResponse(string(responses[0].data), false, context.Background(), nil)
		}
	case QUORUM:
		data, ok := c.IsQuorum(responses)
		handleConsistencyResponse(data, ok, context.Background(), nil)
		// Return request
	case ALL:
		ok := c.IsAll(responses)
		handleConsistencyResponse(string(responses[0].data), ok, context.Background(), nil)
		// Return request [0]
	}
}

func handleConsistencyResponse(d string, ok bool, ctx context.Context, w http.ResponseWriter) {
	if ok {
		w.Write([]byte(d))
	} else {
		http.Error(w, "Failed to retrieve data", http.StatusInternalServerError)
	}
}

func (c *Node) Ping(node string) bool {
	// Check if the node is available.
	return true
}

func (c *Node) IsAll(responses []*Response) bool {
	for _, res := range responses {
		if res.status != 200 {
			return false
		}
	}
	return true
}

func (c *Node) IsQuorum(responses []*Response) (string, bool) {
	if len(responses) == 0 {
		return "", false
	}

	// Map to count occurrences of each unique data value.
	frequency := make(map[string]int)
	for _, resp := range responses {
		// Convert []byte to string for comparison.
		key := string(resp.data)
		frequency[key]++
	}

	// Define the quorum threshold: more than half of the responses.
	quorumThreshold := len(responses)/2 + 1

	// Check if any data value has been returned by a quorum of nodes.
	for data, count := range frequency {
		fmt.Printf("Data %q occurred %d times\n", data, count)
		if count >= quorumThreshold {
			return data, true
		}
	}

	return "", false
}

func (c *Node) AddNode(nodes ...string) {
	c.hashRing.Add(nodes...)
}

type Request struct {
	key   string
	value []byte
}

func (c *Node) GetHash(key string) []string {
	// Forward request to the appropriate node.
	return c.hashRing.Get(key)
}

func (c *Node) ForwardRequest(req *Request) (*Response, error) {
	// Forward request to the appropriate node.
	return &Response{}, nil
}
