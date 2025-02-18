package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/consistency"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/db"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/hasher"
	"github.com/flynnfc/bagginsdb/protos"
)

// BagginsServer is the main server struct for BagginsDB.
// This struct is the highest abstraction available for BagginsDB.
// It handles the storage engine, the hash ring, node communication, and all things distributed systems.
type BagginsServer struct {
	protos.UnimplementedBagginsDBServiceServer // For forward compatibility.
	Mu                                         sync.Mutex
	localNode                                  *protos.Node
	ClusterNodes                               map[string]*protos.Node // Map of node id -> Node.
	db                                         *db.Database
	controlPlane                               *hasher.Hasher
	connPool                                   *ConnectionPool
}

// newServer creates a new instance of bagginsDBServer.
func NewServer(localNode *protos.Node) *BagginsServer {
	logger := logger.InitLogger("bagginsdb")
	defer logger.Sync()

	dbConfig := db.Config{
		MemTableSize: 1024 * 1024, // 1MB
	}

	db := db.NewDatabase(logger, dbConfig)

	nodeConfig := &hasher.HasherConfig{ConsistencyLevel: 1, Replicas: 1, Logger: logger}
	n := hasher.NewHasher(nodeConfig)

	cPool := NewConnectionPool(5*time.Second, 10*time.Second)

	s := &BagginsServer{
		localNode:    localNode,
		ClusterNodes: make(map[string]*protos.Node),
		db:           db,
		controlPlane: n,
		connPool:     cPool,
	}
	// Add ourselves to our view.
	s.ClusterNodes[localNode.GetId()] = localNode
	return s
}

// JoinCluster is called by a new node to join the cluster.
func (s *BagginsServer) JoinCluster(ctx context.Context, req *protos.JoinClusterRequest) (*protos.JoinClusterResponse, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	newNode := req.GetNode()
	if newNode == nil {
		return nil, fmt.Errorf("node information is missing")
	}
	// Add the new node to our local cluster view.
	s.ClusterNodes[newNode.GetId()] = newNode
	s.controlPlane.AddNode(newNode.GetAddress())
	// Prepare the list of all known nodes.
	var nodes []*protos.Node
	for _, n := range s.ClusterNodes {
		nodes = append(nodes, n)
	}

	log.Printf("JoinCluster: Node %s joined. Total nodes: %d", newNode.GetAddress(), len(nodes))
	return &protos.JoinClusterResponse{
		Success:      true,
		Message:      "Node joined successfully",
		ClusterNodes: nodes,
	}, nil
}

func convertStringSliceToByteSlice(strs []string) [][]byte {
	result := make([][]byte, len(strs))
	for i, s := range strs {
		result[i] = []byte(s)
	}
	return result
}

func (s *BagginsServer) handleLocalRequest(ctx context.Context, req *protos.Request) (*protos.Response, error) {
	var data []byte
	switch req.Type {
	case protos.RequestType_READ:
		d, err := s.db.Get([]byte(req.GetPartitionKey()), convertStringSliceToByteSlice(req.ClusteringKeys), []byte(req.ColumnName))
		if err != nil {
			return &protos.Response{
				Status:  500,
				Data:    []byte(err.Error()),
				Message: "ERROR",
			}, nil
		}
		data = d
	case protos.RequestType_WRITE:
		s.db.Put([]byte(req.GetPartitionKey()), convertStringSliceToByteSlice(req.ClusteringKeys), []byte(req.ColumnName), req.Value)
	}
	return &protos.Response{
		Status:  200,
		Data:    data,
		Message: "OK",
	}, nil
}

// HandleRequest processes a client read/write request.
func (s *BagginsServer) HandleRequest(ctx context.Context, req *protos.Request) (*protos.Response, error) {
	nodes := s.controlPlane.GetNodes(req.PartitionKey)
	// Determine the required number of responses.
	numNodes := len(nodes)
	var required int
	switch req.GetConsistencyLevel() {
	case consistency.ONE:
		required = 1
	case consistency.QUORUM:
		required = numNodes/2 + 1
	case consistency.ALL:
		required = numNodes
	default:
		// Fallback: treat unknown as ALL.
		required = numNodes
	}

	// Create a buffered channel to hold responses.
	responses := make(chan *protos.Response, numNodes/2)
	var wg sync.WaitGroup

	// Launch goroutines for each node.
	for _, n := range nodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			var res *protos.Response
			var err error
			if n != s.localNode.GetAddress() {
				log.Printf("HandleRequest: Forwarding request to node %s", n)
				res, err = s.ForwardRequest(ctx, &protos.ForwardedRequest{
					OriginalRequest: req,
					FromNode:        s.localNode,
				})
			} else {
				res, err = s.handleLocalRequest(ctx, req)
			}
			if err != nil {
				if err == context.Canceled {
					log.Printf("Request canceled for node %s another node responded faster", n)
					return
				}
				log.Printf("Error processing request on node %s: %v", n, err)
				return
			}
			responses <- res
		}(n)
	}

	// Close the responses channel once all goroutines complete.
	go func() {
		wg.Wait()
		close(responses)
	}()

	var collected []*protos.Response
	// TODO: Adjust this to timeout when it hits the p99.9 of the expected response time. and retry
	timeout := time.After(5 * time.Second)

collectLoop:
	for {
		select {
		case res, ok := <-responses:
			if !ok {
				break collectLoop
			}
			collected = append(collected, res)
			// If the consistency level is ONE, we can return immediately.
			if req.GetConsistencyLevel() == consistency.ONE && len(collected) >= 1 {
				return collected[0], nil
			}
			// For QUORUM or ALL, if we have enough responses, break out.
			if len(collected) >= required {
				break collectLoop
			}
		case <-timeout:
			log.Println("Timeout waiting for responses")
			break collectLoop
		}
	}

	// Resolve the best response from the collected responses.
	return s.resolveBestResponse(collected), nil
}

func (s *BagginsServer) resolveBestResponse(responses []*protos.Response) *protos.Response {
	for _, r := range responses {
		if r.GetStatus() == 200 {
			return r
		}
	}

	// Fallback: return the first response if none has 200.
	if len(responses) > 0 {
		return responses[0]
	}
	return nil
}

func (s *BagginsServer) ForwardRequest(ctx context.Context, req *protos.ForwardedRequest) (*protos.Response, error) {
	// We create a cancelable context to allow for early exit if we have enough responses.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes := s.controlPlane.GetNodes(req.OriginalRequest.PartitionKey)
	// Use a buffered channel to ensure goroutines don't block if the response is already received.
	responses := make(chan *protos.Response, len(nodes))
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, node := range nodes {
		// Capture the variable to avoid closure issues.
		node := node
		go func() {
			defer wg.Done()
			// Retrieve a connection from the pool.
			conn, err := s.connPool.GetConn(node)
			if err != nil {
				log.Printf("failed to get connection from pool for node %s: %s", node, err.Error())
				return
			}

			client := protos.NewBagginsDBServiceClient(conn)

			res, err := client.HandleRequest(ctx, req.OriginalRequest)
			if err != nil {
				log.Printf("Error forwarding request to node %s: %v", node, err)
				return
			}

			if res.GetStatus() == 200 {
				select {
				case responses <- res:
				case <-ctx.Done():
					// If the context is canceled, just exit.
					return
				}
			}
		}()
	}

	// Wait for the first successful response or the context to be done.
	var result *protos.Response
	select {
	case result = <-responses:
		// Cancel the context to signal other goroutines to stop their work.
		cancel()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for all goroutines to finish before returning.
	wg.Wait()

	return result, nil
}

// Gossip is used for exchanging state with peers.
func (s *BagginsServer) Gossip(ctx context.Context, req *protos.Request) (*protos.Response, error) {
	return s.handleLocalRequest(ctx, req)
}

// HeartBeat checks the liveness of a node.
func (s *BagginsServer) HeartBeat(ctx context.Context, req *protos.HealthCheck) (*protos.Response, error) {
	node := req.GetNode()
	log.Printf("HeartBeat: Received heartbeat from node %s", node.GetAddress())
	return &protos.Response{
		Status:  200,
		Data:    []byte("Heartbeat OK"),
		Message: "OK",
	}, nil
}

// JoinClusterClient dials a seed node and sends a JoinCluster request.
// It retries up to 5 times with exponential backoff.
// 1s, 2s, 4s, 8s, etc.
func (s *BagginsServer) JoinClusterClient(seedAddress string, localNode *protos.Node) (*protos.JoinClusterResponse, error) {
	const maxRetries = 5
	const baseDelay = 1 * time.Second
	var resp *protos.JoinClusterResponse
	var err error

	for i := 0; i < maxRetries; i++ {
		// Try to get a connection from the pool.
		conn, err := s.connPool.GetConn(seedAddress)
		if err != nil {
			delay := baseDelay * time.Duration(1<<i)
			log.Printf("failed to get connection from pool for node %s: %s. Retrying in %v...", seedAddress, err.Error(), delay)
			time.Sleep(delay)
			continue
		}

		// Create a client and execute the JoinCluster RPC.
		client := protos.NewBagginsDBServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err = client.JoinCluster(ctx, &protos.JoinClusterRequest{Node: localNode})
		defer cancel() // Ensure the context is canceled.

		if err != nil {
			delay := baseDelay * time.Duration(1<<i)
			log.Printf("join cluster RPC error: %v. Retrying in %v...", err, delay)
			time.Sleep(delay)
			continue
		}

		// Success!
		return resp, nil
	}

	return nil, fmt.Errorf("failed to join cluster after %d retries: %v", maxRetries, err)
}
