package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/internal/hasher"
	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/protos"
	"google.golang.org/grpc"
)

type bagginsServer struct {
	protos.UnimplementedBagginsDBServiceServer // For forward compatibility.

	Mu           sync.Mutex
	localNode    *protos.Node
	ClusterNodes map[string]*protos.Node // Map of node id -> Node
	db           *database.Database
	controlPlane *hasher.Hasher
	connPool     *ConnectionPool
}

// newServer creates a new instance of bagginsServer.
func NewServer(localNode *protos.Node) *bagginsServer {
	logger := logger.InitLogger("bagginsdb")
	defer logger.Sync()

	// Database configuration
	dbConfig := database.Config{
		Host: "localhost",
	}
	nodeConfig := &hasher.HasherConfig{ConsistencyLevel: 1, Replicas: 1, Logger: logger}
	db := database.NewDatabase(logger, dbConfig)
	n := hasher.NewHasher(nodeConfig)
	cPool := NewConnectionPool(5*time.Second, 10*time.Second)
	s := &bagginsServer{
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
func (s *bagginsServer) JoinCluster(ctx context.Context, req *protos.JoinClusterRequest) (*protos.JoinClusterResponse, error) {
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

func (s *bagginsServer) handleLocalRequest(ctx context.Context, req *protos.Request) (*protos.Response, error) {
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
func (s *bagginsServer) HandleRequest(ctx context.Context, req *protos.Request) (*protos.Response, error) {
	// For demonstration, we simply log the request and return a dummy response.
	log.Printf("HandleRequest: Received %v request for partition key: %s", req.GetType(), req.GetPartitionKey())
	nodes := s.controlPlane.GetHash(req.PartitionKey)
	// Determine the required number of responses.
	numNodes := len(nodes)
	var required int
	switch req.GetConsistencyLevel() {
	case hasher.ONE:
		required = 1
	case hasher.QUORUM:
		required = numNodes/2 + 1
	case hasher.ALL:
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

	// Collect responses until we have at least the required number.
	var collected []*protos.Response
	// Use a timeout in case some nodes never reply.
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
			if req.GetConsistencyLevel() == hasher.ONE && len(collected) >= 1 {
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

func (s *bagginsServer) resolveBestResponse(responses []*protos.Response) *protos.Response {
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

func (s *bagginsServer) ForwardRequest(ctx context.Context, req *protos.ForwardedRequest) (*protos.Response, error) {
	// Create a cancelable context.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodes := s.controlPlane.GetHash(req.OriginalRequest.PartitionKey)
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
			// The RPC call will use the cancelable context.
			res, err := client.HandleRequest(ctx, req.OriginalRequest)
			if err != nil {
				log.Printf("Error forwarding request to node %s: %v", node, err)
				return
			}

			if res.GetStatus() == 200 {
				// Use select to avoid sending on the channel if the context is already canceled.
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
func (s *bagginsServer) Gossip(ctx context.Context, req *protos.Request) (*protos.Response, error) {
	log.Printf("Gossip: Received gossip message for partition key: %s", req.GetPartitionKey())
	// For demo purposes, simply echo back a response.
	return &protos.Response{
		Status:  200,
		Data:    []byte("Gossip processed"),
		Message: "OK",
	}, nil
}

// HeartBeat checks the liveness of a node.
func (s *bagginsServer) HeartBeat(ctx context.Context, req *protos.HealthCheck) (*protos.Response, error) {
	node := req.GetNode()
	log.Printf("HeartBeat: Received heartbeat from node %s", node.GetAddress())
	return &protos.Response{
		Status:  200,
		Data:    []byte("Heartbeat OK"),
		Message: "OK",
	}, nil
}

// joinClusterClient dials a seed node and sends a JoinCluster request.
func JoinClusterClient(seedAddress string, localNode *protos.Node) (*protos.JoinClusterResponse, error) {
	conn, err := grpc.Dial(seedAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to dial seed node: %v", err)
	}
	defer conn.Close()

	client := protos.NewBagginsDBServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &protos.JoinClusterRequest{
		Node: localNode,
	}
	resp, err := client.JoinCluster(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("join cluster RPC error: %v", err)
	}
	return resp, nil
}
