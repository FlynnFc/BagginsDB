package main

import (
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/consistency"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/server"
	"github.com/flynnfc/bagginsdb/protos"
)

// Baggins server is an abstraction on top of the database package. This will handle inter node communication, replication and gossiping.
// the server package also contains the hash ring implementation. which you can use independently of the server.
func main() {
	// Create the information for our local node.
	localNode := &protos.Node{
		Id:      "node-1",
		Address: "localhost:8081",
		Tokens:  []int64{100, 200, 300},
	}
	server := server.NewServer(localNode)

	// You can access the hash ring like so
	// and ensure you utilize the lock provided as well
	server.Mu.Lock()
	server.ClusterNodes["node-1"] = localNode
	server.Mu.Unlock()

	// You can adjust your request to cluster per request. All requests are run through the HandleRequest function.
	// In this case we are writing "Hello, World!" to the key "key1" with a consistency level of QUORUM.
	// This means a majority of replicas must acknowledge the write before it is considered successful.
	server.HandleRequest(nil, &protos.Request{
		Type:             protos.RequestType_WRITE,
		ConsistencyLevel: consistency.QUORUM,
		PartitionKey:     "key1",
		Value:            []byte("Hello, World!"),
		ColumnName:       "data-col",
		ClusteringKeys:   []string{}, // empty clustering keys
	})

	// In this case we are writing "Dangerous!" to the key "key2" with a consistency level of ONE.
	// Only one replica must acknowledge the write before it is considered successful. (Not recommended for production)
	server.HandleRequest(nil, &protos.Request{
		Type:             protos.RequestType_WRITE,
		ConsistencyLevel: consistency.ONE,
		PartitionKey:     "key2",
		Value:            []byte("Dangerous!"),
		ColumnName:       "data-col",
		ClusteringKeys:   []string{}, // empty clustering keys
	})

	// In this case we are reading the data assigned to the key "key2" with a consistency level of ALL.
	// For this read to be returned successfully, all nodes must return the same data. (Also not recommended for production in-case replicas are down/not reachable)
	server.HandleRequest(nil, &protos.Request{
		Type:             protos.RequestType_READ,
		ConsistencyLevel: consistency.ALL,
		PartitionKey:     "key2",
		ColumnName:       "data-col",
		ClusteringKeys:   []string{}, // empty clustering keys
	})

	// Setting up some dummy data to test the forwarding of requests
	dummyRequest := &protos.Request{
		Type:             protos.RequestType_READ,
		ConsistencyLevel: consistency.ALL,
		PartitionKey:     "key2",
		ColumnName:       "data-col",
		ClusteringKeys:   []string{}, // empty clustering keys
	}

	// In the case a server receives a request that has a key that is not in it's hash ring range. It can forward the request and context to the correct node.
	server.ForwardRequest(nil, &protos.ForwardedRequest{
		OriginalRequest: dummyRequest,
		FromNode:        localNode,
	})
}
