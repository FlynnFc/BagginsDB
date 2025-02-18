package main

import (
	"fmt"
	"log"
	"net"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/server"
	"github.com/flynnfc/bagginsdb/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Retrieve configuration from environment variables.
	nodeID := os.Getenv("NODE_ID")
	nodeAddress := os.Getenv("NODE_ADDRESS")
	if nodeAddress == "" {
		nodeAddress = fmt.Sprintf("%s.bagginsdb.default.svc.cluster.local:50051", nodeID)
	}
	seedAddress := os.Getenv("SEED_NODE_ADDRESS")

	if nodeID == "" {
		panic("NODE_ID environment variable must be set")
	}

	// Create our local node with dummy tokens.
	localNode := &protos.Node{
		Id:      nodeID,
		Address: nodeAddress,
		Tokens:  []int64{100, 200, 300},
	}

	// Create a new GRPC server instance.
	serverImpl := server.NewServer(localNode)
	opts := []grpc.ServerOption{
		// For this example we're not requiring authentication.
		// In a production system, you would want to use a more secure method.
		grpc.Creds(insecure.NewCredentials()),
	}

	grpcServer := grpc.NewServer(opts...)

	protos.RegisterBagginsDBServiceServer(grpcServer, serverImpl)

	// Reflection allows us to use grpcurl to interact with the server.
	reflection.Register(grpcServer)
	lis, err := net.Listen("tcp", nodeAddress)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", nodeAddress, err)
	}

	// Start GRPC server in a separate goroutine.
	go func() {
		log.Printf("GRPC server starting on %s", nodeAddress)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("GRPC server error: %v", err)
		}
	}()

	// If a seed node is provided (and it's not us), attempt to join the cluster.
	if seedAddress != "" && seedAddress != nodeAddress {
		// Pause briefly to allow the seed node to be up incase we're starting at the same time.
		time.Sleep(2 * time.Second)

		log.Printf("Attempting to join cluster via seed node at %s", seedAddress)
		resp, err := serverImpl.JoinClusterClient(seedAddress, localNode)
		if err != nil {
			log.Printf("Error joining cluster: %v", err)
		} else {
			log.Printf("JoinCluster succeeded: %s", resp.Message)
			// Update our hash-ring with nodes returned from the seed.
			serverImpl.Mu.Lock()
			for _, n := range resp.ClusterNodes {
				serverImpl.ClusterNodes[n.GetId()] = n
			}
			serverImpl.Mu.Unlock()
		}
	} else {
		log.Println("No seed node provided or seed node same as local; starting as initial node.")
	}

	// Block forever.
	select {}
}
