package main

import (
	"fmt"
	"log"
	"net"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/flynnfc/bagginsdb/protos"
	"github.com/flynnfc/bagginsdb/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Retrieve configuration from environment variables.
	nodeID := os.Getenv("NODE_ID")
	nodeAddress := fmt.Sprintf("%s.bagginsdb:50051", nodeID)
	seedAddress := os.Getenv("SEED_NODE_ADDRESS") // e.g., "localhost:50052"

	// Provide defaults if not set.
	if nodeID == "" {
		nodeID = "node-1"
	}
	if nodeAddress == "" {
		nodeAddress = "localhost:50051"
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
		grpc.Creds(insecure.NewCredentials()),
	}
	grpcServer := grpc.NewServer(opts...)
	protos.RegisterBagginsDBServiceServer(grpcServer, serverImpl)
	reflection.Register(grpcServer)
	// Start listening.
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
		// Pause briefly to allow the seed node to be up.
		time.Sleep(2 * time.Second)
		log.Printf("Attempting to join cluster via seed node at %s", seedAddress)
		resp, err := server.JoinClusterClient(seedAddress, localNode)
		if err != nil {
			log.Printf("Error joining cluster: %v", err)
		} else {
			log.Printf("JoinCluster succeeded: %s", resp.Message)
			// Update our cluster view with nodes returned from the seed.
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

// func main() {
// 	go func() {
// 		// Start the pprof server on port 6060
// 		fmt.Println(http.ListenAndServe("localhost:6060", nil))
// 	}()

// 	// Initialize logger
// 	logger := logger.InitLogger("bagginsdb")
// 	defer logger.Sync()

// 	// Database configuration
// 	dbConfig := database.Config{
// 		Host: "localhost",
// 	}

// 	// Create database server
// 	dbServer := server.NewDatabaseServer(logger, dbConfig)

// 	// Create gRPC server with options
// 	opts := []grpc.ServerOption{
// 		grpc.Creds(insecure.NewCredentials()),
// 	}
// 	grpcServer := grpc.NewServer(opts...)

// 	// Register the database server
// 	protos.RegisterDatabaseServer(grpcServer, dbServer)

// 	// Enable reflection for debugging with grpcurl
// 	reflection.Register(grpcServer)

// 	// Listen on port 8082
// 	listener, err := net.Listen("tcp", ":8082")
// 	if err != nil {
// 		logger.Fatal("Failed to start TCP listener", zap.Error(err))
// 		os.Exit(1)
// 	}
// 	logger.Info("gRPC server listening on :8082")

// 	// Graceful shutdown handling
// 	shutdownChan := make(chan os.Signal, 1)
// 	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

// 	// Run gRPC server
// 	go func() {
// 		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
// 			logger.Fatal("Failed to serve gRPC server", zap.Error(err))
// 		}
// 	}()

// 	// Wait for shutdown signal
// 	<-shutdownChan
// 	logger.Info("Shutdown signal received")

// 	// Create a context for graceful shutdown
// 	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	// Stop the gRPC server gracefully
// 	logger.Info("Stopping gRPC server")
// 	grpcServer.GracefulStop()

// 	// Close the database
// 	logger.Info("Closing database")
// 	dbServer.DB.Close() // Ensure the server struct exposes the DB instance for cleanup

// 	logger.Info("Shutdown complete")
// }
