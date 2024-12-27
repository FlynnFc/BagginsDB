package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/protos"
	"github.com/flynnfc/bagginsdb/server"
	"github.com/flynnfc/bagginsdb/simulation"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {
	go func() {
		// Start the pprof server on port 6060
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	simulation.Dts()
	simulation.Load()

	// Initialize logger
	logger := logger.InitLogger("bagginsdb")
	defer logger.Sync()
	// Database configuration
	dbConfig := database.Config{
		Host: "localhost",
	}

	// Quorum configuration

	// Create database server
	dbServer := server.NewDatabaseServer(logger, dbConfig)

	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()), // Use insecure credentials for testing
	}
	grpcServer := grpc.NewServer(opts...)

	// Register the database server
	protos.RegisterDatabaseServer(grpcServer, dbServer)

	// Enable reflection for debugging with grpcurl
	reflection.Register(grpcServer)

	// Listen on port 8080
	listener, err := net.Listen("tcp", ":8082")
	if err != nil {
		logger.Fatal("Failed to start TCP listener", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("gRPC server listening on :8082")

	// Start gRPC server
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal("Failed to serve gRPC server", zap.Error(err))
	}
}
