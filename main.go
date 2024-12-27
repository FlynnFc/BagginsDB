package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/protos"
	"github.com/flynnfc/bagginsdb/server"
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

	// Initialize logger
	logger := logger.InitLogger("bagginsdb")
	defer logger.Sync()

	// Database configuration
	dbConfig := database.Config{
		Host: "localhost",
	}

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

	// Listen on port 8082
	listener, err := net.Listen("tcp", ":8082")
	if err != nil {
		logger.Fatal("Failed to start TCP listener", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("gRPC server listening on :8082")

	// Graceful shutdown handling
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	// Run gRPC server in a goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			logger.Fatal("Failed to serve gRPC server", zap.Error(err))
		}
	}()

	// Wait for shutdown signal
	<-shutdownChan
	logger.Info("Shutdown signal received")

	// Create a context for graceful shutdown
	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop the gRPC server gracefully
	logger.Info("Stopping gRPC server")
	grpcServer.GracefulStop()

	// Close the database
	logger.Info("Closing database")
	dbServer.DB.Close() // Ensure the server struct exposes the DB instance for cleanup

	logger.Info("Shutdown complete")
}
