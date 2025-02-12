package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/protos"
	"github.com/flynnfc/bagginsdb/server"
	"github.com/flynnfc/bagginsdb/simulation"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

func main() {
	go func() {
		// Start the pprof server on port 6060
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	// simulation.Dts()
	simulation.Load()

	// Initialize logger
	logger := logger.InitLogger("bagginsdb")
	defer logger.Sync()

	// Database configuration
	dbConfig := database.Config{
		Host: "localhost",
	}

	// Create database server
	dbServer := server.NewDatabaseServer(logger, dbConfig)

	// Setup metrics.
	srvMetrics := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		),
	)

	reg := prometheus.NewRegistry()
	reg.MustRegister(srvMetrics)
	exemplarFromContext := func(ctx context.Context) prometheus.Labels {
		if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
			return prometheus.Labels{"traceID": span.TraceID().String()}
		}
		return nil
	}

	// Set up OTLP tracing (stdout for debug).
	exporter, err := stdout.New(stdout.WithPrettyPrint())
	if err != nil {
		logger.Fatal("failed to create OTLP stdout exporter", zap.Error(err))
		os.Exit(1)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	defer func() { _ = exporter.Shutdown(context.Background()) }()

	// Setup metric for panic recoveries.
	panicsTotal := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "grpc_req_panics_recovered_total",
		Help: "Total number of gRPC requests recovered from internal panic.",
	})
	grpcPanicRecoveryHandler := func(p any) (err error) {
		panicsTotal.Inc()
		logger.Fatal("recovered from panic", zap.Any("panic", p), zap.String("stack", string(debug.Stack())))
		return status.Errorf(codes.Internal, "%s", p)
	}

	// Create gRPC server with options
	opts := []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()), // Use insecure credentials for testing
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(grpcprom.WithExemplarFromContext(exemplarFromContext)),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		),
		grpc.ChainStreamInterceptor(
			srvMetrics.StreamServerInterceptor(grpcprom.WithExemplarFromContext(exemplarFromContext)),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		),
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
	// simulation.Load()

	// Graceful shutdown handling
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	// Run gRPC server
	go func() {
		if err := grpcServer.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			logger.Fatal("Failed to serve gRPC server", zap.Error(err))
		}
	}()

	// Run metrics endpoint
	go func() {
		// metrics.AddMetricsToPrometheusRegistry()
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Serving Prometheus metrics on :9092/metrics")
		if err := http.ListenAndServe(":9092", nil); err != nil {
			log.Fatalf("failed to serve http: %v", err)
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
