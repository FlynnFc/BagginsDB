package server

import (
	"context"

	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/protos"
	"go.uber.org/zap"
)

// Server implements the Database gRPC interface.
type Server struct {
	DB *database.Database
	protos.UnimplementedDatabaseServer
}

// NewDatabaseServer creates a new Server with a wide-column Database.
func NewDatabaseServer(l *zap.Logger, c database.Config) *Server {
	return &Server{
		DB: database.NewDatabase(l, c),
	}
}

/*
Expects a GetRequest with:
  - partition_key
  - repeated clustering_keys
  - column_name
*/
func (srv *Server) Get(ctx context.Context, req *protos.GetRequest) (*protos.GetResponse, error) {
	// Extract partition key
	pk := []byte(req.PartitionKey)

	// Convert repeated string clustering_keys into [][]byte
	var cks [][]byte
	for _, ck := range req.ClusteringKeys {
		cks = append(cks, []byte(ck))
	}

	// Convert column_name
	col := []byte(req.ColumnName)

	// Retrieve from DB
	val := srv.DB.Get(pk, cks, col)
	if val == nil {
		// Return an empty string or some default
		return &protos.GetResponse{
			Value: "",
		}, nil
	}

	// Convert []byte -> string
	return &protos.GetResponse{Value: string(val)}, nil
}

/*
Expects a SetRequest with:
  - partition_key
  - repeated clustering_keys
  - column_name
  - value
*/
func (srv *Server) Set(ctx context.Context, req *protos.SetRequest) (*protos.SetResponse, error) {
	pk := []byte(req.PartitionKey)

	var cks [][]byte
	for _, ck := range req.ClusteringKeys {
		cks = append(cks, []byte(ck))
	}

	col := []byte(req.ColumnName)
	val := []byte(req.Value)

	// Insert into the wide-column DB
	srv.DB.Put(pk, cks, col, val)

	return &protos.SetResponse{Success: true}, nil
}
