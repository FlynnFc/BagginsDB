package server

import (
	"context"
	"fmt"

	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/protos"
	"go.uber.org/zap"
)

type Server struct {
	db *database.Database
	protos.UnimplementedDatabaseServer
}

func NewDatabaseServer(l *zap.Logger, c database.Config) *Server {
	return &Server{
		db: database.NewDatabase(l, c),
	}
}

func (srv *Server) Get(ctx context.Context, gr *protos.GetRequest) (*protos.GetResponse, error) {
	println(gr.Key)
	val := srv.db.Get([]byte(gr.Key))
	if val == nil {
		return &protos.GetResponse{
			Value: "hi",
		}, nil
	}
	valBytes, ok := val.([]byte)
	println(valBytes)
	if !ok {
		return &protos.GetResponse{
			Value: "hi there",
		}, fmt.Errorf("value not found")
	}
	return &protos.GetResponse{Value: string(valBytes)}, nil
}

func (srv *Server) Set(ctx context.Context, sr *protos.SetRequest) (*protos.SetResponse, error) {
	srv.db.Put([]byte(sr.Key), []byte(sr.Value))

	return &protos.SetResponse{Success: true}, nil
}
