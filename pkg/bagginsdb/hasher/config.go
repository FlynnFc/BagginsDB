package hasher

import "go.uber.org/zap"

type HasherConfig struct {
	ConsistencyLevel int
	Replicas         int
	HashFn           Hash
	Logger           *zap.Logger
}
