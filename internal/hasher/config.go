package hasher

import "go.uber.org/zap"

const (
	ONE = iota
	QUORUM
	ALL
)

type HasherConfig struct {
	ConsistencyLevel int
	Replicas         int
	HashFn           Hash
	Logger           *zap.Logger
}
