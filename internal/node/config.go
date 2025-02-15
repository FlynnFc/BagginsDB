package node

import "go.uber.org/zap"

const (
	ONE = iota
	QUORUM
	ALL
)

type NodeConfig struct {
	ConsistencyLevel int
	Replicas         int
	HashFn           Hash
	Logger           *zap.Logger
}
