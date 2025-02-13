package node

import "go.uber.org/zap"

const (
	ONE = iota
	QUORUM
	ALL
)

type NodeConfig struct {
	consistencyLevel int
	replicas         int
	hashFn           Hash
	logger           *zap.Logger
}
