package coordinator

import "go.uber.org/zap"

const (
	ONE = iota
	QUORUM
	ALL
)

type CoordinatorConfig struct {
	consistencyLevel int
	replicas         int
	hashFn           Hash
	logger           *zap.Logger
}
