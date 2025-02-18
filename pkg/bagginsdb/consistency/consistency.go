package consistency

// ConsistencyLevel is an enum that represents the consistency level of a read or write operation.
// The recommended value is QUORUM in most cases.
// Both ONE and ALL have a higher likelihood of returning stale data/hanging if a node is down.
const (
	ONE = iota
	QUORUM
	ALL
)
