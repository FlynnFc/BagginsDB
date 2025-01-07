package quorum

import (
	"errors"
	"sync"

	"github.com/flynnfc/bagginsdb/utils"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type RaftNode struct {
	Nodes  int
	Quorum int
	state  interface{}
	role   Role
	leader int
	mu     sync.Mutex
	log    []interface{}
	peers  []*RaftNode
}

func (r *RaftNode) Propose(value interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != Leader {
		return errors.New("only the leader can propose values")
	}

	// Append the proposed value to the log
	r.log = append(r.log, value)
	// Simulate reaching consensus
	if len(r.log) >= r.Quorum {
		r.state = value
		return nil
	}
	return errors.New("failed to reach consensus")
}

func (r *RaftNode) Commit() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == nil {
		return errors.New("no value proposed")
	}
	// Commit the value (in a real implementation, this would involve more steps)
	return nil
}

func (r *RaftNode) Abort() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = nil
	return nil
}

func (r *RaftNode) GetState() (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state, nil
}

func (r *RaftNode) SetState(state interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = state
	return nil
}

func (r *RaftNode) ElectLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// TODO Real
	r.role = Leader
	r.leader = 1 // Assume node 1 is the leader for simplicity
}

func (r *RaftNode) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.role == Leader
}

func (r *RaftNode) DistributeLoad(key string) (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != Leader {
		utils.Assert(r.role != Leader, "only the leader can distribute load")
	}

	// Simple load distribution based on key hash
	hash := hashKey(key)
	node := hash % r.Nodes
	if node == r.leader {
		return r.state, nil
	}
	// Send the key to the appropriate node
	// Send command and ctx so follower can return to the original client
	return nil, errors.New("key does not belong to this node")
}

func hashKey(key string) int {
	// Simple hash function (in a prod env, use a better hash function)
	hash := 0
	for i := 0; i < len(key); i++ {
		hash = int(key[i]) + ((hash << 5) - hash)
	}
	return hash
}

func SendCommand(ctx string, command func()) int {

	return 1
}
