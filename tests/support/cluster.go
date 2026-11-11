package support

import (
	"errors"
	"fmt"
	"time"

	"distributed-kv-store/internal/consensus"

	"github.com/hashicorp/raft"
)

// ErrNoLeader is returned when none of the supplied nodes reports itself as the
// Raft leader before the timeout.
var ErrNoLeader = errors.New("support: no Raft leader elected")

// WaitForLeader polls the supplied nodes until one reports itself as leader.
func WaitForLeader(nodes []*consensus.Node, timeout time.Duration) (*consensus.Node, error) {
	deadline := time.Now().Add(timeout)
	for {
		for _, node := range nodes {
			if node != nil && node.Raft.State() == raft.Leader {
				return node, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, ErrNoLeader
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// ApplyRawToLeader waits for a leader and applies the raw command bytes through
// Raft. It returns the FSM response and never retries, so tests can assert on a
// specific command outcome.
func ApplyRawToLeader(nodes []*consensus.Node, data []byte, timeout time.Duration) (interface{}, error) {
	leader, err := WaitForLeader(nodes, timeout)
	if err != nil {
		return nil, err
	}

	future := leader.Raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("apply command to leader: %w", err)
	}
	return future.Response(), nil
}
