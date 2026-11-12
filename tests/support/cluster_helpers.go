package support

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"distributed-kv-store/internal/consensus"

	pb "distributed-kv-store/proto"

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

// Get sends a read to the current leader and returns the RPC result, including
// application and integrity status errors.
func (c *Cluster) Get(t testing.TB, key string) (*pb.GetResponse, error) {
	t.Helper()
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("support: %v", err)
	}
	return c.GetVia(t, leader.ID, key)
}

// GetVia sends a read through a specific node, exercising forwarding.
func (c *Cluster) GetVia(t testing.TB, nodeID, key string) (*pb.GetResponse, error) {
	t.Helper()
	c.mu.RLock()
	n, ok := c.byID[nodeID]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("support: unknown node %q", nodeID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return n.Client.Get(ctx, &pb.GetRequest{Key: key})
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
