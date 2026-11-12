//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

// TestOneNodeFailureKeepsServing covers a single unreachable node: reads and
// writes continue and health reports degraded until the node returns.
func TestOneNodeFailureKeepsServing(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	value := bytes.Repeat([]byte("one-node-down-"), 128)
	put(t, c, "chaos-one", value, "rs32")
	waitWriteMeta(t, c, "chaos-one")

	var down string
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			down = n.ID
			break
		}
	}
	c.SetUnreachable(down)
	defer c.SetReachable(down)

	get(t, c, "chaos-one", value)
	put(t, c, "chaos-one-second", []byte("still-writable"), "rs32")
	get(t, c, "chaos-one-second", []byte("still-writable"))

	waitCondition(t, 10*time.Second, "health to report one unhealthy node", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 4 && health.GetDegraded()
	})

	c.SetReachable(down)
	waitCondition(t, 15*time.Second, "health to recover", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 5 && !health.GetDegraded()
	})
}

// TestTwoNodeFailureKeepsServing covers the documented two-failure tolerance:
// three live nodes reconstruct RS data and accept a degraded write.
func TestTwoNodeFailureKeepsServing(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)

	value := bytes.Repeat([]byte("two-nodes-down-"), 96)
	put(t, c, "chaos-two", value, "rs32")
	meta := waitWriteMeta(t, c, "chaos-two")
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	down := twoNonLeaderIDs(t, c, meta)
	c.SetUnreachable(down...)
	defer c.SetReachable(down...)

	get(t, c, "chaos-two", value)
	put(t, c, "chaos-two-second", []byte("degraded-but-durable"), "rs32")
	get(t, c, "chaos-two-second", []byte("degraded-but-durable"))

	c.SetReachable(down...)
	waitCondition(t, 20*time.Second, "repair to clear degraded state", func() bool {
		health := clusterHealth(t, c, c.Nodes[0].ID)
		return health.GetHealthyNodes() == 5 && health.GetDegradedStripes() == 0
	})
}

// TestThreeNodeFailureFailsClearly stops the leader and two followers: there is
// no quorum, so the remaining nodes must return an explicit application failure
// or a transport status rather than pretending to succeed.
func TestThreeNodeFailureFailsClearly(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	stopped := map[string]bool{leader.ID: true}
	for _, n := range c.Nodes {
		if len(stopped) == 3 {
			break
		}
		if n.ID != leader.ID {
			stopped[n.ID] = true
		}
	}
	for id := range stopped {
		c.StopNode(t, id)
	}

	var survivor *support.Node
	for _, n := range c.Nodes {
		if !stopped[n.ID] {
			survivor = n
			break
		}
	}
	if survivor == nil {
		t.Fatal("no survivor node found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := survivor.Client.Put(ctx, &pb.PutRequest{Key: "chaos-three", Value: []byte("x")})
	if err == nil && resp.GetSuccess() {
		t.Fatalf("write succeeded without a quorum: %+v", resp)
	}
	if err == nil && resp.GetMessage() == "" {
		t.Fatal("failed write did not explain itself")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := survivor.Client.Get(ctx, &pb.GetRequest{Key: "chaos-three"}); err == nil {
		t.Fatal("read succeeded without a quorum")
	}
}

// TestLeaderKillReelects stops the leader with a quorum still alive, waits for
// a replacement, verifies the committed value remains readable, then restarts
// the old leader and confirms it rejoins without corrupting the key.
func TestLeaderKillReelects(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	value := []byte("survives-leader-kill")
	put(t, c, "chaos-leader-kill", value, "rs32")
	waitWriteMeta(t, c, "chaos-leader-kill")

	oldLeader := leader.ID
	c.StopNode(t, oldLeader)

	var replacement *support.Node
	waitCondition(t, 20*time.Second, "replacement leader", func() bool {
		replacement, _ = c.WaitLeader(2 * time.Second)
		return replacement != nil && replacement.ID != oldLeader
	})

	waitCondition(t, 20*time.Second, "replacement leader to expose metadata", func() bool {
		_, err := replacement.Store.GetKey("chaos-leader-kill")
		return err == nil
	})
	get(t, c, "chaos-leader-kill", value)

	c.StartNode(t, oldLeader)
	waitCondition(t, 20*time.Second, "old leader to rejoin", func() bool {
		node := nodeByID(c, oldLeader)
		return node != nil && node.Raft != nil && node.Raft.Raft.Leader() != ""
	})
	get(t, c, "chaos-leader-kill", value)
}
