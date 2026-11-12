//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

func waitCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func waitWriteMeta(t *testing.T, c *support.Cluster, key string) storage.KeyMeta {
	t.Helper()
	var meta storage.KeyMeta
	waitCondition(t, 20*time.Second, "metadata for "+key, func() bool {
		for _, n := range c.Nodes {
			if n.Store == nil {
				continue
			}
			current, err := n.Store.GetKey(key)
			if err != nil || current.Version != 1 {
				return false
			}
			meta = current
		}
		return meta.Key != ""
	})
	return meta
}

func nodeByID(c *support.Cluster, id string) *support.Node {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func placementFor(meta storage.KeyMeta, nodeID string) storage.ShardPlacement {
	for _, placement := range meta.Placement {
		if placement.NodeID == nodeID {
			return placement
		}
	}
	return storage.ShardPlacement{}
}

func totalFragments(c *support.Cluster, stripeID string) int {
	total := 0
	for _, n := range c.Nodes {
		if n.Store == nil {
			continue
		}
		refs, err := n.Store.ListFragments(stripeID)
		if err != nil {
			continue
		}
		total += len(refs)
	}
	return total
}

func waitFragmentTotal(t *testing.T, c *support.Cluster, stripeID string, want int) {
	t.Helper()
	waitCondition(t, 20*time.Second, "expected fragment total", func() bool {
		return totalFragments(c, stripeID) == want
	})
}

func twoNonLeaderIDs(t *testing.T, c *support.Cluster, meta storage.KeyMeta) []string {
	t.Helper()
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	var ids []string
	seen := map[string]bool{}
	for _, placement := range meta.Placement {
		if placement.NodeID != leader.ID && !seen[placement.NodeID] {
			seen[placement.NodeID] = true
			ids = append(ids, placement.NodeID)
		}
		if len(ids) == 2 {
			return ids
		}
	}
	t.Fatalf("wanted two non-leader owners, got %v", ids)
	return nil
}

func put(t *testing.T, c *support.Cluster, key string, value []byte, mode string) *pb.PutResponse {
	t.Helper()
	resp := c.Put(t, &pb.PutRequest{Key: key, Value: value, Mode: mode})
	if !resp.GetSuccess() {
		t.Fatalf("Put(%q) rejected: %s", key, resp.GetMessage())
	}
	return resp
}

func get(t *testing.T, c *support.Cluster, key string, want []byte) {
	t.Helper()
	resp, err := c.Get(t, key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !resp.GetSuccess() || !bytes.Equal(resp.GetValue(), want) {
		t.Fatalf("Get(%q) success=%v value=%d bytes", key, resp.GetSuccess(), len(resp.GetValue()))
	}
}

func clusterHealth(t *testing.T, c *support.Cluster, nodeID string) *pb.ClusterHealthResponse {
	t.Helper()
	node := nodeByID(c, nodeID)
	if node == nil || node.Client == nil {
		t.Fatalf("node %s is not available", nodeID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	health, err := node.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
	if err != nil {
		t.Fatalf("ClusterHealth on %s: %v", nodeID, err)
	}
	return health
}
