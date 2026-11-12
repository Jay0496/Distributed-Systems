//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"
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
	waitCondition(t, 20*time.Second, "five fragments", func() bool {
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		health, err := leader.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		return err == nil && health.GetHealthyNodes() == 4 && health.GetDegraded()
	})

	c.SetReachable(down)
	waitCondition(t, 15*time.Second, "health to recover", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		health, err := leader.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		return err == nil && health.GetHealthyNodes() == 5 && !health.GetDegraded()
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		health, err := c.Nodes[0].Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		if err != nil {
			return false
		}
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

// TestRestartRefillsReturningNode deletes two owners' fragments, restarts those
// nodes, and expects the leader's detector-driven repair to refill five shards.
func TestRestartRefillsReturningNode(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)

	value := bytes.Repeat([]byte("restart-refill-"), 111)
	put(t, c, "chaos-restart", value, "rs32")
	meta := waitWriteMeta(t, c, "chaos-restart")
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	owners := twoNonLeaderIDs(t, c, meta)
	for _, id := range owners {
		n := nodeByID(c, id)
		if err := n.Store.DeleteFragment(meta.StripeID, placementFor(meta, id).ShardIndex); err != nil {
			t.Fatalf("delete fragment on %s: %v", id, err)
		}
	}
	for _, id := range owners {
		c.StopNode(t, id)
	}
	time.Sleep(500 * time.Millisecond)
	for _, id := range owners {
		c.StartNode(t, id)
	}

	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
	get(t, c, "chaos-restart", value)
}

// TestCorruptShardRepairsWithForce corrupts one replica's bytes while keeping a
// matching fragment checksum, then asks for a forced repair and verifies the
// committed shard checksum returns.
func TestCorruptShardRepairsWithForce(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	value := bytes.Repeat([]byte("corrupt-me-"), 73)
	put(t, c, "chaos-corrupt", value, "rs32")
	meta := waitWriteMeta(t, c, "chaos-corrupt")
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	var target storage.ShardPlacement
	for _, placement := range meta.Placement {
		if placement.NodeID != leader.ID {
			target = placement
			break
		}
	}
	owner := nodeByID(c, target.NodeID)
	frag, err := owner.Store.GetFragment(meta.StripeID, target.ShardIndex)
	if err != nil {
		t.Fatalf("get fragment: %v", err)
	}
	corrupt := append([]byte(nil), frag.Data...)
	corrupt[0] ^= 0xff
	if err := owner.Store.PutFragment(storage.Fragment{
		StripeID:   meta.StripeID,
		ShardIndex: target.ShardIndex,
		IsParity:   frag.IsParity,
		Data:       corrupt,
		Checksum:   redundancy.Checksum(corrupt),
	}); err != nil {
		t.Fatalf("corrupt fragment: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := leader.Client.Repair(ctx, &pb.RepairRequest{
		Key:      "chaos-corrupt",
		StripeId: meta.StripeID,
		Force:    true,
	})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Repair rejected: %s", resp.GetMessage())
	}

	waitCondition(t, 20*time.Second, "corrupt shard repair", func() bool {
		refs, err := owner.Store.ListFragments(meta.StripeID)
		if err != nil {
			return false
		}
		for _, ref := range refs {
			if ref.ShardIndex == target.ShardIndex {
				return ref.Checksum == redundancy.Checksum(frag.Data)
			}
		}
		return false
	})
}

// TestPartitionDegradesAndRecovers drops traffic to two nodes, proves reads and
// writes still work, and waits for the repairer to clear degraded state.
func TestPartitionDegradesAndRecovers(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	value := []byte("partition-value")
	put(t, c, "chaos-partition", value, "rs32")
	meta := waitWriteMeta(t, c, "chaos-partition")
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
	down := twoNonLeaderIDs(t, c, meta)

	c.SetUnreachable(down...)
	defer c.SetReachable(down...)

	get(t, c, "chaos-partition", value)
	put(t, c, "chaos-partition-second", []byte("during-partition"), "rs32")
	get(t, c, "chaos-partition-second", []byte("during-partition"))

	waitCondition(t, 15*time.Second, "partition health degradation", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		health, err := leader.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		return err == nil && health.GetHealthyNodes() == 3 && health.GetDegraded()
	})

	c.SetReachable(down...)
	waitCondition(t, 25*time.Second, "partition repair", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		health, err := leader.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		return err == nil && health.GetHealthyNodes() == 5 && !health.GetDegraded()
	})
}

// TestMetadataOnlySnapshotRestoreRefillsFragments deletes a node's local
// fragments while leaving replicated metadata intact, which is the state after
// restoring a metadata-only snapshot. Repair must rebuild the local fragments.
func TestMetadataOnlySnapshotRestoreRefillsFragments(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	value := bytes.Repeat([]byte("snapshot-refill-"), 67)
	put(t, c, "chaos-snapshot", value, "rs32")
	meta := waitWriteMeta(t, c, "chaos-snapshot")
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	var target storage.ShardPlacement
	for _, placement := range meta.Placement {
		if placement.NodeID != leader.ID {
			target = placement
			break
		}
	}
	owner := nodeByID(c, target.NodeID)
	if err := owner.Store.DeleteFragment(meta.StripeID, target.ShardIndex); err != nil {
		t.Fatalf("delete local fragment: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := leader.Client.Repair(ctx, &pb.RepairRequest{Key: "chaos-snapshot", StripeId: meta.StripeID})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Repair rejected: %s", resp.GetMessage())
	}
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
	get(t, c, "chaos-snapshot", value)
}
