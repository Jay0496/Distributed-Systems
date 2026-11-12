//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

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
	// Wait for the leader's detector to observe the outage instead of sleeping.
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	waitCondition(t, 15*time.Second, "detector to report two stopped nodes", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 3
	})
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

// TestOrphanSweepRespectsGracePeriod writes unreferenced fragments directly to
// a real SQLite store and proves the sweeper removes only fragments older than
// the grace period.
func TestOrphanSweepRespectsGracePeriod(t *testing.T) {
	c := support.StartCluster(t, 5)
	node := c.Nodes[0]
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

	old := []byte("old-orphan")
	if err := node.Store.PutFragment(storage.Fragment{
		StripeID:   "orphan-old",
		ShardIndex: 0,
		Data:       old,
		Checksum:   redundancy.Checksum(old),
		CreatedAt:  now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("PutFragment old: %v", err)
	}
	recent := []byte("recent-in-flight")
	if err := node.Store.PutFragment(storage.Fragment{
		StripeID:   "orphan-recent",
		ShardIndex: 1,
		Data:       recent,
		Checksum:   redundancy.Checksum(recent),
		CreatedAt:  now.Add(-time.Second),
	}); err != nil {
		t.Fatalf("PutFragment recent: %v", err)
	}

	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       node.Store,
		SelfNode:    node.ID,
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Logf:        t.Logf,
	})
	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if result.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", result.Deleted)
	}
	if _, err := node.Store.GetFragment("orphan-old", 0); err == nil {
		t.Fatal("old orphan fragment was not deleted")
	}
	if _, err := node.Store.GetFragment("orphan-recent", 1); err != nil {
		t.Fatalf("recent in-flight fragment was deleted: %v", err)
	}
}
