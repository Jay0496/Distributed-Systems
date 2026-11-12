package integration

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

// TestMetadataSnapshotRestoreRepairsFragments deletes one node's SQLite file
// after forcing a Raft snapshot, restarts the node, and proves that metadata is
// restored from the snapshot while repair refills the missing local fragment.
func TestMetadataSnapshotRestoreRepairsFragments(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	value := bytes.Repeat([]byte("snapshot-restore-"), 128)
	resp := c.Put(t, &pb.PutRequest{Key: "snapshot-key", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "snapshot-key", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	var target storage.ShardPlacement
	for _, placement := range meta.Placement {
		if placement.NodeID != leader.ID {
			target = placement
			break
		}
	}
	if target.NodeID == "" {
		t.Fatal("no non-leader owner found")
	}

	targetNode := c.Node(t, target.NodeID)
	if err := targetNode.Raft.Raft.Snapshot().Error(); err != nil {
		t.Fatalf("force snapshot on %s: %v", target.NodeID, err)
	}

	c.StopNode(t, target.NodeID)
	c.RemoveStore(t, target.NodeID)
	c.StartNode(t, target.NodeID)

	// Metadata must be repopulated from Raft state into the fresh database.
	c.WaitFor(t, 20*time.Second, "metadata restore on "+target.NodeID, func() bool {
		if targetNode.Store == nil {
			return false
		}
		current, err := targetNode.Store.GetKey("snapshot-key")
		return err == nil && current.StripeID == meta.StripeID
	})

	if refs := c.FragmentRows(t, target.NodeID, meta.StripeID); len(refs) != 0 {
		t.Fatalf("fresh store unexpectedly has %d local fragments", len(refs))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repair, err := leader.Client.Repair(ctx, &pb.RepairRequest{Key: "snapshot-key", StripeId: meta.StripeID})
	if err != nil || !repair.GetSuccess() {
		t.Fatalf("Repair = %+v, %v", repair, err)
	}

	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
	_, err = c.Get(t, "snapshot-key")
	if err != nil {
		t.Fatalf("Get after snapshot restore: %v", err)
	}
	got, err := c.Get(t, "snapshot-key")
	if err != nil || !bytes.Equal(got.GetValue(), value) {
		t.Fatalf("Get after restore returned %d bytes, %v", len(got.GetValue()), err)
	}
}

// TestSQLiteSurvivesCleanRestart stops a follower and starts it again from the
// same data directory, proving Raft log replay and SQLite reopen preserve the
// committed metadata and local fragments while the rest of the cluster serves.
func TestSQLiteSurvivesCleanRestart(t *testing.T) {
	c := support.StartCluster(t, 5)
	value := []byte("durable-value")
	resp := c.Put(t, &pb.PutRequest{Key: "durable", Value: value, Mode: "replication"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "durable", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	follower := c.NonLeaderIDs(t)[0]
	c.StopNode(t, follower)
	// The rest of the cluster keeps serving while the follower is down.
	if _, err := c.Get(t, "durable"); err != nil {
		t.Fatalf("Get while follower is down: %v", err)
	}
	c.StartNode(t, follower)

	c.WaitFor(t, 30*time.Second, "metadata after follower restart", func() bool {
		node := c.Node(t, follower)
		if node.Store == nil {
			return false
		}
		current, err := node.Store.GetKey("durable")
		return err == nil && current.StripeID == meta.StripeID
	})
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
	got, err := c.Get(t, "durable")
	if err != nil || !bytes.Equal(got.GetValue(), value) {
		t.Fatalf("Get after follower restart returned %d bytes, %v", len(got.GetValue()), err)
	}
}
