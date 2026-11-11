package integration

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func consensusNodes(c *support.Cluster) []*consensus.Node {
	nodes := make([]*consensus.Node, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		nodes = append(nodes, node.Raft)
	}
	return nodes
}

func nodeByID(t *testing.T, c *support.Cluster, id string) *support.Node {
	t.Helper()
	for _, node := range c.Nodes {
		if node.ID == id {
			return node
		}
	}
	t.Fatalf("unknown node %q", id)
	return nil
}

// TestReadRS32WithTwoNodesDown writes a healthy RS stripe, makes two of its
// non-leader owners unreachable, and reads the value from the remaining three.
func TestReadRS32WithTwoNodesDown(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	value := bytes.Repeat([]byte("two-nodes-down-"), 257)

	resp := c.Put(t, &pb.PutRequest{Key: "two-down", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "two-down", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	var down []string
	for _, placement := range meta.Placement {
		if placement.NodeID == leader.ID {
			continue
		}
		down = append(down, placement.NodeID)
		if len(down) == 2 {
			break
		}
	}
	c.SetUnreachable(down...)
	defer c.SetReachable(down...)

	got, err := c.Get(t, "two-down")
	if err != nil {
		t.Fatalf("read with two nodes down: %v", err)
	}
	if !got.GetSuccess() || !bytes.Equal(got.GetValue(), value) {
		t.Fatalf("read with two nodes down returned success=%v value=%d bytes", got.GetSuccess(), len(got.GetValue()))
	}
	if got.GetMode() != "rs32" || got.GetStripeId() != meta.StripeID || got.GetVersion() != 1 {
		t.Fatalf("read response = %+v", got)
	}
}

// TestReadRepairsMissingShard corrupts a stored shard, then verifies that a read
// still returns the committed value and asynchronously restores the intended
// fragment on its owner.
func TestReadRepairsMissingShard(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	value := bytes.Repeat([]byte("read-repair-"), 191)

	resp := c.Put(t, &pb.PutRequest{Key: "repair", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "repair", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	// Corrupt a non-leader owner's fragment through the store API. The new
	// bytes carry a matching checksum, so only the value checksum can detect
	// the corruption and identify the bad shard.
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
	owner := nodeByID(t, c, target.NodeID)
	frag, err := owner.Store.GetFragment(meta.StripeID, target.ShardIndex)
	if err != nil {
		t.Fatalf("get fragment: %v", err)
	}
	corruptData := append([]byte(nil), frag.Data...)
	corruptData[0] ^= 0xff
	if err := owner.Store.PutFragment(storage.Fragment{
		StripeID:   meta.StripeID,
		ShardIndex: target.ShardIndex,
		IsParity:   frag.IsParity,
		Data:       corruptData,
		Checksum:   redundancy.Checksum(corruptData),
	}); err != nil {
		t.Fatalf("corrupt fragment: %v", err)
	}

	// Mark the stripe degraded so the read inspects every placement and can
	// find the internally consistent corrupt shard.
	statusCmd, err := json.Marshal(consensus.Command{
		Op:       consensus.OpSetMetaStatus,
		Key:      "repair",
		StripeID: meta.StripeID,
		Version:  meta.Version,
		Degraded: true,
	})
	if err != nil {
		t.Fatalf("marshal set_meta_status: %v", err)
	}
	if _, err := support.ApplyRawToLeader(consensusNodes(c), statusCmd, 10*time.Second); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}

	got, err := c.Get(t, "repair")
	if err != nil {
		t.Fatalf("read with corrupt shard: %v", err)
	}
	if !bytes.Equal(got.GetValue(), value) {
		t.Fatal("read returned the wrong value")
	}
	if !got.GetRepaired() {
		t.Fatal("read did not schedule repair for the corrupt shard")
	}

	engine := redundancy.NewRS32Engine()
	shards, err := engine.Encode(value, redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("encode expected shards: %v", err)
	}
	wantChecksum := shards[target.ShardIndex].Checksum

	deadline := time.Now().Add(20 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		refs, err := owner.Store.ListFragments(meta.StripeID)
		if err != nil {
			t.Fatalf("list fragments: %v", err)
		}
		for _, ref := range refs {
			if ref.ShardIndex == target.ShardIndex {
				last = ref.Checksum
				if ref.Checksum == wantChecksum {
					if totalFragments(c, meta.StripeID) != redundancy.TotalShards {
						t.Fatalf("repair changed the placement count")
					}
					return
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("shard %d on %s was not repaired: checksum %s, want %s", target.ShardIndex, target.NodeID, last, wantChecksum)
}

// TestReadReplicationFallback leaves a single surviving remote copy and reads
// the value through it.
func TestReadReplicationFallback(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	value := bytes.Repeat([]byte("one-copy-"), 233)

	resp := c.Put(t, &pb.PutRequest{Key: "one-copy", Value: value, Mode: "replication"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "one-copy", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	var keep storage.ShardPlacement
	for _, placement := range meta.Placement {
		if placement.NodeID != leader.ID {
			keep = placement
			break
		}
	}
	if keep.NodeID == "" {
		t.Fatal("no remote copy found")
	}

	var down []string
	for _, placement := range meta.Placement {
		if placement.NodeID == keep.NodeID {
			continue
		}
		if placement.NodeID == leader.ID {
			// The local fast path ignores unreachable dials, so remove the
			// leader's own copy to force the remote fallback.
			if err := leader.Store.DeleteFragment(meta.StripeID, placement.ShardIndex); err != nil {
				t.Fatalf("delete leader copy: %v", err)
			}
			continue
		}
		down = append(down, placement.NodeID)
	}
	c.SetUnreachable(down...)
	defer c.SetReachable(down...)

	got, err := c.Get(t, "one-copy")
	if err != nil {
		t.Fatalf("read with one surviving copy: %v", err)
	}
	if !bytes.Equal(got.GetValue(), value) {
		t.Fatal("read from a surviving replication copy returned the wrong value")
	}
	if got.GetMode() != "replication" || got.GetShardsUsed() != 1 {
		t.Fatalf("read response = %+v", got)
	}
}

// TestReadFailsWithThreeShardsDown makes three RS owners unreachable. The two
// remaining shards are insufficient, so the read must fail explicitly.
func TestReadFailsWithThreeShardsDown(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	value := bytes.Repeat([]byte("three-down-"), 151)

	resp := c.Put(t, &pb.PutRequest{Key: "three-down", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "three-down", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	var down []string
	for _, placement := range meta.Placement {
		if placement.NodeID == leader.ID {
			continue
		}
		down = append(down, placement.NodeID)
		if len(down) == 3 {
			break
		}
	}
	c.SetUnreachable(down...)
	defer c.SetReachable(down...)

	_, err = c.Get(t, "three-down")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("read with three shards down = %v, want FailedPrecondition", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("failed owners")) {
		t.Fatalf("read error does not list failed owners: %v", err)
	}
}

func TestReadNotFound(t *testing.T) {
	c := support.StartCluster(t, 5)
	follower := c.NonLeaderIDs(t)[0]

	_, err := c.GetVia(t, follower, "missing-key")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing-key read = %v, want NotFound through follower forwarding", err)
	}
}
