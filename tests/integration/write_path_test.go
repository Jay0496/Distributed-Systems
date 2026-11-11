package integration

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

const writeTimeoutForTest = 20 * time.Second

func waitWriteMeta(t *testing.T, c *support.Cluster, key string, version int64) storage.KeyMeta {
	t.Helper()

	deadline := time.Now().Add(writeTimeoutForTest)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		var meta storage.KeyMeta
		for _, n := range c.Nodes {
			current, err := n.Store.GetKey(key)
			if err != nil {
				last = n.ID + ": " + err.Error()
				ok = false
				continue
			}
			if current.Version != version {
				last = n.ID + ": wrong version"
				ok = false
				continue
			}
			meta = current
		}
		if ok {
			return meta
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q version %d: %s", key, version, last)
	return storage.KeyMeta{}
}

func totalFragments(c *support.Cluster, stripeID string) int {
	total := 0
	for _, n := range c.Nodes {
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

	deadline := time.Now().Add(writeTimeoutForTest)
	var last int
	for time.Now().Before(deadline) {
		last = totalFragments(c, stripeID)
		if last == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("stripe %q has %d fragments, want %d", stripeID, last, want)
}

func assertMetadataHasNoPayload(t *testing.T, meta storage.KeyMeta, payload []byte) {
	t.Helper()

	data, err := json.Marshal(consensus.Command{Op: consensus.OpPutMeta, Meta: &meta})
	if err != nil {
		t.Fatalf("marshal metadata command: %v", err)
	}
	if len(data) > 64*1024 {
		t.Fatalf("metadata command is %d bytes", len(data))
	}
	if bytes.Contains(data, payload) {
		t.Fatal("metadata command contains the payload")
	}
	if !bytes.Contains(data, []byte(meta.Checksum)) {
		t.Fatal("metadata command is missing the checksum")
	}
}

func TestWriteRS32PlacesFiveShards(t *testing.T) {
	c := support.StartCluster(t, 5)
	value := bytes.Repeat([]byte("r"), 3001)

	resp := c.Put(t, &pb.PutRequest{Key: "rs-key", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	if resp.GetMode() != "rs32" || resp.GetVersion() != 1 || resp.GetDegraded() {
		t.Fatalf("PutResponse = %+v", resp)
	}
	if resp.GetShardsStored() != redundancy.TotalShards {
		t.Fatalf("stored %d shards, want %d", resp.GetShardsStored(), redundancy.TotalShards)
	}

	meta := waitWriteMeta(t, c, "rs-key", 1)
	if meta.Mode != "rs32" || meta.Size != int64(len(value)) || meta.Checksum != redundancy.Checksum(value) {
		t.Fatalf("metadata = %+v", meta)
	}
	if len(meta.Placement) != redundancy.TotalShards {
		t.Fatalf("metadata has %d placements", len(meta.Placement))
	}
	assertMetadataHasNoPayload(t, meta, value)

	owners := make(map[string]int, len(meta.Placement))
	shardSize := (len(value) + redundancy.RSDataShards - 1) / redundancy.RSDataShards
	for _, placement := range meta.Placement {
		owners[placement.NodeID] = placement.ShardIndex
	}
	if len(owners) != redundancy.TotalShards {
		t.Fatalf("placements use %d distinct nodes", len(owners))
	}

	for _, n := range c.Nodes {
		refs, err := n.Store.ListFragments(meta.StripeID)
		if err != nil {
			t.Fatalf("%s list: %v", n.ID, err)
		}
		wantShard, ok := owners[n.ID]
		if !ok {
			t.Fatalf("%s is not an owner", n.ID)
		}
		if len(refs) != 1 || refs[0].ShardIndex != wantShard {
			t.Fatalf("%s fragments = %+v, want shard %d", n.ID, refs, wantShard)
		}
		if refs[0].Size != int64(shardSize) {
			t.Fatalf("%s shard %d is %d bytes, want %d", n.ID, wantShard, refs[0].Size, shardSize)
		}
		wantParity := wantShard >= redundancy.RSDataShards
		if refs[0].IsParity != wantParity {
			t.Fatalf("%s shard %d parity=%v, want %v", n.ID, wantShard, refs[0].IsParity, wantParity)
		}
	}
}

func TestWriteEmptyValue(t *testing.T) {
	c := support.StartCluster(t, 5)

	resp := c.Put(t, &pb.PutRequest{Key: "empty", Value: nil, Mode: "replication"})
	if !resp.GetSuccess() {
		t.Fatalf("empty Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "empty", 1)
	if meta.Size != 0 || meta.Checksum != redundancy.Checksum(nil) {
		t.Fatalf("empty metadata = %+v", meta)
	}
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
}

func TestWriteReplicationPlacesFiveCopies(t *testing.T) {
	c := support.StartCluster(t, 5)
	value := bytes.Repeat([]byte("replication-copy-"), 512)

	resp := c.Put(t, &pb.PutRequest{Key: "replicated", Value: value, Mode: "replication"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	if resp.GetMode() != "replication" || resp.GetDegraded() || resp.GetShardsStored() != redundancy.TotalShards {
		t.Fatalf("PutResponse = %+v", resp)
	}

	meta := waitWriteMeta(t, c, "replicated", 1)
	if meta.Mode != "replication" {
		t.Fatalf("mode = %q", meta.Mode)
	}
	if totalFragments(c, meta.StripeID) != redundancy.TotalShards {
		t.Fatalf("replication stripe has %d fragments", totalFragments(c, meta.StripeID))
	}

	for _, n := range c.Nodes {
		refs, err := n.Store.ListFragments(meta.StripeID)
		if err != nil {
			t.Fatalf("%s list: %v", n.ID, err)
		}
		if len(refs) != 1 {
			t.Fatalf("%s has %d fragments, want 1", n.ID, len(refs))
		}
		frag, err := n.Store.GetFragment(meta.StripeID, refs[0].ShardIndex)
		if err != nil {
			t.Fatalf("%s get: %v", n.ID, err)
		}
		if !bytes.Equal(frag.Data, value) {
			t.Fatalf("%s copy differs from the value", n.ID)
		}
		if refs[0].IsParity {
			t.Fatalf("%s replication copy is marked parity", n.ID)
		}
	}
}

func TestWriteDegradedWithTwoFailures(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	down := make([]string, 0, 2)
	for _, id := range c.NonLeaderIDs(t) {
		if len(down) == 2 {
			break
		}
		down = append(down, id)
	}
	c.SetUnreachable(down...)

	resp := c.Put(t, &pb.PutRequest{Key: "degraded-rs", Value: []byte("still readable"), Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("degraded Put rejected: %s", resp.GetMessage())
	}
	if !resp.GetDegraded() || resp.GetShardsStored() != 3 {
		t.Fatalf("PutResponse = %+v, want degraded with 3 shards", resp)
	}

	meta := waitWriteMeta(t, c, "degraded-rs", 1)
	if !meta.Degraded {
		t.Fatal("metadata is not marked degraded")
	}
	if len(meta.Placement) != redundancy.TotalShards {
		t.Fatalf("degraded metadata lost its intended placement: %+v", meta.Placement)
	}
	waitFragmentTotal(t, c, meta.StripeID, 3)

	// The leader itself must own one of the stored shards.
	stored := 0
	for _, n := range c.Nodes {
		refs, _ := n.Store.ListFragments(meta.StripeID)
		stored += len(refs)
	}
	if stored != 3 {
		t.Fatalf("stored %d shards, want 3", stored)
	}
	if leader == nil {
		t.Fatal("no leader")
	}
}

func TestWriteFailsBelowQuorum(t *testing.T) {
	c := support.StartCluster(t, 5)
	down := c.NonLeaderIDs(t)[:3]
	c.SetUnreachable(down...)

	resp := c.Put(t, &pb.PutRequest{Key: "no-quorum", Value: []byte("never visible"), Mode: "rs32"})
	if resp.GetSuccess() {
		t.Fatalf("below-quorum write succeeded: %+v", resp)
	}

	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	if _, err := leader.Store.GetKey("no-quorum"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed write left metadata: %v", err)
	}
	waitFragmentTotal(t, c, "", 0)
}

func TestOverwriteGarbageCollects(t *testing.T) {
	c := support.StartCluster(t, 5)

	first := c.Put(t, &pb.PutRequest{Key: "overwrite", Value: []byte("first value"), Mode: "rs32"})
	if !first.GetSuccess() {
		t.Fatalf("first Put rejected: %s", first.GetMessage())
	}
	firstMeta := waitWriteMeta(t, c, "overwrite", 1)
	waitFragmentTotal(t, c, firstMeta.StripeID, redundancy.TotalShards)

	second := c.Put(t, &pb.PutRequest{Key: "overwrite", Value: []byte("second value"), Mode: "replication"})
	if !second.GetSuccess() {
		t.Fatalf("second Put rejected: %s", second.GetMessage())
	}
	if second.GetVersion() != 2 {
		t.Fatalf("version = %d, want 2", second.GetVersion())
	}
	secondMeta := waitWriteMeta(t, c, "overwrite", 2)
	if secondMeta.StripeID == firstMeta.StripeID {
		t.Fatal("overwrite reused the stripe")
	}
	if secondMeta.Mode != "replication" {
		t.Fatalf("second mode = %q", secondMeta.Mode)
	}

	waitFragmentTotal(t, c, firstMeta.StripeID, 0)
	waitFragmentTotal(t, c, secondMeta.StripeID, redundancy.TotalShards)
}
