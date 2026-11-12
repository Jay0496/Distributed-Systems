package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// waitForConfig polls every node until its applied cluster configuration
// matches want.
func waitForConfig(t *testing.T, c *support.Cluster, want string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range c.Nodes {
			if n.Store == nil {
				continue
			}
			cfg, err := n.Store.GetConfig()
			if err != nil {
				last = n.ID + ": " + err.Error()
				ok = false
				continue
			}
			if cfg.DefaultMode != want {
				last = n.ID + ": " + cfg.DefaultMode
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for cluster config %q: %s", want, last)
}

// waitForKeyAbsent polls every node until metadata for key is gone.
func waitForKeyAbsent(t *testing.T, c *support.Cluster, key string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range c.Nodes {
			if n.Store == nil {
				continue
			}
			if _, err := n.Store.GetKey(key); err == nil {
				last = n.ID + ": metadata still present"
				ok = false
			} else if !errors.Is(err, storage.ErrNotFound) {
				last = n.ID + ": " + err.Error()
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for key %q to disappear: %s", key, last)
}

// TestConfigPersistsAcrossRestart stops a follower, changes the global mode,
// restarts the follower from its persisted SQLite and Raft files, and verifies
// that the committed config replays onto it.
func TestConfigPersistsAcrossRestart(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	var follower *support.Node
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}
	c.StopNode(t, follower.ID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := leader.Client.SetConfig(ctx, &pb.SetConfigRequest{DefaultMode: "replication"})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("SetConfig: resp=%+v err=%v", resp, err)
	}

	c.StartNode(t, follower.ID)
	waitForConfig(t, c, "replication")

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := follower.Client.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		t.Fatalf("GetConfig after restart: %v", err)
	}
	if got.GetDefaultMode() != "replication" {
		t.Fatalf("default_mode after restart = %q, want replication", got.GetDefaultMode())
	}
}

func TestDeleteRemovesAllShards(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	put := c.Put(t, &pb.PutRequest{Key: "delete-all", Value: []byte("delete payload"), Mode: "rs32"})
	if !put.GetSuccess() {
		t.Fatalf("Put rejected: %s", put.GetMessage())
	}
	meta := waitWriteMeta(t, c, "delete-all", 1)
	waitFragmentTotal(t, c, meta.StripeID, 5)

	var follower *support.Node
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := follower.Client.Delete(ctx, &pb.DeleteRequest{
		Key:             "delete-all",
		ExpectedVersion: put.GetVersion() + 1,
	}); status.Code(err) != codes.Aborted {
		t.Fatalf("stale-version Delete = %v, want Aborted", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := follower.Client.Delete(ctx, &pb.DeleteRequest{
		Key:             "delete-all",
		ExpectedVersion: put.GetVersion(),
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !resp.GetSuccess() || !resp.GetFound() || resp.GetOldStripeId() != meta.StripeID {
		t.Fatalf("Delete response = %+v", resp)
	}
	waitForKeyAbsent(t, c, "delete-all")
	waitFragmentTotal(t, c, meta.StripeID, 0)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	missing, err := follower.Client.Delete(ctx, &pb.DeleteRequest{Key: "delete-all"})
	if err != nil {
		t.Fatalf("idempotent Delete: %v", err)
	}
	if !missing.GetSuccess() || missing.GetFound() {
		t.Fatalf("idempotent Delete response = %+v", missing)
	}
}

func TestStatsReflectWrites(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	for _, key := range []string{"stats-one", "stats-two"} {
		resp := c.Put(t, &pb.PutRequest{Key: key, Value: []byte("stats"), Mode: "replication"})
		if !resp.GetSuccess() {
			t.Fatalf("Put(%q) rejected: %s", key, resp.GetMessage())
		}
		waitWriteMeta(t, c, key, 1)
	}

	deadline := time.Now().Add(30 * time.Second)
	var last *pb.StatsResponse
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stats, err := leader.Client.Stats(ctx, &pb.StatsRequest{})
		cancel()
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		last = stats
		if stats.GetKeys() == 2 && stats.GetStripes() == 2 && stats.GetFragments() == 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if last == nil {
		t.Fatal("Stats returned nothing")
	}
	if last.GetKeys() != 2 || last.GetStripes() != 2 || last.GetFragments() != 2 {
		t.Fatalf("stats = keys=%d stripes=%d fragments=%d, want 2/2/2",
			last.GetKeys(), last.GetStripes(), last.GetFragments())
	}
	if last.GetStorageBytes() == 0 {
		t.Fatal("storage_bytes is zero for a non-empty SQLite database")
	}
	if !last.GetLeader() {
		t.Fatalf("leader %s reported leader=false", leader.ID)
	}
	if last.GetMode() != "rs32" {
		t.Fatalf("mode = %q, want rs32", last.GetMode())
	}
	if last.GetUptimeSeconds() < 0 {
		t.Fatalf("uptime_seconds = %d", last.GetUptimeSeconds())
	}
	if last.GetRepairQueueDepth() != 0 {
		t.Fatalf("repair_queue_depth = %d before task 09", last.GetRepairQueueDepth())
	}
}

func TestHealthReportsLeaderAndDegraded(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	var follower *support.Node
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	healthy, err := follower.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
	if err != nil {
		t.Fatalf("ClusterHealth: %v", err)
	}
	if !healthy.GetSuccess() || healthy.GetLeaderId() != leader.ID {
		t.Fatalf("healthy cluster response = %+v", healthy)
	}
	if healthy.GetTotalNodes() != 5 || healthy.GetHealthyNodes() != 5 {
		t.Fatalf("health = %d/%d, want 5/5", healthy.GetHealthyNodes(), healthy.GetTotalNodes())
	}
	if healthy.GetDegraded() || healthy.GetDegradedStripes() != 0 {
		t.Fatalf("healthy cluster reported degraded: %+v", healthy)
	}
	if healthy.GetMode() != "rs32" {
		t.Fatalf("mode = %q, want rs32", healthy.GetMode())
	}

	c.SetUnreachable(follower.ID)
	defer c.SetReachable(follower.ID)

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	degradedPut, err := leader.Client.Put(ctx, &pb.PutRequest{
		Key:   "degraded-health",
		Value: []byte("value"),
		Mode:  "rs32",
	})
	if err != nil || !degradedPut.GetSuccess() {
		t.Fatalf("degraded Put: resp=%+v err=%v", degradedPut, err)
	}
	if !degradedPut.GetDegraded() {
		t.Fatalf("write with one owner unreachable was not degraded: %+v", degradedPut)
	}

	health, err := leader.Client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
	if err != nil {
		t.Fatalf("ClusterHealth after failure: %v", err)
	}
	if !health.GetDegraded() {
		t.Fatalf("cluster health did not report degraded: %+v", health)
	}
	if health.GetHealthyNodes() != 4 || health.GetTotalNodes() != 5 {
		t.Fatalf("health = %d/%d nodes, want 4/5", health.GetHealthyNodes(), health.GetTotalNodes())
	}
	if health.GetDegradedStripes() == 0 {
		t.Fatal("degraded_stripes is zero after a degraded write")
	}
	var found bool
	for _, node := range health.GetNodes() {
		if node.GetNodeId() == follower.ID {
			found = true
			if node.GetHealthy() {
				t.Fatalf("unreachable node %s reported healthy", follower.ID)
			}
			if node.GetError() == "" {
				t.Fatalf("unreachable node %s reported no error", follower.ID)
			}
		}
	}
	if !found {
		t.Fatalf("health omitted %s", follower.ID)
	}
}
