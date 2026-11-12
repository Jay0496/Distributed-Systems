package server

import (
	"fmt"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/redundancy"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// waitForClusterConfig polls every node until its applied configuration
// matches want. Raft applies config changes asynchronously on followers.
func waitForClusterConfig(t *testing.T, cluster *testCluster, want string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range cluster.nodes {
			cfg, err := tn.store.GetConfig()
			if err != nil {
				last = tn.id + ": " + err.Error()
				ok = false
				continue
			}
			if cfg.DefaultMode != want {
				last = tn.id + ": " + cfg.DefaultMode
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
func waitForKeyAbsent(t *testing.T, cluster *testCluster, key string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range cluster.nodes {
			if _, err := tn.store.GetKey(key); err == nil {
				last = tn.id + ": metadata still present"
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

// waitForStripeFragments polls every node until the stripe has want local
// fragments.
func waitForStripeFragments(t *testing.T, cluster *testCluster, stripeID string, want int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range cluster.nodes {
			refs, err := tn.store.ListFragments(stripeID)
			if err != nil {
				last = tn.id + ": " + err.Error()
				ok = false
				continue
			}
			if len(refs) != want {
				last = fmt.Sprintf("%s: %d fragments", tn.id, len(refs))
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for stripe %q to have %d fragments per node: %s", stripeID, want, last)
}

func TestServerConfigRPCs(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	follower := c.waitFollower(t)

	t.Run("get config defaults to rs32 through a follower", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		resp, err := follower.client.GetConfig(ctx, &pb.GetConfigRequest{})
		if err != nil {
			t.Fatalf("GetConfig: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("GetConfig rejected: %s", resp.GetMessage())
		}
		if resp.GetDefaultMode() != string(redundancy.ModeRS32) {
			t.Fatalf("default_mode = %q, want rs32", resp.GetDefaultMode())
		}
		if resp.GetLeaderId() != leader.id {
			t.Fatalf("leader_id = %q, want %q", resp.GetLeaderId(), leader.id)
		}
	})

	t.Run("set config through a follower commits on the leader", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 10*time.Second)
		defer cancel()

		resp, err := follower.client.SetConfig(ctx, &pb.SetConfigRequest{DefaultMode: string(redundancy.ModeReplication)})
		if err != nil {
			t.Fatalf("SetConfig: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("SetConfig rejected: %s", resp.GetMessage())
		}
		if resp.GetDefaultMode() != string(redundancy.ModeReplication) {
			t.Fatalf("default_mode = %q, want replication", resp.GetDefaultMode())
		}
		if resp.GetLeaderId() != leader.id {
			t.Fatalf("leader_id = %q, want %q", resp.GetLeaderId(), leader.id)
		}
		waitForClusterConfig(t, c, string(redundancy.ModeReplication))

		ctx, cancel = ctxWithTimeout(t, 5*time.Second)
		defer cancel()
		got, err := leader.client.GetConfig(ctx, &pb.GetConfigRequest{})
		if err != nil {
			t.Fatalf("GetConfig after set: %v", err)
		}
		if got.GetDefaultMode() != string(redundancy.ModeReplication) {
			t.Fatalf("default_mode = %q, want replication", got.GetDefaultMode())
		}
	})

	t.Run("request mode overrides the global mode", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 10*time.Second)
		defer cancel()

		fromConfig, err := leader.client.Put(ctx, &pb.PutRequest{Key: "config-mode", Value: []byte("v")})
		if err != nil || !fromConfig.GetSuccess() {
			t.Fatalf("Put using config mode: resp=%+v err=%v", fromConfig, err)
		}
		if fromConfig.GetMode() != string(redundancy.ModeReplication) {
			t.Fatalf("Put mode = %q, want replication from cluster config", fromConfig.GetMode())
		}

		override, err := leader.client.Put(ctx, &pb.PutRequest{
			Key:   "override-mode",
			Value: []byte("v"),
			Mode:  string(redundancy.ModeRS32),
		})
		if err != nil || !override.GetSuccess() {
			t.Fatalf("Put with override: resp=%+v err=%v", override, err)
		}
		if override.GetMode() != string(redundancy.ModeRS32) {
			t.Fatalf("Put mode = %q, want rs32 override", override.GetMode())
		}
	})

	t.Run("invalid and empty modes are rejected without changing config", func(t *testing.T) {
		for _, mode := range []string{"", "triple"} {
			ctx, cancel := ctxWithTimeout(t, 5*time.Second)
			resp, err := follower.client.SetConfig(ctx, &pb.SetConfigRequest{DefaultMode: mode})
			cancel()
			if err != nil {
				t.Fatalf("SetConfig(%q) RPC failed: %v", mode, err)
			}
			if resp.GetSuccess() {
				t.Fatalf("SetConfig(%q) was accepted", mode)
			}
			if resp.GetMessage() == "" {
				t.Fatalf("SetConfig(%q) did not explain the rejection", mode)
			}
		}

		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()
		got, err := leader.client.GetConfig(ctx, &pb.GetConfigRequest{})
		if err != nil {
			t.Fatalf("GetConfig: %v", err)
		}
		if got.GetDefaultMode() != string(redundancy.ModeReplication) {
			t.Fatalf("rejected sets changed default_mode to %q", got.GetDefaultMode())
		}
	})
}

func TestServerDeleteRPCs(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	follower := c.waitFollower(t)

	ctx, cancel := ctxWithTimeout(t, 10*time.Second)
	defer cancel()
	put, err := leader.client.Put(ctx, &pb.PutRequest{
		Key:   "delete-me",
		Value: []byte("payload"),
		Mode:  string(redundancy.ModeRS32),
	})
	if err != nil || !put.GetSuccess() {
		t.Fatalf("seed Put: resp=%+v err=%v", put, err)
	}
	waitForMetaVersion(t, c, "delete-me", 1)

	t.Run("version mismatch returns Aborted and keeps the key", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		_, err := follower.client.Delete(ctx, &pb.DeleteRequest{
			Key:             "delete-me",
			ExpectedVersion: put.GetVersion() + 1,
		})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("Delete with stale version = %v, want Aborted", err)
		}
	})

	t.Run("delete removes metadata and all stripe fragments", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 10*time.Second)
		defer cancel()

		resp, err := follower.client.Delete(ctx, &pb.DeleteRequest{
			Key:             "delete-me",
			ExpectedVersion: put.GetVersion(),
		})
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if !resp.GetSuccess() || !resp.GetFound() {
			t.Fatalf("Delete response = %+v, want found success", resp)
		}
		if resp.GetOldStripeId() != put.GetStripeId() {
			t.Fatalf("old_stripe_id = %q, want %q", resp.GetOldStripeId(), put.GetStripeId())
		}

		waitForKeyAbsent(t, c, "delete-me")
		waitForStripeFragments(t, c, put.GetStripeId(), 0)

		_, err = leader.client.Get(ctx, &pb.GetRequest{Key: "delete-me"})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("Get after delete = %v, want NotFound", err)
		}
	})

	t.Run("deleting a missing key is idempotent", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		resp, err := follower.client.Delete(ctx, &pb.DeleteRequest{Key: "absent"})
		if err != nil {
			t.Fatalf("Delete missing key: %v", err)
		}
		if !resp.GetSuccess() || resp.GetFound() {
			t.Fatalf("Delete missing key response = %+v", resp)
		}
	})

	t.Run("empty key is rejected", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		_, err := leader.client.Delete(ctx, &pb.DeleteRequest{Key: ""})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Delete empty key = %v, want InvalidArgument", err)
		}
	})
}

func TestServerStatsAndHealth(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	follower := c.waitFollower(t)

	ctx, cancel := ctxWithTimeout(t, 15*time.Second)
	defer cancel()
	for _, key := range []string{"stats-a", "stats-b"} {
		resp, err := leader.client.Put(ctx, &pb.PutRequest{
			Key:   key,
			Value: []byte("stats-value"),
			Mode:  string(redundancy.ModeRS32),
		})
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("Put(%q): resp=%+v err=%v", key, resp, err)
		}
		waitForMetaVersion(t, c, key, 1)
	}

	t.Run("local stats reflect committed writes", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			ctx, cancel := ctxWithTimeout(t, 5*time.Second)
			stats, err := follower.client.Stats(ctx, &pb.StatsRequest{})
			cancel()
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if stats.GetNodeId() != follower.id {
				t.Fatalf("node_id = %q, want %q", stats.GetNodeId(), follower.id)
			}
			if stats.GetLeader() {
				t.Fatalf("follower %s reported leader=true", follower.id)
			}
			if stats.GetMode() != string(redundancy.ModeRS32) {
				t.Fatalf("mode = %q, want rs32", stats.GetMode())
			}
			if stats.GetKeys() == 2 && stats.GetStripes() == 2 {
				if stats.GetFragments() == 0 {
					t.Fatal("stats reported no fragments after two writes")
				}
				if stats.GetUptimeSeconds() < 0 {
					t.Fatalf("uptime = %d", stats.GetUptimeSeconds())
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("stats did not converge: keys=%d stripes=%d fragments=%d",
					stats.GetKeys(), stats.GetStripes(), stats.GetFragments())
			}
			time.Sleep(25 * time.Millisecond)
		}
	})

	t.Run("health lists five healthy nodes and forwards", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 15*time.Second)
		defer cancel()

		health, err := follower.client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		if err != nil {
			t.Fatalf("ClusterHealth: %v", err)
		}
		if !health.GetSuccess() {
			t.Fatalf("ClusterHealth rejected: %s", health.GetMessage())
		}
		if health.GetLeaderId() != leader.id {
			t.Fatalf("leader_id = %q, want %q", health.GetLeaderId(), leader.id)
		}
		if health.GetTotalNodes() != 5 || health.GetHealthyNodes() != 5 {
			t.Fatalf("health = %d/%d nodes, want 5/5", health.GetHealthyNodes(), health.GetTotalNodes())
		}
		if health.GetDegraded() {
			t.Fatalf("healthy cluster reported degraded: %s", health.GetMessage())
		}
		if len(health.GetNodes()) != 5 {
			t.Fatalf("health listed %d nodes, want 5", len(health.GetNodes()))
		}
	})

	t.Run("an unreachable node and a degraded write mark the cluster degraded", func(t *testing.T) {
		target := ""
		for _, tn := range c.nodes {
			if tn.id != leader.id {
				target = tn.id
				break
			}
		}
		if target == "" {
			t.Fatal("no non-leader node to make unreachable")
		}
		c.SetUnreachable(target)
		t.Cleanup(func() { c.SetReachable(target) })

		ctx, cancel := ctxWithTimeout(t, 15*time.Second)
		defer cancel()
		put, err := leader.client.Put(ctx, &pb.PutRequest{
			Key:   "degraded-health",
			Value: []byte("value"),
			Mode:  string(redundancy.ModeRS32),
		})
		if err != nil || !put.GetSuccess() {
			t.Fatalf("degraded Put: resp=%+v err=%v", put, err)
		}
		if !put.GetDegraded() {
			t.Fatalf("write with an unreachable owner was not degraded: %+v", put)
		}

		health, err := leader.client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		if err != nil {
			t.Fatalf("ClusterHealth: %v", err)
		}
		if !health.GetDegraded() {
			t.Fatalf("health did not report degraded: %+v", health)
		}
		if health.GetHealthyNodes() != 4 || health.GetTotalNodes() != 5 {
			t.Fatalf("health = %d/%d nodes, want 4/5", health.GetHealthyNodes(), health.GetTotalNodes())
		}
		if health.GetDegradedStripes() == 0 {
			t.Fatal("degraded stripe count is zero")
		}
		var found bool
		for _, node := range health.GetNodes() {
			if node.GetNodeId() == target {
				found = true
				if node.GetHealthy() {
					t.Fatalf("unreachable node %s reported healthy", target)
				}
				if node.GetError() == "" {
					t.Fatalf("unreachable node %s reported no error", target)
				}
			}
		}
		if !found {
			t.Fatalf("health response omitted %s", target)
		}
	})
}

func TestCheckAuth(t *testing.T) {
	open := &Server{cfg: config.Config{}}
	if err := open.CheckAuth(); err != nil {
		t.Fatalf("auth-disabled server CheckAuth = %v", err)
	}

	locked := &Server{cfg: config.Config{AuthToken: "token"}}
	if err := locked.CheckAuth(); err == nil {
		t.Fatal("AUTH_TOKEN without an authorizer did not refuse to start")
	}

	locked.SetAuthorizer(allowAllAuthorizer{})
	if err := locked.CheckAuth(); err != nil {
		t.Fatalf("configured authorizer rejected: %v", err)
	}
}
