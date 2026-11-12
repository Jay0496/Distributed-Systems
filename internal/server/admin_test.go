package server

import (
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"

	pb "distributed-kv-store/proto"
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
