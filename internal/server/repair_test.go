package server

import (
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"
)

func TestRepairRPIViaLeaderAndFollower(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	follower := c.waitFollower(t)

	mustPut(t, leader.client, "repair-leader", "repair-value", "rs32")
	waitForMetaVersion(t, c, "repair-leader", 1)

	for _, tc := range []struct {
		name   string
		client pb.StorageServiceClient
	}{
		{name: "leader", client: leader.client},
		{name: "follower", client: follower.client},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := ctxWithTimeout(t, 10*time.Second)
			defer cancel()
			resp, err := tc.client.Repair(ctx, &pb.RepairRequest{Key: "repair-leader"})
			if err != nil {
				t.Fatalf("Repair: %v", err)
			}
			if !resp.GetSuccess() {
				t.Fatalf("Repair rejected: %s", resp.GetMessage())
			}
			if resp.GetMessage() == "" {
				t.Fatal("Repair accepted without an explanatory message")
			}
		})
	}
}

func TestRepairRPCValidation(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	mustPut(t, leader.client, "repair-validation", "value", "rs32")
	waitForMetaVersion(t, c, "repair-validation", 1)

	cases := []struct {
		name string
		req  *pb.RepairRequest
	}{
		{name: "blank key", req: &pb.RepairRequest{Key: "  "}},
		{name: "unknown key", req: &pb.RepairRequest{Key: "does-not-exist"}},
		{name: "wrong stripe", req: &pb.RepairRequest{Key: "repair-validation", StripeId: "not-the-stripe"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := ctxWithTimeout(t, 10*time.Second)
			defer cancel()
			resp, err := leader.client.Repair(ctx, tc.req)
			if err != nil {
				t.Fatalf("Repair: %v", err)
			}
			if resp.GetSuccess() {
				t.Fatalf("Repair accepted %s: %s", tc.name, resp.GetMessage())
			}
			if resp.GetMessage() == "" {
				t.Fatalf("Repair rejection for %s did not explain itself", tc.name)
			}
		})
	}
}

func TestRepairRPCForceReplacesCorruptShard(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	mustPut(t, leader.client, "repair-force", "force-repair-value", "rs32")
	waitForMetaVersion(t, c, "repair-force", 1)

	ctx, cancel := ctxWithTimeout(t, 10*time.Second)
	meta, err := leader.store.GetKey("repair-force")
	cancel()
	if err != nil {
		t.Fatalf("get metadata: %v", err)
	}

	var target storage.ShardPlacement
	for _, placement := range meta.Placement {
		if placement.NodeID != leader.id {
			target = placement
			break
		}
	}
	if target.NodeID == "" {
		t.Fatal("no non-leader placement found")
	}
	owner := c.byID[target.NodeID]
	frag, err := owner.store.GetFragment(meta.StripeID, target.ShardIndex)
	if err != nil {
		t.Fatalf("get fragment: %v", err)
	}
	corrupt := append([]byte(nil), frag.Data...)
	corrupt[0] ^= 0xff
	if err := owner.store.PutFragment(storage.Fragment{
		StripeID:   meta.StripeID,
		ShardIndex: target.ShardIndex,
		IsParity:   frag.IsParity,
		Data:       corrupt,
		Checksum:   redundancy.Checksum(corrupt),
	}); err != nil {
		t.Fatalf("corrupt fragment: %v", err)
	}

	ctx, cancel = ctxWithTimeout(t, 10*time.Second)
	resp, err := leader.client.Repair(ctx, &pb.RepairRequest{
		Key:      "repair-force",
		StripeId: meta.StripeID,
		Force:    true,
	})
	cancel()
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Repair rejected: %s", resp.GetMessage())
	}

	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		refs, err := owner.store.ListFragments(meta.StripeID)
		if err == nil {
			for _, ref := range refs {
				if ref.ShardIndex == target.ShardIndex {
					last = ref.Checksum
					if ref.Checksum == redundancy.Checksum(frag.Data) {
						return
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("shard %d on %s was not repaired: checksum %s", target.ShardIndex, target.NodeID, last)
}
