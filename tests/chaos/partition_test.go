//go:build chaos

package chaos

import (
	"bytes"
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/tests/support"
)

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

	c.Partition(down...)
	defer c.Heal(down...)

	get(t, c, "chaos-partition", value)
	put(t, c, "chaos-partition-second", []byte("during-partition"), "rs32")
	get(t, c, "chaos-partition-second", []byte("during-partition"))

	waitCondition(t, 15*time.Second, "partition health degradation", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 3 && health.GetDegraded()
	})

	c.Heal(down...)
	waitCondition(t, 25*time.Second, "partition repair", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 5 && !health.GetDegraded()
	})
}

// TestOneWayPartitionDegradesWrites blocks only the leader's outbound dials to
// two owners. Raft is unaffected (it uses its own transport), but shard push and
// repair from the leader fail, so a fresh RS write is degraded until traffic is
// restored.
func TestOneWayPartitionDegradesWrites(t *testing.T) {
	c := support.StartCluster(t, 5)
	c.EnableFailureManagement(t)
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	put(t, c, "chaos-oneway", bytes.Repeat([]byte("before-"), 64), "rs32")
	meta := waitWriteMeta(t, c, "chaos-oneway")
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)
	down := twoNonLeaderIDs(t, c, meta)

	for _, id := range down {
		c.DropTraffic(leader.ID, id)
	}
	defer func() {
		for _, id := range down {
			c.RestoreTraffic(leader.ID, id)
		}
	}()

	resp := put(t, c, "chaos-oneway-second", []byte("degraded"), "rs32")
	if !resp.GetDegraded() || resp.GetShardsStored() != 3 {
		t.Fatalf("one-way partition Put = %+v, want 3 stored shards", resp)
	}
	get(t, c, "chaos-oneway-second", []byte("degraded"))

	waitCondition(t, 15*time.Second, "directed partition health degradation", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 3 && health.GetDegraded()
	})

	for _, id := range down {
		c.RestoreTraffic(leader.ID, id)
	}
	waitCondition(t, 25*time.Second, "directed partition repair", func() bool {
		health := clusterHealth(t, c, leader.ID)
		return health.GetHealthyNodes() == 5 && !health.GetDegraded()
	})
}
