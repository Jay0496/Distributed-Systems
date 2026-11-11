package redundancy_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

func placementNodes() []string {
	return []string{"node1", "node2", "node3", "node4", "node5"}
}

func TestPlacementIsDeterministic(t *testing.T) {
	placer := redundancy.NewPlacer()
	first, err := placer.Owners("stripe-001", redundancy.ModeRS32, redundancy.TotalShards, placementNodes())
	if err != nil {
		t.Fatalf("Owners: %v", err)
	}

	for i := 0; i < 100; i++ {
		next, err := placer.Owners("stripe-001", redundancy.ModeRS32, redundancy.TotalShards, placementNodes())
		if err != nil {
			t.Fatalf("Owners iteration %d: %v", i, err)
		}
		if len(next) != len(first) {
			t.Fatalf("Owners iteration %d returned %d placements, want %d", i, len(next), len(first))
		}
		for j := range first {
			if next[j] != first[j] {
				t.Fatalf("Owners iteration %d changed placement %d: %+v != %+v", i, j, next[j], first[j])
			}
		}
	}
}

func TestPlacementIgnoresNodeInputOrder(t *testing.T) {
	placer := redundancy.NewPlacer()
	nodes := placementNodes()
	want, err := placer.Owners("stripe-order", redundancy.ModeRS32, redundancy.TotalShards, nodes)
	if err != nil {
		t.Fatalf("Owners: %v", err)
	}

	shuffled := append([]string(nil), nodes...)
	rand.New(rand.NewSource(42)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	got, err := placer.Owners("stripe-order", redundancy.ModeRS32, redundancy.TotalShards, shuffled)
	if err != nil {
		t.Fatalf("Owners(shuffled): %v", err)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("placement %d depends on node input order: %+v != %+v", i, got[i], want[i])
		}
	}
}

func TestPlacementUsesFiveDistinctNodes(t *testing.T) {
	placer := redundancy.NewPlacer()
	nodes := placementNodes()

	for i := 0; i < 1000; i++ {
		stripeID := fmt.Sprintf("stripe-%04d", i)
		placements, err := placer.Owners(stripeID, redundancy.ModeRS32, redundancy.TotalShards, nodes)
		if err != nil {
			t.Fatalf("Owners(%s): %v", stripeID, err)
		}

		seen := make(map[string]int, redundancy.TotalShards)
		for _, placement := range placements {
			seen[placement.NodeID]++
		}
		if len(seen) != redundancy.TotalShards {
			t.Fatalf("stripe %s used %d nodes, want %d: %+v", stripeID, len(seen), redundancy.TotalShards, placements)
		}
		for _, node := range nodes {
			if seen[node] != 1 {
				t.Fatalf("stripe %s placed shards on %s %d times: %+v", stripeID, node, seen[node], placements)
			}
		}
	}
}

func TestPlacementParityFlags(t *testing.T) {
	placer := redundancy.NewPlacer()
	nodes := placementNodes()

	rs, err := placer.Owners("stripe-rs", redundancy.ModeRS32, redundancy.TotalShards, nodes)
	if err != nil {
		t.Fatalf("Owners(rs32): %v", err)
	}
	for _, placement := range rs {
		wantParity := placement.ShardIndex >= redundancy.RSDataShards
		if placement.IsParity != wantParity {
			t.Errorf("RS shard %d IsParity = %v, want %v", placement.ShardIndex, placement.IsParity, wantParity)
		}
	}

	replicated, err := placer.Owners("stripe-rep", redundancy.ModeReplication, redundancy.TotalShards, nodes)
	if err != nil {
		t.Fatalf("Owners(replication): %v", err)
	}
	for _, placement := range replicated {
		if placement.IsParity {
			t.Errorf("replication copy %d is marked as parity", placement.ShardIndex)
		}
	}
}

func TestPlacementRotatesAcrossStripes(t *testing.T) {
	placer := redundancy.NewPlacer()
	nodes := placementNodes()
	counts := make(map[string]int, len(nodes))
	firstShard := make(map[string]int, len(nodes))

	const stripes = 5000
	for i := 0; i < stripes; i++ {
		placements, err := placer.Owners(fmt.Sprintf("stripe-%d", i), redundancy.ModeRS32, redundancy.TotalShards, nodes)
		if err != nil {
			t.Fatalf("Owners(%d): %v", i, err)
		}
		firstShard[placements[0].NodeID]++
		for _, placement := range placements {
			counts[placement.NodeID]++
		}
	}

	want := stripes // every stripe places one shard on every node
	for _, node := range nodes {
		if counts[node] != want {
			t.Errorf("node %s received %d shards, want %d", node, counts[node], want)
		}
	}

	wantFirst := stripes / len(nodes)
	tolerance := stripes / (len(nodes) * 5) // 20 percent of the mean
	for _, node := range nodes {
		if delta := firstShard[node] - wantFirst; delta > tolerance || delta < -tolerance {
			t.Errorf("first-shard rotation for %s = %d, want %d +/- %d", node, firstShard[node], wantFirst, tolerance)
		}
	}
}

func TestPlacementRejectsInvalidInput(t *testing.T) {
	placer := redundancy.NewPlacer()

	cases := []struct {
		name       string
		mode       redundancy.Mode
		shardCount int
		nodes      []string
		want       string
	}{
		{
			name:       "four nodes",
			mode:       redundancy.ModeRS32,
			shardCount: redundancy.TotalShards,
			nodes:      placementNodes()[:4],
			want:       "at least 5 nodes",
		},
		{
			name:       "four shards",
			mode:       redundancy.ModeRS32,
			shardCount: 4,
			nodes:      placementNodes(),
			want:       "exactly 5 shards",
		},
		{
			name:       "duplicate node",
			mode:       redundancy.ModeRS32,
			shardCount: redundancy.TotalShards,
			nodes:      []string{"node1", "node1", "node3", "node4", "node5"},
			want:       "duplicate node ID",
		},
		{
			name:       "empty node ID",
			mode:       redundancy.ModeRS32,
			shardCount: redundancy.TotalShards,
			nodes:      []string{"", "node2", "node3", "node4", "node5"},
			want:       "must not be empty",
		},
		{
			name:       "invalid mode",
			mode:       "triple",
			shardCount: redundancy.TotalShards,
			nodes:      placementNodes(),
			want:       "invalid redundancy mode",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			placements, err := placer.Owners("stripe", tc.mode, tc.shardCount, tc.nodes)
			if err == nil {
				t.Fatalf("Owners() = %+v, want error", placements)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Owners() error = %v, want %q", err, tc.want)
			}
			if placements != nil {
				t.Fatalf("Owners() returned placements on error: %+v", placements)
			}
		})
	}
}

func TestPlacementEmptyModeUsesReplicationParityFlags(t *testing.T) {
	placer := redundancy.NewPlacer()
	placements, err := placer.Owners("stripe-default", redundancy.Mode(""), redundancy.TotalShards, placementNodes())
	if err != nil {
		t.Fatalf("Owners(default): %v", err)
	}
	for _, placement := range placements {
		if placement.IsParity {
			t.Errorf("default-mode placement %d is marked as parity", placement.ShardIndex)
		}
	}
}
