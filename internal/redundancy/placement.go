package redundancy

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// Placement assigns one shard or copy to one node.
type Placement struct {
	ShardIndex int
	NodeID     string
	IsParity   bool
}

// Placer maps a stripe's shard indices to node IDs.
type Placer interface {
	// Owners returns one placement per shard. The five shards are assigned to
	// five distinct nodes. The assignment is deterministic for a stripe ID and
	// sorted node set.
	Owners(stripeID string, mode Mode, shardCount int, nodes []string) ([]Placement, error)
}

// HashPlacer is the deterministic five-node placer. It sorts the node IDs,
// rotates the starting node by a hash of the stripe ID, and assigns consecutive
// shard indices to consecutive nodes in that rotation.
type HashPlacer struct{}

var _ Placer = HashPlacer{}

// NewPlacer returns the deterministic hash placer.
func NewPlacer() Placer {
	return HashPlacer{}
}

// Owners implements Placer. It requires exactly TotalShards shards and at least
// that many distinct, non-empty node IDs. Extra nodes participate in the
// rotation but do not receive a shard for this stripe.
func (HashPlacer) Owners(stripeID string, mode Mode, shardCount int, nodes []string) ([]Placement, error) {
	resolved, err := resolveMode(mode)
	if err != nil {
		return nil, err
	}
	if shardCount != TotalShards {
		return nil, fmt.Errorf("placement requires exactly %d shards, got %d", TotalShards, shardCount)
	}
	if len(nodes) < TotalShards {
		return nil, fmt.Errorf("placement requires at least %d nodes, got %d", TotalShards, len(nodes))
	}

	sorted := append([]string(nil), nodes...)
	sort.Strings(sorted)

	seen := make(map[string]struct{}, len(sorted))
	for _, node := range sorted {
		if node == "" {
			return nil, fmt.Errorf("node ID must not be empty")
		}
		if _, duplicate := seen[node]; duplicate {
			return nil, fmt.Errorf("duplicate node ID %q", node)
		}
		seen[node] = struct{}{}
	}

	offset := int(hashStripeID(stripeID) % uint64(len(sorted)))
	placements := make([]Placement, shardCount)
	for i := 0; i < shardCount; i++ {
		placements[i] = Placement{
			ShardIndex: i,
			NodeID:     sorted[(offset+i)%len(sorted)],
			IsParity:   resolved == ModeRS32 && i >= RSDataShards,
		}
	}
	return placements, nil
}

func hashStripeID(stripeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(stripeID))
	return h.Sum64()
}
