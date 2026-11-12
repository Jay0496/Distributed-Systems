// Package gateway maps keys to storage-node addresses with a consistent hash
// ring and provides the CLI client that retries across those nodes. The ring
// chooses an entry node; the entry node forwards the RPC to the Raft leader,
// which owns placement and fan-out. Consistent hashing spreads client
// connections and gives the retry loop a deterministic candidate order.
package gateway

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// defaultVNodes is the virtual-node count used when the caller passes a
// non-positive value.
const defaultVNodes = 100

// Router maps keys to node addresses using a consistent hash ring.
type Router struct {
	positions []uint32
	ring      map[uint32]string
	nodes     []string
	replicas  int
}

// NewRouter builds a ring from the supplied addresses. Duplicate and empty
// addresses are ignored, and the remaining addresses are sorted so ring
// construction is deterministic for the same configuration.
func NewRouter(nodes []string, vnodes int) *Router {
	if vnodes <= 0 {
		vnodes = defaultVNodes
	}

	r := &Router{
		ring:     make(map[uint32]string),
		replicas: vnodes,
	}
	for _, node := range normalizeNodes(nodes) {
		r.AddNode(node)
	}
	return r
}

// normalizeNodes returns the sorted, de-duplicated, non-empty node list.
func normalizeNodes(nodes []string) []string {
	seen := make(map[string]struct{}, len(nodes))
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

func (r *Router) hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// AddNode adds one node's virtual nodes to the ring. Adding a node that is
// already present is a no-op.
func (r *Router) AddNode(node string) {
	if node == "" {
		return
	}
	for _, existing := range r.nodes {
		if existing == node {
			return
		}
	}
	if r.ring == nil {
		r.ring = make(map[uint32]string)
	}
	if r.replicas <= 0 {
		r.replicas = defaultVNodes
	}

	for i := 0; i < r.replicas; i++ {
		position := r.hash(node + "#" + strconv.Itoa(i))
		if _, exists := r.ring[position]; !exists {
			r.positions = append(r.positions, position)
		}
		r.ring[position] = node
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})

	r.nodes = append(r.nodes, node)
	sort.Strings(r.nodes)
}

// lookup returns the index of the first ring position at or after the key's
// hash, wrapping at the ring boundary.
func (r *Router) lookup(key string) (int, bool) {
	if len(r.positions) == 0 {
		return 0, false
	}

	position := r.hash(key)
	index := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= position
	})
	if index == len(r.positions) {
		index = 0
	}
	return index, true
}

// GetNode returns the entry node for key. The boolean is false when the ring
// has no nodes, which lets callers report a configuration error instead of
// dialing an empty address.
func (r *Router) GetNode(key string) (string, bool) {
	index, ok := r.lookup(key)
	if !ok {
		return "", false
	}
	return r.ring[r.positions[index]], true
}

// Candidates returns a de-duplicated retry order for key: the entry node
// first, then the ring successors, then every remaining configured node. The
// list always contains each configured node exactly once.
func (r *Router) Candidates(key string) []string {
	if len(r.positions) == 0 {
		return nil
	}

	index, _ := r.lookup(key)
	seen := make(map[string]struct{}, len(r.nodes))
	out := make([]string, 0, len(r.nodes))
	for i := 0; i < len(r.positions); i++ {
		node := r.ring[r.positions[(index+i)%len(r.positions)]]
		if node == "" {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	for _, node := range r.nodes {
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		out = append(out, node)
	}
	return out
}
