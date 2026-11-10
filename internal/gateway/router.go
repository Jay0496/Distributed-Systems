// Package gateway maps keys to storage-node addresses with a consistent hash
// ring. The CLI in cmd/gateway owns argument parsing and gRPC calls; this
// package owns only placement.
package gateway

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

// Router maps keys to node addresses using a consistent hash ring.
type Router struct {
	nodes    []uint32
	ring     map[uint32]string
	replicas int
}

func NewRouter(vnodes int) *Router {
	if vnodes <= 0 {
		vnodes = 100
	}

	r := &Router{
		ring:     make(map[uint32]string),
		replicas: vnodes,
	}
	for port := 50051; port <= 50055; port++ {
		r.AddNode(fmt.Sprintf("localhost:%d", port))
	}
	return r
}

func (r *Router) hash(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (r *Router) AddNode(node string) {
	for i := 0; i < r.replicas; i++ {
		position := r.hash(node + "#" + strconv.Itoa(i))
		if _, exists := r.ring[position]; !exists {
			r.nodes = append(r.nodes, position)
		}
		r.ring[position] = node
	}
	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i] < r.nodes[j]
	})
}

func (r *Router) GetNode(key string) string {
	if len(r.nodes) == 0 {
		return ""
	}

	position := r.hash(key)
	index := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= position
	})
	if index == len(r.nodes) {
		index = 0
	}
	return r.ring[r.nodes[index]]
}
