package gateway

import (
	"fmt"
	"testing"
)

func TestRouterRoutesDeterministically(t *testing.T) {
	router := NewRouter(100)

	const key = "user_session_123"
	first := router.GetNode(key)
	if first == "" {
		t.Fatal("expected a node address")
	}
	for i := 0; i < 10; i++ {
		if got := router.GetNode(key); got != first {
			t.Fatalf("routing was not deterministic: first %q, got %q", first, got)
		}
	}
}

func TestRouterDistributesKeysAcrossNodes(t *testing.T) {
	router := NewRouter(100)
	counts := make(map[string]int)
	for i := 0; i < 5000; i++ {
		counts[router.GetNode(fmt.Sprintf("key-%d", i))]++
	}

	if len(counts) != 5 {
		t.Fatalf("expected all 5 nodes to receive keys, got %d: %v", len(counts), counts)
	}
	for node, count := range counts {
		if count == 0 {
			t.Errorf("node %s received no keys", node)
		}
	}
}

func TestRouterWrapsAtRingBoundary(t *testing.T) {
	router := NewRouter(1)
	if len(router.nodes) == 0 {
		t.Fatal("expected virtual nodes")
	}

	first := router.nodes[0]
	last := router.nodes[len(router.nodes)-1]
	if got := router.ring[first]; got == "" {
		t.Fatal("expected the first ring position to have a node")
	}
	if last < first {
		t.Fatalf("ring positions are not sorted: first=%d last=%d", first, last)
	}
}

func TestRouterEmptyRing(t *testing.T) {
	router := &Router{ring: make(map[uint32]string)}
	if got := router.GetNode("key"); got != "" {
		t.Fatalf("expected empty ring to return empty address, got %q", got)
	}
}
