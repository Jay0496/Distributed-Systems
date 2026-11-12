package gateway_test

import (
	"fmt"
	"testing"

	"distributed-kv-store/internal/gateway"
)

func TestRouterRoutesDeterministically(t *testing.T) {
	router := gateway.NewRouter([]string{"n1", "n2", "n3", "n4", "n5"}, 100)

	const key = "user_session_123"
	first, ok := router.GetNode(key)
	if !ok || first == "" {
		t.Fatal("expected a node address")
	}
	for i := 0; i < 10; i++ {
		if got, _ := router.GetNode(key); got != first {
			t.Fatalf("routing was not deterministic: first %q, got %q", first, got)
		}
	}
}

func TestRouterDistributesKeysAcrossEveryNode(t *testing.T) {
	router := gateway.NewRouter([]string{"n1", "n2", "n3", "n4", "n5"}, 100)
	counts := make(map[string]int)
	for i := 0; i < 5000; i++ {
		node, ok := router.GetNode(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("GetNode reported an empty ring")
		}
		counts[node]++
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

func TestRouterEmptyRing(t *testing.T) {
	router := gateway.NewRouter(nil, 100)
	if node, ok := router.GetNode("key"); ok || node != "" {
		t.Fatalf("expected empty ring to report no node, got %q/%v", node, ok)
	}
	if candidates := router.Candidates("key"); candidates != nil {
		t.Fatalf("expected no candidates for an empty ring, got %v", candidates)
	}
}

func TestRouterCandidatesStartWithEntryAndCoverEveryNode(t *testing.T) {
	nodes := []string{"n5", "n1", "n3", "n2", "n4"}
	router := gateway.NewRouter(nodes, 100)

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("candidate-%d", i)
		entry, ok := router.GetNode(key)
		if !ok {
			t.Fatal("GetNode reported an empty ring")
		}
		candidates := router.Candidates(key)
		if len(candidates) != len(nodes) {
			t.Fatalf("Candidates(%q) returned %d nodes, want %d: %v", key, len(candidates), len(nodes), candidates)
		}
		if candidates[0] != entry {
			t.Fatalf("Candidates(%q)[0] = %q, want entry %q", key, candidates[0], entry)
		}

		seen := make(map[string]bool, len(candidates))
		for _, node := range candidates {
			if seen[node] {
				t.Fatalf("Candidates(%q) contains duplicate node %q: %v", key, node, candidates)
			}
			seen[node] = true
		}
		for _, node := range nodes {
			if !seen[node] {
				t.Fatalf("Candidates(%q) omitted node %q: %v", key, node, candidates)
			}
		}
	}
}

func TestRouterIgnoresDuplicateAndEmptyNodes(t *testing.T) {
	router := gateway.NewRouter([]string{"n2", "", "n1", "n2", "n1"}, 50)
	if got := router.Candidates("key"); len(got) != 2 {
		t.Fatalf("Candidates = %v, want two unique nodes", got)
	}

	router.AddNode("n1")
	if got := router.Candidates("key"); len(got) != 2 {
		t.Fatalf("AddNode duplicated a node: %v", got)
	}
	router.AddNode("n3")
	if got := router.Candidates("key"); len(got) != 3 {
		t.Fatalf("AddNode did not add n3: %v", got)
	}
}

func TestRouterAddNodeKeepsRoutingStable(t *testing.T) {
	router := gateway.NewRouter([]string{"n1"}, 20)
	router.AddNode("n2")
	after, ok := router.GetNode("a-key")
	if !ok || after == "" {
		t.Fatal("ring is empty after AddNode")
	}
	for i := 0; i < 5; i++ {
		if got, _ := router.GetNode("a-key"); got != after {
			t.Fatalf("routing changed after AddNode: %q then %q", after, got)
		}
	}
}
