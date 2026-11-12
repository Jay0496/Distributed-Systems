package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/gateway"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

// gatewayTestConfig maps every in-process node ID to itself so the support
// cluster's dialer can resolve it.
func gatewayTestConfig(c *support.Cluster) config.GatewayConfig {
	nodes := make(map[string]string, len(c.Peers))
	order := make([]string, 0, len(c.Peers))
	for id := range c.Peers {
		nodes[id] = id
		order = append(order, id)
	}
	sort.Strings(order)
	return config.GatewayConfig{
		Nodes:           nodes,
		Order:           order,
		VNodes:          100,
		Timeout:         20 * time.Second,
		Retries:         3,
		MaxMessageBytes: 8 << 20,
	}
}

// TestGatewayCLICommands drives the real CLI and client against a five-node
// in-process cluster: put with a mode override, get, mode get/set, delete,
// status, health, stats, and repair.
func TestGatewayCLICommands(t *testing.T) {
	c := support.StartCluster(t, 5)

	client := gateway.NewClientWithDialer(gatewayTestConfig(c), c.Dialer())
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	cli := gateway.NewCLI(client, out, errOut)
	ctx := context.Background()

	run := func(args ...string) string {
		t.Helper()
		out.Reset()
		errOut.Reset()
		if err := cli.Run(ctx, args); err != nil {
			t.Fatalf("gateway %v: %v\nstderr: %s", args, err, errOut.String())
		}
		return out.String()
	}

	if got := run("put", "gw-key", "hello", "--mode", "rs32"); !strings.Contains(got, "put committed") || !strings.Contains(got, "mode=rs32") {
		t.Fatalf("put output = %q", got)
	}
	if got := run("get", "gw-key"); got != "hello\n" {
		t.Fatalf("get output = %q, want hello", got)
	}
	if got := run("get", "gw-key", "--json"); !strings.Contains(got, `"value": "hello"`) {
		t.Fatalf("get --json output = %q", got)
	}

	if got := run("mode"); !strings.Contains(got, "default mode: rs32") {
		t.Fatalf("mode output = %q", got)
	}
	if got := run("mode", "replication"); !strings.Contains(got, "default mode set to replication") {
		t.Fatalf("mode set output = %q", got)
	}
	if got := run("mode", "--json"); !strings.Contains(got, `"default_mode": "replication"`) {
		t.Fatalf("mode --json output = %q", got)
	}

	if got := run("put", "gw-key-2", "world"); !strings.Contains(got, "mode=replication") {
		t.Fatalf("put with global mode = %q", got)
	}
	if got := run("delete", "gw-key"); !strings.Contains(got, "removed") {
		t.Fatalf("delete output = %q", got)
	}
	if got := run("delete", "gw-key"); !strings.Contains(got, "already absent") {
		t.Fatalf("second delete output = %q", got)
	}

	status := run("status")
	if !strings.Contains(status, "node1") || !strings.Contains(status, "node5") {
		t.Fatalf("status output = %q", status)
	}
	if health := run("health"); !strings.Contains(health, "healthy=true") {
		t.Fatalf("health output = %q", health)
	}

	var stats map[string]any
	if err := json.Unmarshal([]byte(run("stats", "--json")), &stats); err != nil {
		t.Fatalf("stats --json: %v", err)
	}
	if _, ok := stats["fragments"]; !ok {
		t.Fatalf("stats JSON = %v", stats)
	}

	if got := run("repair", "gw-key-2", "--force"); !strings.Contains(got, "repair accepted") {
		t.Fatalf("repair output = %q", got)
	}
}

// keyForEntry finds a key whose consistent-hash entry node is want.
func keyForEntry(t *testing.T, cfg config.GatewayConfig, want string) string {
	t.Helper()
	router := gateway.NewRouter(cfg.NodeAddrs(), cfg.VNodes)
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("gateway-entry-%d", i)
		if node, _ := router.GetNode(key); node == want {
			return key
		}
	}
	t.Fatalf("no key routed to %s", want)
	return ""
}

// TestGatewayUsesFollowerEntryAndForwardsToLeader proves that the ring's entry
// node is a follower and that the follower's forwarding path completes the
// write on the leader.
func TestGatewayUsesFollowerEntryAndForwardsToLeader(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	follower := c.NonLeaderIDs(t)[0]
	cfg := gatewayTestConfig(c)
	key := keyForEntry(t, cfg, follower)

	client := gateway.NewClientWithDialer(cfg, c.Dialer())
	served, resp, err := client.Put(context.Background(), &pb.PutRequest{
		Key: key, Value: []byte("forwarded"), Mode: "rs32",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	if served != follower {
		t.Fatalf("served by %q, want entry node %q", served, follower)
	}
	if served == leader.ID {
		t.Fatalf("entry node was the leader %q; test did not exercise fan-out", served)
	}
	waitWriteMeta(t, c, key, 1)
}

// TestGatewayRetriesPastStoppedEntryNode stops the entry node for a key and
// verifies that the retry loop succeeds through a ring successor.
func TestGatewayRetriesPastStoppedEntryNode(t *testing.T) {
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	cfg := gatewayTestConfig(c)
	cfg.Retries = 4

	var stopped string
	for _, id := range c.NonLeaderIDs(t) {
		if id != leader.ID {
			stopped = id
			break
		}
	}
	key := keyForEntry(t, cfg, stopped)
	c.StopNode(t, stopped)
	t.Cleanup(func() {
		// The cluster cleanup tolerates an already-stopped node; restarting it
		// only makes the failure report clearer if the assertion below fails.
		if c.Node(t, stopped).Raft == nil {
			c.StartNode(t, stopped)
		}
	})

	client := gateway.NewClientWithDialer(cfg, c.Dialer())
	served, resp, err := client.Put(context.Background(), &pb.PutRequest{
		Key: key, Value: []byte("retry-value"), Mode: "rs32",
	})
	if err != nil {
		t.Fatalf("retry Put: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("retry Put rejected: %s", resp.GetMessage())
	}
	if served == stopped {
		t.Fatalf("served by stopped node %q", stopped)
	}
	readNode, got, err := client.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get after retry Put: %v", err)
	}
	if readNode == stopped || !bytes.Equal(got.GetValue(), []byte("retry-value")) {
		t.Fatalf("Get served by %q with %d bytes", readNode, len(got.GetValue()))
	}
}
