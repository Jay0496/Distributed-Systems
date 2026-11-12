package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/gateway"
	"distributed-kv-store/tests/support"
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
