package config_test

import (
	"testing"

	"distributed-kv-store/internal/config"
)

// clearConfigEnv removes every variable Load reads so a case starts from a
// known state. t.Setenv restores the previous values when the test ends.
func clearConfigEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"NODE_ID",
		"PORT",
		"RAFT_PORT",
		"DATA_DIR",
		"BOOTSTRAP",
		"JOIN_ADDR",
		"TLS_ENABLED",
		"TLS_CERT_FILE",
		"TLS_KEY_FILE",
		"TLS_CA_FILE",
		"BOOTSTRAP_SERVERS",
		"PEERS",
	} {
		t.Setenv(key, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg := config.Load()

	if cfg.NodeID != "node1" {
		t.Errorf("NodeID = %q, want node1", cfg.NodeID)
	}
	if cfg.GRPCPort != "50051" {
		t.Errorf("GRPCPort = %q, want 50051", cfg.GRPCPort)
	}
	if cfg.RaftPort != "60051" {
		t.Errorf("RaftPort = %q, want 60051", cfg.RaftPort)
	}
	if cfg.DataDir != "/data" {
		t.Errorf("DataDir = %q, want /data", cfg.DataDir)
	}
	if cfg.Bootstrap {
		t.Error("Bootstrap = true, want false")
	}
	if cfg.TLSEnabled {
		t.Error("TLSEnabled = true, want false")
	}
	if len(cfg.BootstrapServers) != 0 {
		t.Errorf("BootstrapServers = %v, want empty", cfg.BootstrapServers)
	}
	if len(cfg.Peers) != 0 {
		t.Errorf("Peers = %v, want empty", cfg.Peers)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv("NODE_ID", "node4")
	t.Setenv("PORT", "50054")
	t.Setenv("RAFT_PORT", "60054")
	t.Setenv("DATA_DIR", "/tmp/node4")
	t.Setenv("BOOTSTRAP", "true")
	t.Setenv("JOIN_ADDR", "node1:50051")
	t.Setenv("TLS_ENABLED", "true")
	t.Setenv("TLS_CERT_FILE", "/certs/node.crt")
	t.Setenv("TLS_KEY_FILE", "/certs/node.key")
	t.Setenv("TLS_CA_FILE", "/certs/ca.crt")

	cfg := config.Load()

	if cfg.NodeID != "node4" {
		t.Errorf("NodeID = %q, want node4", cfg.NodeID)
	}
	if cfg.GRPCPort != "50054" {
		t.Errorf("GRPCPort = %q, want 50054", cfg.GRPCPort)
	}
	if cfg.RaftPort != "60054" {
		t.Errorf("RaftPort = %q, want 60054", cfg.RaftPort)
	}
	if cfg.DataDir != "/tmp/node4" {
		t.Errorf("DataDir = %q, want /tmp/node4", cfg.DataDir)
	}
	if !cfg.Bootstrap {
		t.Error("Bootstrap = false, want true")
	}
	if cfg.JoinAddr != "node1:50051" {
		t.Errorf("JoinAddr = %q, want node1:50051", cfg.JoinAddr)
	}
	if !cfg.TLSEnabled {
		t.Error("TLSEnabled = false, want true")
	}
	if cfg.TLSCert != "/certs/node.crt" || cfg.TLSKey != "/certs/node.key" || cfg.TLSCA != "/certs/ca.crt" {
		t.Errorf("TLS paths = %q/%q/%q, want the configured files", cfg.TLSCert, cfg.TLSKey, cfg.TLSCA)
	}
}

func TestLoadParsesPeerMaps(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv("PEERS", "node1=node1:50051, node2=node2:50052,node3=node3:50053")
	t.Setenv("BOOTSTRAP_SERVERS", "node1=node1:60051,node2=node2:60052")

	cfg := config.Load()

	wantPeers := map[string]string{
		"node1": "node1:50051",
		"node2": "node2:50052",
		"node3": "node3:50053",
	}
	for id, addr := range wantPeers {
		if got := cfg.Peers[id]; got != addr {
			t.Errorf("Peers[%q] = %q, want %q", id, got, addr)
		}
	}
	if len(cfg.Peers) != len(wantPeers) {
		t.Errorf("Peers = %v, want %d entries", cfg.Peers, len(wantPeers))
	}

	if got := cfg.BootstrapServers["node2"]; got != "node2:60052" {
		t.Errorf("BootstrapServers[node2] = %q, want node2:60052", got)
	}
}

func TestLoadIgnoresMalformedPeerEntries(t *testing.T) {
	clearConfigEnv(t)

	// Missing '=', empty id, empty address, and a bare separator must all be
	// skipped without dropping valid neighbours.
	t.Setenv("PEERS", "node1=node1:50051,broken,,=orphan,node2=,node2=node2:50052")

	cfg := config.Load()

	if len(cfg.Peers) != 2 {
		t.Fatalf("Peers = %v, want only the two valid entries", cfg.Peers)
	}
	if got := cfg.Peers["node1"]; got != "node1:50051" {
		t.Errorf("Peers[node1] = %q, want node1:50051", got)
	}
	if got := cfg.Peers["node2"]; got != "node2:50052" {
		t.Errorf("Peers[node2] = %q, want node2:50052", got)
	}
}

func TestLoadTrimsWhitespace(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv("PEERS", "  node1 = node1:50051  ")

	cfg := config.Load()

	if got := cfg.Peers["node1"]; got != "node1:50051" {
		t.Errorf("Peers[node1] = %q, want node1:50051", got)
	}
}
