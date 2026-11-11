package config_test

import (
	"path/filepath"
	"testing"
	"time"

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
		"STORE_PATH",
		"STORE_BUSY_TIMEOUT",
		"MAX_FRAGMENT_SIZE",
		"MAX_MESSAGE_SIZE",
		"UPLOAD_CHUNK_SIZE",
		"FRAGMENT_TIMEOUT",
		"MAX_VALUE_SIZE",
		"WRITE_TIMEOUT",
		"SHARD_UPLOAD_TIMEOUT",
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
	if want := filepath.Join("/data", "node.db"); cfg.StorePath != want {
		t.Errorf("StorePath = %q, want %q", cfg.StorePath, want)
	}
	if cfg.StoreBusyTimeout != 5*time.Second {
		t.Errorf("StoreBusyTimeout = %v, want 5s", cfg.StoreBusyTimeout)
	}
	if cfg.MaxFragmentSize != config.DefaultMaxFragmentSize {
		t.Errorf("MaxFragmentSize = %d, want %d", cfg.MaxFragmentSize, config.DefaultMaxFragmentSize)
	}
	if cfg.MaxMessageSize != config.DefaultMaxMessageSize {
		t.Errorf("MaxMessageSize = %d, want %d", cfg.MaxMessageSize, config.DefaultMaxMessageSize)
	}
	if cfg.UploadChunkSize != config.DefaultUploadChunkSize {
		t.Errorf("UploadChunkSize = %d, want %d", cfg.UploadChunkSize, config.DefaultUploadChunkSize)
	}
	if cfg.FragmentTimeout != config.DefaultFragmentTimeout {
		t.Errorf("FragmentTimeout = %v, want %v", cfg.FragmentTimeout, config.DefaultFragmentTimeout)
	}
	if cfg.MaxValueSize != config.DefaultMaxValueSize {
		t.Errorf("MaxValueSize = %d, want %d", cfg.MaxValueSize, config.DefaultMaxValueSize)
	}
	if cfg.WriteTimeout != config.DefaultWriteTimeout {
		t.Errorf("WriteTimeout = %v, want %v", cfg.WriteTimeout, config.DefaultWriteTimeout)
	}
	if cfg.ShardUploadTimeout != config.DefaultShardUploadTimeout {
		t.Errorf("ShardUploadTimeout = %v, want %v", cfg.ShardUploadTimeout, config.DefaultShardUploadTimeout)
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
	t.Setenv("STORE_PATH", "/tmp/node4/custom.db")
	t.Setenv("STORE_BUSY_TIMEOUT", "2s")
	t.Setenv("MAX_FRAGMENT_SIZE", "1048576")
	t.Setenv("MAX_MESSAGE_SIZE", "524288")
	t.Setenv("UPLOAD_CHUNK_SIZE", "65536")
	t.Setenv("FRAGMENT_TIMEOUT", "3s")
	t.Setenv("MAX_VALUE_SIZE", "4194304")
	t.Setenv("WRITE_TIMEOUT", "20s")
	t.Setenv("SHARD_UPLOAD_TIMEOUT", "2s")
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
	if cfg.StorePath != "/tmp/node4/custom.db" {
		t.Errorf("StorePath = %q, want /tmp/node4/custom.db", cfg.StorePath)
	}
	if cfg.StoreBusyTimeout != 2*time.Second {
		t.Errorf("StoreBusyTimeout = %v, want 2s", cfg.StoreBusyTimeout)
	}
	if cfg.MaxFragmentSize != 1048576 {
		t.Errorf("MaxFragmentSize = %d, want 1048576", cfg.MaxFragmentSize)
	}
	if cfg.MaxMessageSize != 524288 {
		t.Errorf("MaxMessageSize = %d, want 524288", cfg.MaxMessageSize)
	}
	if cfg.UploadChunkSize != 65536 {
		t.Errorf("UploadChunkSize = %d, want 65536", cfg.UploadChunkSize)
	}
	if cfg.FragmentTimeout != 3*time.Second {
		t.Errorf("FragmentTimeout = %v, want 3s", cfg.FragmentTimeout)
	}
	if cfg.MaxValueSize != 4194304 {
		t.Errorf("MaxValueSize = %d, want 4194304", cfg.MaxValueSize)
	}
	if cfg.WriteTimeout != 20*time.Second {
		t.Errorf("WriteTimeout = %v, want 20s", cfg.WriteTimeout)
	}
	if cfg.ShardUploadTimeout != 2*time.Second {
		t.Errorf("ShardUploadTimeout = %v, want 2s", cfg.ShardUploadTimeout)
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

func TestLoadRejectsInvalidBusyTimeout(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv("STORE_BUSY_TIMEOUT", "not-a-duration")

	cfg := config.Load()

	if cfg.StoreBusyTimeout != 5*time.Second {
		t.Errorf("StoreBusyTimeout = %v, want fallback 5s", cfg.StoreBusyTimeout)
	}
}

func TestLoadRejectsInvalidFragmentLimits(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv("MAX_FRAGMENT_SIZE", "-1")
	t.Setenv("MAX_MESSAGE_SIZE", "not-a-number")
	t.Setenv("UPLOAD_CHUNK_SIZE", "0")
	t.Setenv("FRAGMENT_TIMEOUT", "-2s")
	t.Setenv("MAX_VALUE_SIZE", "0")
	t.Setenv("WRITE_TIMEOUT", "not-a-duration")
	t.Setenv("SHARD_UPLOAD_TIMEOUT", "-1s")

	cfg := config.Load()

	if cfg.MaxFragmentSize != config.DefaultMaxFragmentSize {
		t.Errorf("MaxFragmentSize = %d, want fallback %d", cfg.MaxFragmentSize, config.DefaultMaxFragmentSize)
	}
	if cfg.MaxMessageSize != config.DefaultMaxMessageSize {
		t.Errorf("MaxMessageSize = %d, want fallback %d", cfg.MaxMessageSize, config.DefaultMaxMessageSize)
	}
	if cfg.UploadChunkSize != config.DefaultUploadChunkSize {
		t.Errorf("UploadChunkSize = %d, want fallback %d", cfg.UploadChunkSize, config.DefaultUploadChunkSize)
	}
	if cfg.FragmentTimeout != config.DefaultFragmentTimeout {
		t.Errorf("FragmentTimeout = %v, want fallback %v", cfg.FragmentTimeout, config.DefaultFragmentTimeout)
	}
	if cfg.MaxValueSize != config.DefaultMaxValueSize {
		t.Errorf("MaxValueSize = %d, want fallback %d", cfg.MaxValueSize, config.DefaultMaxValueSize)
	}
	if cfg.WriteTimeout != config.DefaultWriteTimeout {
		t.Errorf("WriteTimeout = %v, want fallback %v", cfg.WriteTimeout, config.DefaultWriteTimeout)
	}
	if cfg.ShardUploadTimeout != config.DefaultShardUploadTimeout {
		t.Errorf("ShardUploadTimeout = %v, want fallback %v", cfg.ShardUploadTimeout, config.DefaultShardUploadTimeout)
	}
}

func TestWithDefaultsFillsZeroValues(t *testing.T) {
	cfg := config.Config{}.WithDefaults()

	if cfg.MaxFragmentSize != config.DefaultMaxFragmentSize ||
		cfg.MaxMessageSize != config.DefaultMaxMessageSize ||
		cfg.UploadChunkSize != config.DefaultUploadChunkSize ||
		cfg.FragmentTimeout != config.DefaultFragmentTimeout ||
		cfg.MaxValueSize != config.DefaultMaxValueSize ||
		cfg.WriteTimeout != config.DefaultWriteTimeout ||
		cfg.ShardUploadTimeout != config.DefaultShardUploadTimeout {
		t.Fatalf("WithDefaults = %+v", cfg)
	}

	explicit := config.Config{
		MaxFragmentSize:    10,
		MaxMessageSize:     9,
		UploadChunkSize:    8,
		FragmentTimeout:    time.Second,
		MaxValueSize:       7,
		WriteTimeout:       6 * time.Second,
		ShardUploadTimeout: 5 * time.Second,
	}.WithDefaults()
	if explicit.MaxFragmentSize != 10 || explicit.MaxMessageSize != 9 ||
		explicit.UploadChunkSize != 8 || explicit.FragmentTimeout != time.Second ||
		explicit.MaxValueSize != 7 || explicit.WriteTimeout != 6*time.Second ||
		explicit.ShardUploadTimeout != 5*time.Second {
		t.Fatalf("WithDefaults overwrote explicit values: %+v", explicit)
	}
}
