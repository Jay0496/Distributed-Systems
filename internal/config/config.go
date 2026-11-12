// Package config parses and validates the runtime configuration for a single
// storage node. All values come from environment variables so the same binary
// can run in Docker Compose and in tests without a configuration file.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Fragment transport defaults. MAX_FRAGMENT_SIZE bounds one complete
// fragment, MAX_MESSAGE_SIZE bounds one gRPC message, UPLOAD_CHUNK_SIZE is the
// chunk size clients use (and the server uses when streaming downloads), and
// FRAGMENT_TIMEOUT bounds one upload or download operation.
const (
	DefaultMaxFragmentSize = 64 << 20
	DefaultMaxMessageSize  = 32 << 20
	DefaultUploadChunkSize = 1 << 20
	DefaultFragmentTimeout = 10 * time.Second
)

// Write path defaults. MAX_VALUE_SIZE bounds the original client value,
// WRITE_TIMEOUT bounds one whole Put (metadata reads and the Raft commit), and
// SHARD_UPLOAD_TIMEOUT bounds one owner's fragment upload attempt.
const (
	DefaultMaxValueSize       = 8 << 20
	DefaultWriteTimeout       = 15 * time.Second
	DefaultShardUploadTimeout = 5 * time.Second
)

// Read path defaults. READ_TIMEOUT bounds one owner fetch, READ_PARALLELISM
// bounds concurrent owner fetches, and READ_REPAIR_WORKERS bounds the
// asynchronous read-repair worker pool.
const (
	DefaultReadTimeout       = 5 * time.Second
	DefaultReadParallelism   = 5
	DefaultReadRepairWorkers = 2
)

// Admin defaults. CLUSTER_PROBE_TIMEOUT bounds one ClusterHealth probe of one
// node. AUTH_TOKEN, when non-empty, is attached to outbound internal RPCs and
// requires an authorizer; server-side enforcement lands in task 10.
const DefaultClusterProbeTimeout = 1 * time.Second

// Config holds the runtime configuration for a single storage node.
type Config struct {
	NodeID     string
	GRPCPort   string
	RaftPort   string
	DataDir    string
	Bootstrap  bool
	JoinAddr   string
	TLSEnabled bool
	TLSCert    string
	TLSKey     string
	TLSCA      string

	// StorePath is the SQLite database for metadata and local fragments.
	StorePath string
	// StoreBusyTimeout bounds SQLite lock waits.
	StoreBusyTimeout time.Duration

	// MaxFragmentSize bounds one complete fragment payload (default 64 MiB).
	MaxFragmentSize int
	// MaxMessageSize bounds one gRPC request or response (default 32 MiB).
	MaxMessageSize int
	// UploadChunkSize is the fragment chunk size used by clients and by
	// streaming downloads (default 1 MiB).
	UploadChunkSize int
	// FragmentTimeout bounds one upload or download operation (default 10s).
	FragmentTimeout time.Duration

	// MaxValueSize bounds the original value in a Put (default 8 MiB).
	MaxValueSize int
	// WriteTimeout bounds one whole write, including the Raft metadata commit
	// (default 15s).
	WriteTimeout time.Duration
	// ShardUploadTimeout bounds one shard upload to one owner (default 5s).
	ShardUploadTimeout time.Duration

	// ReadTimeout bounds one owner fragment fetch (default 5s).
	ReadTimeout time.Duration
	// ReadParallelism bounds concurrent owner fragment fetches (default 5).
	ReadParallelism int
	// ReadRepairWorkers bounds the asynchronous read-repair worker pool
	// (default 2).
	ReadRepairWorkers int

	// ClusterProbeTimeout bounds one node probe issued by ClusterHealth
	// (default 1s).
	ClusterProbeTimeout time.Duration
	// AuthToken is the shared bearer token attached to outbound internal
	// RPCs. Empty disables auth. Task 10 adds server-side enforcement.
	AuthToken string

	// BootstrapServers maps Raft server ID -> Raft address.
	BootstrapServers map[string]string
	// Peers maps Raft server ID -> gRPC address.
	Peers map[string]string
}

// Load reads the node configuration from environment variables.
func Load() Config {
	dataDir := envOr("DATA_DIR", "/data")

	cfg := Config{
		NodeID:              envOr("NODE_ID", "node1"),
		GRPCPort:            envOr("PORT", "50051"),
		RaftPort:            envOr("RAFT_PORT", "60051"),
		DataDir:             dataDir,
		StorePath:           envOr("STORE_PATH", filepath.Join(dataDir, "node.db")),
		StoreBusyTimeout:    durationOr("STORE_BUSY_TIMEOUT", 5*time.Second),
		MaxFragmentSize:     intOr("MAX_FRAGMENT_SIZE", DefaultMaxFragmentSize),
		MaxMessageSize:      intOr("MAX_MESSAGE_SIZE", DefaultMaxMessageSize),
		UploadChunkSize:     intOr("UPLOAD_CHUNK_SIZE", DefaultUploadChunkSize),
		FragmentTimeout:     durationOr("FRAGMENT_TIMEOUT", DefaultFragmentTimeout),
		MaxValueSize:        intOr("MAX_VALUE_SIZE", DefaultMaxValueSize),
		WriteTimeout:        durationOr("WRITE_TIMEOUT", DefaultWriteTimeout),
		ShardUploadTimeout:  durationOr("SHARD_UPLOAD_TIMEOUT", DefaultShardUploadTimeout),
		ReadTimeout:         durationOr("READ_TIMEOUT", DefaultReadTimeout),
		ReadParallelism:     intOr("READ_PARALLELISM", DefaultReadParallelism),
		ReadRepairWorkers:   intOr("READ_REPAIR_WORKERS", DefaultReadRepairWorkers),
		ClusterProbeTimeout: durationOr("CLUSTER_PROBE_TIMEOUT", DefaultClusterProbeTimeout),
		AuthToken:           strings.TrimSpace(os.Getenv("AUTH_TOKEN")),
		Bootstrap:           os.Getenv("BOOTSTRAP") == "true",
		JoinAddr:            os.Getenv("JOIN_ADDR"),
		TLSEnabled:          os.Getenv("TLS_ENABLED") == "true",
		TLSCert:             os.Getenv("TLS_CERT_FILE"),
		TLSKey:              os.Getenv("TLS_KEY_FILE"),
		TLSCA:               os.Getenv("TLS_CA_FILE"),
		BootstrapServers:    parsePeerMap(os.Getenv("BOOTSTRAP_SERVERS")),
		Peers:               parsePeerMap(os.Getenv("PEERS")),
	}
	return cfg
}

// WithDefaults fills unset fragment transport limits with their documented
// defaults. It lets callers that construct Config directly, including tests,
// rely on the same limits as a process started through Load.
func (c Config) WithDefaults() Config {
	if c.MaxFragmentSize <= 0 {
		c.MaxFragmentSize = DefaultMaxFragmentSize
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = DefaultMaxMessageSize
	}
	if c.UploadChunkSize <= 0 {
		c.UploadChunkSize = DefaultUploadChunkSize
	}
	if c.FragmentTimeout <= 0 {
		c.FragmentTimeout = DefaultFragmentTimeout
	}
	if c.MaxValueSize <= 0 {
		c.MaxValueSize = DefaultMaxValueSize
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.ShardUploadTimeout <= 0 {
		c.ShardUploadTimeout = DefaultShardUploadTimeout
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = DefaultReadTimeout
	}
	if c.ReadParallelism <= 0 {
		c.ReadParallelism = DefaultReadParallelism
	}
	if c.ReadRepairWorkers <= 0 {
		c.ReadRepairWorkers = DefaultReadRepairWorkers
	}
	if c.ClusterProbeTimeout <= 0 {
		c.ClusterProbeTimeout = DefaultClusterProbeTimeout
	}
	return c
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intOr(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		fmt.Fprintf(os.Stderr, "invalid %s %q, using %d\n", key, raw, fallback)
		return fallback
	}
	return value
}

func durationOr(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		fmt.Fprintf(os.Stderr, "invalid %s %q, using %s\n", key, raw, fallback)
		return fallback
	}
	return value
}

// parsePeerMap parses "id1=host:port,id2=host:port" into a map.
func parsePeerMap(raw string) map[string]string {
	out := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			fmt.Fprintf(os.Stderr, "ignoring malformed peer entry %q\n", entry)
			continue
		}
		out[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return out
}
