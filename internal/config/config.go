// Package config parses and validates the runtime configuration for a single
// storage node. All values come from environment variables so the same binary
// can run in Docker Compose and in tests without a configuration file.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// Failure management defaults. The detector probes every DETECT_INTERVAL with
// DETECT_TIMEOUT, marks a node down after DETECT_FAILURES consecutive failures,
// and marks it up after DETECT_SUCCESSES consecutive successes. Repair is
// bounded by REPAIR_WORKERS and REPAIR_MAX_ATTEMPTS, sweeps every
// REPAIR_SWEEP_INTERVAL, and limits each owner to REPAIR_RATE_LIMIT uploads per
// second. Orphan fragments are kept for ORPHAN_GRACE_PERIOD so an in-flight
// write cannot lose data before its metadata commits.
const (
	DefaultDetectInterval      = 2 * time.Second
	DefaultDetectTimeout       = 1 * time.Second
	DefaultDetectFailures      = 3
	DefaultDetectSuccesses     = 1
	DefaultRepairWorkers       = 2
	DefaultRepairMaxAttempts   = 5
	DefaultRepairSweepInterval = 60 * time.Second
	DefaultRepairRateLimit     = 20
	DefaultOrphanGracePeriod   = 10 * time.Minute
	DefaultOrphanMaxIndexLag   = 1000
)

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

	// DetectInterval is the failure-detector probe period (default 2s).
	DetectInterval time.Duration
	// DetectTimeout bounds one detector probe (default 1s).
	DetectTimeout time.Duration
	// DetectFailures is the consecutive-failure threshold that marks a node
	// down (default 3).
	DetectFailures int
	// DetectSuccesses is the consecutive-success threshold that marks a node
	// up (default 1).
	DetectSuccesses int
	// RepairWorkers bounds concurrent repair handlers (default 2).
	RepairWorkers int
	// RepairMaxAttempts bounds retries per repair task (default 5).
	RepairMaxAttempts int
	// RepairSweepInterval is the periodic metadata sweep period (default 60s).
	RepairSweepInterval time.Duration
	// RepairRateLimit bounds uploads per owner per second (default 20).
	RepairRateLimit int
	// OrphanGracePeriod protects recently written fragments from the orphan
	// sweep (default 10m).
	OrphanGracePeriod time.Duration
	// OrphanMaxIndexLag bounds how far this node's applied index may trail the
	// leader's commit index before the orphan sweep skips a pass (default
	// 1000).
	OrphanMaxIndexLag uint64

	// BootstrapServers maps Raft server ID -> Raft address.
	BootstrapServers map[string]string
	// Peers maps Raft server ID -> gRPC address.
	Peers map[string]string
}

// Gateway defaults. GATEWAY_NODES is required and has no localhost fallback;
// GATEWAY_VNODES controls the virtual-node count, GATEWAY_TIMEOUT bounds one
// RPC attempt, and GATEWAY_RETRIES is the number of retries after the first
// attempt.
const (
	DefaultGatewayVNodes  = 100
	DefaultGatewayTimeout = 15 * time.Second
	DefaultGatewayRetries = 3
)

// GatewayConfig holds the client configuration for cmd/gateway. Nodes maps a
// node ID to its gRPC address and Order lists the IDs sorted so the consistent
// hash ring is built deterministically.
type GatewayConfig struct {
	Nodes           map[string]string
	Order           []string
	VNodes          int
	Timeout         time.Duration
	Retries         int
	Token           string
	MaxMessageBytes int
}

// LoadGatewayConfig reads the gateway environment. GATEWAY_NODES is required
// and must use the "id=host:port,..." form; there is deliberately no localhost
// default so a container cannot silently dial the wrong host.
func LoadGatewayConfig() (GatewayConfig, error) {
	rawNodes := strings.TrimSpace(os.Getenv("GATEWAY_NODES"))
	if rawNodes == "" {
		return GatewayConfig{}, fmt.Errorf(
			"GATEWAY_NODES is required (example: GATEWAY_NODES=node1=localhost:50051,node2=localhost:50052)")
	}

	nodes := ParsePeerMap(rawNodes)
	if len(nodes) == 0 {
		return GatewayConfig{}, fmt.Errorf(
			"GATEWAY_NODES=%q contains no valid id=host:port entries (example: GATEWAY_NODES=node1=localhost:50051,node2=localhost:50052)",
			rawNodes)
	}

	order := make([]string, 0, len(nodes))
	for id := range nodes {
		order = append(order, id)
	}
	sort.Strings(order)

	return GatewayConfig{
		Nodes:           nodes,
		Order:           order,
		VNodes:          intOr("GATEWAY_VNODES", DefaultGatewayVNodes),
		Timeout:         durationOr("GATEWAY_TIMEOUT", DefaultGatewayTimeout),
		Retries:         nonNegativeIntOr("GATEWAY_RETRIES", DefaultGatewayRetries),
		Token:           strings.TrimSpace(os.Getenv("AUTH_TOKEN")),
		MaxMessageBytes: intOr("MAX_MESSAGE_SIZE", DefaultMaxMessageSize),
	}, nil
}

// NodeAddrs returns the configured gRPC addresses in deterministic order.
// Callers that construct GatewayConfig directly may leave Order empty; the
// addresses are then sorted by node ID.
func (c GatewayConfig) NodeAddrs() []string {
	order := c.Order
	if len(order) == 0 {
		order = make([]string, 0, len(c.Nodes))
		for id := range c.Nodes {
			order = append(order, id)
		}
		sort.Strings(order)
	}

	addrs := make([]string, 0, len(order))
	for _, id := range order {
		if addr := c.Nodes[id]; addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
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
		DetectInterval:      durationOr("DETECT_INTERVAL", DefaultDetectInterval),
		DetectTimeout:       durationOr("DETECT_TIMEOUT", DefaultDetectTimeout),
		DetectFailures:      intOr("DETECT_FAILURES", DefaultDetectFailures),
		DetectSuccesses:     intOr("DETECT_SUCCESSES", DefaultDetectSuccesses),
		RepairWorkers:       intOr("REPAIR_WORKERS", DefaultRepairWorkers),
		RepairMaxAttempts:   intOr("REPAIR_MAX_ATTEMPTS", DefaultRepairMaxAttempts),
		RepairSweepInterval: durationOr("REPAIR_SWEEP_INTERVAL", DefaultRepairSweepInterval),
		RepairRateLimit:     intOr("REPAIR_RATE_LIMIT", DefaultRepairRateLimit),
		OrphanGracePeriod:   durationOr("ORPHAN_GRACE_PERIOD", DefaultOrphanGracePeriod),
		OrphanMaxIndexLag:   uint64Or("ORPHAN_MAX_INDEX_LAG", DefaultOrphanMaxIndexLag),
		Bootstrap:           os.Getenv("BOOTSTRAP") == "true",
		JoinAddr:            os.Getenv("JOIN_ADDR"),
		TLSEnabled:          os.Getenv("TLS_ENABLED") == "true",
		TLSCert:             os.Getenv("TLS_CERT_FILE"),
		TLSKey:              os.Getenv("TLS_KEY_FILE"),
		TLSCA:               os.Getenv("TLS_CA_FILE"),
		BootstrapServers:    ParsePeerMap(os.Getenv("BOOTSTRAP_SERVERS")),
		Peers:               ParsePeerMap(os.Getenv("PEERS")),
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
	if c.DetectInterval <= 0 {
		c.DetectInterval = DefaultDetectInterval
	}
	if c.DetectTimeout <= 0 {
		c.DetectTimeout = DefaultDetectTimeout
	}
	if c.DetectFailures <= 0 {
		c.DetectFailures = DefaultDetectFailures
	}
	if c.DetectSuccesses <= 0 {
		c.DetectSuccesses = DefaultDetectSuccesses
	}
	if c.RepairWorkers <= 0 {
		c.RepairWorkers = DefaultRepairWorkers
	}
	if c.RepairMaxAttempts <= 0 {
		c.RepairMaxAttempts = DefaultRepairMaxAttempts
	}
	if c.RepairSweepInterval <= 0 {
		c.RepairSweepInterval = DefaultRepairSweepInterval
	}
	if c.RepairRateLimit <= 0 {
		c.RepairRateLimit = DefaultRepairRateLimit
	}
	if c.OrphanGracePeriod <= 0 {
		c.OrphanGracePeriod = DefaultOrphanGracePeriod
	}
	if c.OrphanMaxIndexLag == 0 {
		c.OrphanMaxIndexLag = DefaultOrphanMaxIndexLag
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

func nonNegativeIntOr(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 0 {
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

func uint64Or(key string, fallback uint64) uint64 {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || value == 0 {
		fmt.Fprintf(os.Stderr, "invalid %s %q, using %d\n", key, raw, fallback)
		return fallback
	}
	return value
}

// ParsePeerMap parses "id1=host:port,id2=host:port" into a map. Entries that
// are empty or lack both an ID and an address are ignored; malformed entries are
// reported to stderr and skipped. The server uses it for PEERS and
// BOOTSTRAP_SERVERS, and the gateway uses it for GATEWAY_NODES.
func ParsePeerMap(raw string) map[string]string {
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
