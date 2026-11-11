package storage

import (
	"io"
	"time"
)

// KVStore is the legacy key-value surface used by the transitional Raft FSM.
// It is removed in task 15 once the metadata-only write path is fully adopted.
//
// New code should depend on Store, which embeds KVStore and adds the metadata,
// fragment, configuration, and observability repositories.
type KVStore interface {
	Put(key string, value []byte, isParity bool) error
	Get(key string) ([]byte, error)
}

// Store is the production persistence contract for one storage node. Metadata
// mutations are transactional and advance raft_state.last_applied_index in the
// same transaction as the metadata change.
//
// Implementations must return ErrNotFound for a missing row and ErrClosed after
// Close has been called. Other failures should wrap the underlying I/O error so
// callers can distinguish a missing record from a broken database.
type Store interface {
	// Legacy transitional KV methods. Removed in task 15.
	Put(key string, value []byte, isParity bool) error
	Get(key string) ([]byte, error)

	// FSM-facing metadata mutations. Each method updates the metadata tables
	// and raft_state.last_applied_index in one SQLite transaction.
	ApplyPutMeta(raftIndex uint64, meta KeyMeta) (old KeyMeta, err error)
	ApplyMetaStatus(raftIndex uint64, key, stripeID string, version int64, degraded bool) (KeyMeta, error)
	ApplyDeleteMeta(raftIndex uint64, key string, expectedVersion int64) (old KeyMeta, err error)
	ApplySetConfig(raftIndex uint64, cfg ClusterConfig) error

	// Metadata reads.
	GetKey(key string) (KeyMeta, error)
	ListKeys() ([]KeyMeta, error)
	LastAppliedIndex() (uint64, error)

	// Fragments.
	PutFragment(frag Fragment) error
	GetFragment(stripeID string, shardIndex int) (Fragment, error)
	DeleteFragment(stripeID string, shardIndex int) error
	DeleteStripe(stripeID string) error
	// ListFragments returns local fragment metadata ordered by stripe and shard
	// index. An empty stripeID lists every local fragment.
	ListFragments(stripeID string) ([]FragmentRef, error)

	// Cluster config. ApplySetConfig is the FSM path; PutConfig is for seeding
	// and tests.
	PutConfig(cfg ClusterConfig) error
	GetConfig() (ClusterConfig, error)

	// Observability.
	Stats() (StoreStats, error)

	io.Closer
}

// KeyMeta is the committed metadata for one key. It contains no payload bytes.
// Mode is validated at the consensus/server boundary so storage does not need
// to import internal/redundancy.
type KeyMeta struct {
	Key       string           `json:"key"`
	StripeID  string           `json:"stripe_id"`
	Mode      string           `json:"mode"`
	Size      int64            `json:"size"`
	Checksum  string           `json:"checksum"`
	Version   int64            `json:"version"`
	Degraded  bool             `json:"degraded"`
	Placement []ShardPlacement `json:"placement,omitempty"`
}

// ShardPlacement records the deterministic owner of one shard, copy, or parity
// fragment. Shard indices are global: 0-2 are RS data shards, 3-4 are RS parity
// shards, and every index is a full copy in replication mode.
type ShardPlacement struct {
	ShardIndex int    `json:"shard_index"`
	NodeID     string `json:"node_id"`
	IsParity   bool   `json:"is_parity"`
}

// Fragment is one locally stored shard or copy payload.
type Fragment struct {
	StripeID   string    `json:"stripe_id"`
	ShardIndex int       `json:"shard_index"`
	IsParity   bool      `json:"is_parity"`
	Data       []byte    `json:"data"`
	Checksum   string    `json:"checksum"`
	CreatedAt  time.Time `json:"created_at"`
}

// FragmentRef is fragment metadata without the payload. Repair scheduling and
// the orphan sweep use it to avoid loading fragment bytes.
type FragmentRef struct {
	StripeID   string    `json:"stripe_id"`
	ShardIndex int       `json:"shard_index"`
	IsParity   bool      `json:"is_parity"`
	Size       int64     `json:"size"`
	Checksum   string    `json:"checksum"`
	CreatedAt  time.Time `json:"created_at"`
}

// ClusterConfig is the singleton cluster-wide configuration stored through
// Raft. DefaultMode is "rs32" or "replication".
type ClusterConfig struct {
	DefaultMode string `json:"default_mode"`
}

// StoreStats describes local storage usage.
type StoreStats struct {
	Bytes         int64 `json:"bytes"`          // SQLite + WAL + SHM file sizes
	FragmentBytes int64 `json:"fragment_bytes"` // payload bytes in the fragments table
	KeyCount      int64 `json:"key_count"`
	StripeCount   int64 `json:"stripe_count"`
	FragmentCount int64 `json:"fragment_count"`
}
