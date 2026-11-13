package consensus

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

// Raft command operations. Every command is metadata-only; payload bytes never
// enter the replicated log.
const (
	OpPutMeta       = "put_meta"
	OpSetMetaStatus = "set_meta_status"
	OpGetMeta       = "get_meta"
	OpDeleteMeta    = "delete_meta"
	OpSetConfig     = "set_config"
	OpGetConfig     = "get_config"
)

// metadataMaxBytes bounds the encoded size of a metadata command. It is far
// larger than five placements plus a checksum, but small enough that a payload
// cannot be smuggled into the log through a future field.
const metadataMaxBytes = 64 * 1024

// Result error codes. Apply never panics or returns a Go error for an expected
// failure because Raft treats a panic as fatal. Callers map these codes to gRPC
// statuses.
const (
	ResultErrConflict = "conflict"
	ResultErrInvalid  = "invalid"
	ResultErrNotFound = "not_found"
	ResultErrInternal = "internal"
)

// Command is the envelope serialized into the Raft log. It carries only a
// KeyMeta, config, or identifiers; there is deliberately no value-shaped field.
type Command struct {
	Op       string                 `json:"op"`
	Key      string                 `json:"key,omitempty"`
	Meta     *storage.KeyMeta       `json:"meta,omitempty"`
	StripeID string                 `json:"stripe_id,omitempty"`
	Version  int64                  `json:"version,omitempty"`
	Degraded bool                   `json:"degraded,omitempty"`
	Config   *storage.ClusterConfig `json:"config,omitempty"`
}

// Result is returned by the FSM for metadata commands. It uses stable error
// codes rather than Go errors so a failed apply never looks like a Raft fault.
type Result struct {
	Meta    *storage.KeyMeta       `json:"meta,omitempty"`
	OldMeta *storage.KeyMeta       `json:"old_meta,omitempty"`
	Config  *storage.ClusterConfig `json:"config,omitempty"`
	Found   bool                   `json:"found"`
	Err     string                 `json:"err,omitempty"`
}

// IsMetadata reports whether op is one of the metadata-only commands.
func IsMetadata(op string) bool {
	switch op {
	case OpPutMeta, OpSetMetaStatus, OpGetMeta, OpDeleteMeta, OpSetConfig, OpGetConfig:
		return true
	default:
		return false
	}
}

func isMetadataMutation(op string) bool {
	switch op {
	case OpPutMeta, OpSetMetaStatus, OpDeleteMeta, OpSetConfig:
		return true
	default:
		return false
	}
}

// Validate checks the command fields and enforces the metadata size guard.
func (c Command) Validate() error {
	if !IsMetadata(c.Op) {
		return fmt.Errorf("unknown metadata command %q", c.Op)
	}

	switch c.Op {
	case OpPutMeta:
		if c.Meta == nil {
			return errors.New("put_meta requires metadata")
		}
		if c.Key != "" && c.Key != c.Meta.Key {
			return errors.New("put_meta key does not match metadata key")
		}
		if c.StripeID != "" && c.StripeID != c.Meta.StripeID {
			return errors.New("put_meta stripe ID does not match metadata stripe ID")
		}
		if err := validateKeyMeta(*c.Meta); err != nil {
			return err
		}
	case OpSetMetaStatus:
		if err := validateMetadataKey(c.Key); err != nil {
			return err
		}
		if strings.TrimSpace(c.StripeID) == "" {
			return errors.New("set_meta_status requires a stripe ID")
		}
		if c.Version < 1 {
			return fmt.Errorf("set_meta_status version must be at least 1, got %d", c.Version)
		}
	case OpGetMeta:
		if err := validateMetadataKey(c.Key); err != nil {
			return err
		}
	case OpDeleteMeta:
		if err := validateMetadataKey(c.Key); err != nil {
			return err
		}
		if c.Version < 0 {
			return fmt.Errorf("delete_meta version must be non-negative, got %d", c.Version)
		}
	case OpSetConfig:
		if c.Config == nil {
			return errors.New("set_config requires a config")
		}
		if err := validateConfigMode(c.Config.DefaultMode); err != nil {
			return err
		}
	case OpGetConfig:
		// No fields are required.
	}

	return validateMetadataSize(c, nil)
}

// validateMetadataSize rejects oversized metadata commands.
func validateMetadataSize(c Command, raw []byte) error {
	if raw == nil {
		data, err := json.Marshal(c)
		if err != nil {
			return fmt.Errorf("marshal metadata command: %w", err)
		}
		raw = data
	}
	if len(raw) > metadataMaxBytes {
		return fmt.Errorf("metadata command is %d bytes, limit is %d", len(raw), metadataMaxBytes)
	}
	return nil
}

func validateMetadataKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("key must not be empty")
	}
	return nil
}

func validateConfigMode(mode string) error {
	if strings.TrimSpace(mode) == "" {
		return errors.New("config mode must not be empty")
	}
	if _, err := redundancy.ParseMode(mode); err != nil {
		return err
	}
	return nil
}

func validateKeyMeta(meta storage.KeyMeta) error {
	if err := validateMetadataKey(meta.Key); err != nil {
		return err
	}
	if strings.TrimSpace(meta.StripeID) == "" {
		return errors.New("stripe ID must not be empty")
	}
	if strings.TrimSpace(meta.Mode) == "" {
		return errors.New("metadata mode must not be empty")
	}
	parsed, err := redundancy.ParseMode(meta.Mode)
	if err != nil {
		return err
	}
	if !parsed.Valid() {
		return fmt.Errorf("invalid metadata mode %q", meta.Mode)
	}
	if meta.Size < 0 {
		return fmt.Errorf("metadata size must be non-negative, got %d", meta.Size)
	}
	if err := validateChecksum(meta.Checksum); err != nil {
		return err
	}
	if meta.Version < 1 {
		return fmt.Errorf("metadata version must be at least 1, got %d", meta.Version)
	}
	if len(meta.Placement) != redundancy.TotalShards {
		return fmt.Errorf("metadata must describe exactly %d placements, got %d", redundancy.TotalShards, len(meta.Placement))
	}

	seenIndex := make(map[int]struct{}, len(meta.Placement))
	seenNode := make(map[string]struct{}, len(meta.Placement))
	for _, placement := range meta.Placement {
		if placement.ShardIndex < 0 || placement.ShardIndex >= redundancy.TotalShards {
			return fmt.Errorf("invalid shard index %d", placement.ShardIndex)
		}
		if _, exists := seenIndex[placement.ShardIndex]; exists {
			return fmt.Errorf("duplicate shard index %d", placement.ShardIndex)
		}
		seenIndex[placement.ShardIndex] = struct{}{}

		if strings.TrimSpace(placement.NodeID) == "" {
			return errors.New("placement must have a non-empty node ID")
		}
		if _, exists := seenNode[placement.NodeID]; exists {
			return fmt.Errorf("duplicate placement node %q", placement.NodeID)
		}
		seenNode[placement.NodeID] = struct{}{}

		wantParity := parsed == redundancy.ModeRS32 && placement.ShardIndex >= redundancy.RSDataShards
		if placement.IsParity != wantParity {
			return fmt.Errorf("shard %d parity=%v does not match mode %q", placement.ShardIndex, placement.IsParity, meta.Mode)
		}
	}

	return nil
}

func validateChecksum(checksum string) error {
	if len(checksum) != 64 {
		return fmt.Errorf("checksum must be 64 hex characters, got %d", len(checksum))
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return fmt.Errorf("checksum is not valid hex: %w", err)
	}
	return nil
}

func resultFromError(err error) *Result {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrConflict):
		return &Result{Err: ResultErrConflict}
	case errors.Is(err, storage.ErrNotFound):
		return &Result{Err: ResultErrNotFound}
	default:
		return &Result{Err: ResultErrInternal}
	}
}
