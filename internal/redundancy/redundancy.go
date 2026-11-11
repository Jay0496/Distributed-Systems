// Package redundancy implements Reed-Solomon RS(3,2) erasure coding, five-copy
// replication, shard checksums, and deterministic five-node placement.
//
// This package is storage-agnostic: it turns values into shards and back, and
// maps shard indices to node IDs. The write path is responsible for moving the
// shards to their owners and committing metadata through Raft.
package redundancy

import "fmt"

// RedundancyEngine is the legacy pass-through seam kept for compatibility with
// the pre-redundancy write path.
//
// Deprecated: use Engine. This interface and NoopRedundancy are removed once
// the real engines replace them in task 03.
type RedundancyEngine interface {
	// Encode splits value into data and parity fragments according to mode.
	Encode(key string, value []byte, mode string) ([][]byte, error)
}

// NoopRedundancy is a temporary pass-through implementation so the consensus
// layer can be developed and tested independently.
//
// Deprecated: use RS32Engine or ReplicationEngine.
type NoopRedundancy struct{}

// NewNoopRedundancy returns the temporary pass-through redundancy engine.
//
// Deprecated: use NewRS32Engine or NewReplicationEngine.
func NewNoopRedundancy() RedundancyEngine {
	return NoopRedundancy{}
}

// Encode returns the value unchanged as a single fragment.
//
// Deprecated: use Engine.Encode.
func (NoopRedundancy) Encode(key string, value []byte, mode string) ([][]byte, error) {
	return [][]byte{value}, nil
}

// ValidateMode rejects unknown redundancy modes. An empty mode selects the
// default, replication.
//
// Deprecated: use ParseMode.
func ValidateMode(mode string) error {
	if _, err := ParseMode(mode); err != nil {
		return fmt.Errorf("invalid redundancy mode %q (expected %q or %q)", mode, ModeReplication, ModeRS32)
	}
	return nil
}
