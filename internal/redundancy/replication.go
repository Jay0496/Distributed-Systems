package redundancy

import (
	"fmt"
	"strings"
)

// ReplicationEngine implements five full copies of a value.
//
// Every copy carries the same global index pattern 0..4. All indices are data
// copies, so IsParity is false for every shard.
type ReplicationEngine struct{}

var _ Engine = (*ReplicationEngine)(nil)

// NewReplicationEngine returns a five-copy replication engine.
func NewReplicationEngine() *ReplicationEngine {
	return &ReplicationEngine{}
}

// Encode returns TotalShards identical full copies of value.
func (e *ReplicationEngine) Encode(value []byte, mode Mode) ([]Shard, error) {
	resolved, err := resolveMode(mode)
	if err != nil {
		return nil, err
	}
	if resolved != ModeReplication {
		return nil, fmt.Errorf("ReplicationEngine cannot encode mode %q", resolved)
	}

	checksum := Checksum(value)
	totalSize := int64(len(value))
	shards := make([]Shard, TotalShards)
	for i := range shards {
		shards[i] = Shard{
			Index:     i,
			IsParity:  false,
			Data:      append([]byte(nil), value...),
			Checksum:  checksum,
			TotalSize: totalSize,
		}
	}
	return shards, nil
}

// Decode returns the value from any checksum-valid copy. Corrupt copies are
// skipped when a valid copy exists; when none is valid, the returned error lists
// every mismatch so repair has something actionable to report.
func (e *ReplicationEngine) Decode(shards []Shard, mode Mode, totalSize int64) ([]byte, error) {
	resolved, err := resolveMode(mode)
	if err != nil {
		return nil, err
	}
	if resolved != ModeReplication {
		return nil, fmt.Errorf("ReplicationEngine cannot decode mode %q", resolved)
	}
	if totalSize < 0 {
		return nil, fmt.Errorf("negative total size %d", totalSize)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("no replication copies supplied")
	}

	seen := make(map[int]struct{}, len(shards))
	for _, shard := range shards {
		if shard.Index < 0 || shard.Index >= TotalShards {
			return nil, fmt.Errorf("shard index %d out of range [0,%d)", shard.Index, TotalShards)
		}
		if _, duplicate := seen[shard.Index]; duplicate {
			return nil, fmt.Errorf("duplicate shard index %d", shard.Index)
		}
		seen[shard.Index] = struct{}{}
	}

	mismatches := make([]string, 0, len(shards))
	for _, shard := range shards {
		if shard.TotalSize != 0 && shard.TotalSize != totalSize {
			mismatches = append(mismatches, fmt.Sprintf("shard %d declares total size %d, want %d", shard.Index, shard.TotalSize, totalSize))
			continue
		}
		if int64(len(shard.Data)) != totalSize {
			mismatches = append(mismatches, fmt.Sprintf("shard %d has %d bytes, want %d", shard.Index, len(shard.Data), totalSize))
			continue
		}
		if err := VerifyChecksum(shard.Data, shard.Checksum); err != nil {
			mismatches = append(mismatches, fmt.Sprintf("shard %d: %v", shard.Index, err))
			continue
		}
		return append([]byte(nil), shard.Data...), nil
	}

	return nil, fmt.Errorf("no valid replication copy: %s", strings.Join(mismatches, "; "))
}
