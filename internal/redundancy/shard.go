package redundancy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const (
	// TotalShards is the number of shards or copies written for every stripe.
	TotalShards = 5
	// RSDataShards is the number of data shards in RS(3,2).
	RSDataShards = 3
	// RSParityShards is the number of parity shards in RS(3,2).
	RSParityShards = 2
)

// Shard is one encoded fragment of a value.
//
// Index is global across both modes: 0-2 are Reed-Solomon data shards, 3-4 are
// Reed-Solomon parity shards, and every index is a full copy in replication
// mode. TotalSize repeats the original value size on every shard so a single
// shard is self-describing during transfer and repair.
type Shard struct {
	Index     int    // global index: 0..4
	IsParity  bool   // true for RS indices 3 and 4; false for replication
	Data      []byte // shard bytes, or a full copy in replication mode
	Checksum  string // SHA-256 hex of Data
	TotalSize int64  // original value size, repeated on every shard
}

// Checksum returns the lowercase hex SHA-256 of data.
func Checksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// VerifyChecksum reports whether data matches the expected SHA-256 hex digest.
func VerifyChecksum(data []byte, expected string) error {
	if expected == "" {
		return fmt.Errorf("missing checksum")
	}
	if actual := Checksum(data); actual != expected {
		return fmt.Errorf("checksum mismatch: got %s, want %s", actual, expected)
	}
	return nil
}

// Verify checks the shard's global index and payload checksum. Mode-specific
// length rules are enforced by the engines and ValidateShards.
func (s Shard) Verify() error {
	if s.Index < 0 || s.Index >= TotalShards {
		return fmt.Errorf("shard index %d out of range [0,%d)", s.Index, TotalShards)
	}
	if s.TotalSize < 0 {
		return fmt.Errorf("shard %d has negative total size %d", s.Index, s.TotalSize)
	}
	if err := VerifyChecksum(s.Data, s.Checksum); err != nil {
		return fmt.Errorf("shard %d: %w", s.Index, err)
	}
	return nil
}

// ValidateShards performs the validation shared by both engines: mode, index
// range, index uniqueness, checksum, and total-size consistency.
//
// A shard TotalSize of zero is treated as unspecified when totalSize is
// non-zero, which keeps hand-built test fixtures and repair requests simple.
// Concrete engines still enforce their own payload-length rules.
func ValidateShards(shards []Shard, mode Mode, totalSize int64) error {
	if !mode.Valid() {
		return fmt.Errorf("invalid redundancy mode %q", mode)
	}
	if totalSize < 0 {
		return fmt.Errorf("negative total size %d", totalSize)
	}
	if len(shards) == 0 {
		return fmt.Errorf("no shards supplied")
	}

	seen := make(map[int]struct{}, len(shards))
	for _, shard := range shards {
		if err := shard.Verify(); err != nil {
			return err
		}
		if _, duplicate := seen[shard.Index]; duplicate {
			return fmt.Errorf("duplicate shard index %d", shard.Index)
		}
		seen[shard.Index] = struct{}{}
		if shard.TotalSize != 0 && shard.TotalSize != totalSize {
			return fmt.Errorf("shard %d total size %d does not match expected %d", shard.Index, shard.TotalSize, totalSize)
		}
	}
	return nil
}
