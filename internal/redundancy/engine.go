package redundancy

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// newRS32Codec builds the fixed RS(3,2) encoder used by RS32Engine.
func newRS32Codec() (reedsolomon.Encoder, error) {
	return reedsolomon.New(RSDataShards, RSParityShards)
}

// Engine encodes a value into the shards or copies for a mode and reconstructs
// the original value from a sufficient subset.
type Engine interface {
	// Encode splits value into TotalShards fragments according to mode.
	Encode(value []byte, mode Mode) ([]Shard, error)
	// Decode reconstructs the original value from shards. totalSize is the
	// original value size recorded in the stripe metadata.
	Decode(shards []Shard, mode Mode, totalSize int64) ([]byte, error)
}

// resolveMode resolves the empty string to the default mode and rejects unknown
// modes. Engines use it so callers can pass an unset per-request override.
func resolveMode(mode Mode) (Mode, error) {
	if mode == "" {
		return ModeReplication, nil
	}
	if !mode.Valid() {
		return "", fmt.Errorf("invalid redundancy mode %q", mode)
	}
	return mode, nil
}
