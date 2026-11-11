package redundancy

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// RS32Engine implements Reed-Solomon RS(3,2) erasure coding.
//
// A value is split into three equal data shards and two parity shards, all of
// the same length. Shard indices are global: 0-2 are data and 3-4 are parity.
type RS32Engine struct {
	codec reedsolomon.Encoder
}

var _ Engine = (*RS32Engine)(nil)

// NewRS32Engine returns an RS(3,2) engine. The parameters are fixed, so the
// constructor panics if the library rejects them; that would be a programming
// error rather than a runtime condition.
func NewRS32Engine() *RS32Engine {
	codec, err := newRS32Codec()
	if err != nil {
		panic("redundancy: invalid RS(3,2) parameters: " + err.Error())
	}
	return &RS32Engine{codec: codec}
}

// Encode splits value into five RS shards. The final data shards are zero-padded
// to the common shard size required by the library.
func (e *RS32Engine) Encode(value []byte, mode Mode) ([]Shard, error) {
	resolved, err := resolveMode(mode)
	if err != nil {
		return nil, err
	}
	if resolved != ModeRS32 {
		return nil, fmt.Errorf("RS32Engine cannot encode mode %q", resolved)
	}

	shardSize := shardSizeFor(len(value))
	encoded := make([][]byte, TotalShards)
	for i := range encoded {
		encoded[i] = make([]byte, shardSize)
	}

	// Data shards receive the value in order; the final shard keeps the
	// zero padding that Reed-Solomon requires for equal-length inputs.
	offset := 0
	for i := 0; i < RSDataShards; i++ {
		copied := copy(encoded[i], value[offset:])
		offset += copied
	}

	if err := e.codec.Encode(encoded); err != nil {
		return nil, fmt.Errorf("encode RS shards: %w", err)
	}

	shards := make([]Shard, TotalShards)
	for i, data := range encoded {
		shards[i] = Shard{
			Index:     i,
			IsParity:  i >= RSDataShards,
			Data:      data,
			Checksum:  Checksum(data),
			TotalSize: int64(len(value)),
		}
	}
	return shards, nil
}

// Decode reconstructs the value from any three or more distinct RS shards. The
// shards may be any mix of data and parity. Every supplied shard checksum is
// verified before reconstruction, and the decoded length is truncated to
// totalSize so padding never appears in the returned value.
func (e *RS32Engine) Decode(shards []Shard, mode Mode, totalSize int64) ([]byte, error) {
	resolved, err := resolveMode(mode)
	if err != nil {
		return nil, err
	}
	if resolved != ModeRS32 {
		return nil, fmt.Errorf("RS32Engine cannot decode mode %q", resolved)
	}
	if len(shards) < RSDataShards {
		return nil, fmt.Errorf("RS(3,2) decode requires at least %d shards, got %d", RSDataShards, len(shards))
	}
	if err := ValidateShards(shards, ModeRS32, totalSize); err != nil {
		return nil, err
	}

	shardSize := len(shards[0].Data)
	if shardSize == 0 {
		return nil, fmt.Errorf("RS shards must not be empty")
	}

	matrix := make([][]byte, TotalShards)
	for _, shard := range shards {
		if len(shard.Data) != shardSize {
			return nil, fmt.Errorf("shard %d length %d does not match shard length %d", shard.Index, len(shard.Data), shardSize)
		}
		// Clone so reconstruct cannot mutate the caller's shards.
		matrix[shard.Index] = append([]byte(nil), shard.Data...)
	}

	missing := 0
	for _, data := range matrix {
		if len(data) == 0 {
			missing++
		}
	}
	if missing > RSParityShards {
		return nil, fmt.Errorf("RS(3,2) decode tolerates at most %d missing shards, got %d", RSParityShards, missing)
	}

	if missing > 0 {
		if err := e.codec.Reconstruct(matrix); err != nil {
			return nil, fmt.Errorf("reconstruct RS shards: %w", err)
		}
	}
	ok, err := e.codec.Verify(matrix)
	if err != nil {
		return nil, fmt.Errorf("verify RS shards: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("verify RS shards: parity mismatch")
	}

	decoded := make([]byte, 0, shardSize*RSDataShards)
	for i := 0; i < RSDataShards; i++ {
		if len(matrix[i]) == 0 {
			return nil, fmt.Errorf("data shard %d was not reconstructed", i)
		}
		decoded = append(decoded, matrix[i]...)
	}

	if int64(len(decoded)) < totalSize {
		return nil, fmt.Errorf("decoded %d bytes is smaller than total size %d", len(decoded), totalSize)
	}
	return decoded[:totalSize], nil
}

// shardSizeFor returns the common shard size for an original value. The minimum
// of one byte is required because Reed-Solomon cannot encode zero-length
// shards; an empty value therefore occupies five one-byte shards.
func shardSizeFor(valueLen int) int {
	if valueLen == 0 {
		return 1
	}
	return (valueLen + RSDataShards - 1) / RSDataShards
}
