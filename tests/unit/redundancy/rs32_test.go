package redundancy_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

func testValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte('a' + (i % 26))
	}
	return value
}

func TestRS32EncodeLayout(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	sizes := []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 1024}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			value := testValue(size)
			shards, err := engine.Encode(value, redundancy.ModeRS32)
			if err != nil {
				t.Fatalf("Encode(%d bytes) returned error: %v", size, err)
			}
			if len(shards) != redundancy.TotalShards {
				t.Fatalf("Encode(%d bytes) returned %d shards, want %d", size, len(shards), redundancy.TotalShards)
			}

			wantShardSize := (size + redundancy.RSDataShards - 1) / redundancy.RSDataShards
			if wantShardSize < 1 {
				wantShardSize = 1
			}

			for i, shard := range shards {
				if shard.Index != i {
					t.Errorf("shard %d has index %d", i, shard.Index)
				}
				if wantParity := i >= redundancy.RSDataShards; shard.IsParity != wantParity {
					t.Errorf("shard %d IsParity = %v, want %v", i, shard.IsParity, wantParity)
				}
				if len(shard.Data) != wantShardSize {
					t.Errorf("shard %d length = %d, want %d", i, len(shard.Data), wantShardSize)
				}
				if shard.TotalSize != int64(size) {
					t.Errorf("shard %d TotalSize = %d, want %d", i, shard.TotalSize, size)
				}
				if err := shard.Verify(); err != nil {
					t.Errorf("shard %d failed verification: %v", i, err)
				}
			}
		})
	}
}

func TestRS32ZeroPaddingNeverLeaks(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	for size := 1; size <= 10; size++ {
		value := testValue(size)
		shards, err := engine.Encode(value, redundancy.ModeRS32)
		if err != nil {
			t.Fatalf("Encode(%d bytes): %v", size, err)
		}
		decoded, err := engine.Decode(shards, redundancy.ModeRS32, int64(size))
		if err != nil {
			t.Fatalf("Decode(%d bytes): %v", size, err)
		}
		if !bytes.Equal(decoded, value) {
			t.Fatalf("Decode(%d bytes) = %q, want %q", size, decoded, value)
		}
		if len(decoded) != size {
			t.Fatalf("Decode(%d bytes) returned %d bytes", size, len(decoded))
		}
	}
}

func TestRS32RoundTrip(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	sizes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1024}

	for _, size := range sizes {
		value := testValue(size)
		shards, err := engine.Encode(value, redundancy.ModeRS32)
		if err != nil {
			t.Fatalf("Encode(%d bytes): %v", size, err)
		}
		decoded, err := engine.Decode(shards, redundancy.ModeRS32, int64(size))
		if err != nil {
			t.Fatalf("Decode(%d bytes): %v", size, err)
		}
		if !bytes.Equal(decoded, value) {
			t.Fatalf("round trip for %d bytes changed the value", size)
		}
	}
}

func TestRS32DecodeEveryThreeShardSubset(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	value := []byte("hello world")
	shards, err := engine.Encode(value, redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	for i := 0; i < len(shards); i++ {
		for j := i + 1; j < len(shards); j++ {
			for k := j + 1; k < len(shards); k++ {
				subset := []redundancy.Shard{shards[i], shards[j], shards[k]}
				decoded, err := engine.Decode(subset, redundancy.ModeRS32, int64(len(value)))
				if err != nil {
					t.Fatalf("Decode(shards %d,%d,%d): %v", i, j, k, err)
				}
				if !bytes.Equal(decoded, value) {
					t.Fatalf("Decode(shards %d,%d,%d) = %q, want %q", i, j, k, decoded, value)
				}
			}
		}
	}
}

func TestRS32DecodeRejectsInvalidInput(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	value := []byte("hello world")
	shards, err := engine.Encode(value, redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	totalSize := int64(len(value))

	corrupt := func(index int) []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		out[index].Data = append([]byte(nil), out[index].Data...)
		out[index].Data[0] ^= 0xFF
		return out
	}
	withIndex := func(index int) []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		out[0].Index = index
		return out
	}
	mismatchedLength := func() []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		out[0].Data = append([]byte(nil), out[0].Data[:len(out[0].Data)-1]...)
		out[0].Checksum = redundancy.Checksum(out[0].Data)
		return out
	}
	mismatchedTotalSize := func() []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		out[0].TotalSize = totalSize + 1
		return out
	}
	oversizedTotalSize := func(size int64) []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		for i := range out {
			out[i].TotalSize = size
		}
		return out
	}

	cases := []struct {
		name      string
		shards    []redundancy.Shard
		mode      redundancy.Mode
		totalSize int64
		want      string
	}{
		{
			name:      "fewer than three",
			shards:    shards[:2],
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "at least 3",
		},
		{
			name:      "more than two missing",
			shards:    shards[:2],
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "at least 3",
		},
		{
			name:      "duplicate index",
			shards:    []redundancy.Shard{shards[0], shards[0], shards[1]},
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "duplicate shard index",
		},
		{
			name:      "index out of range",
			shards:    withIndex(redundancy.TotalShards),
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "out of range",
		},
		{
			name:      "corrupted shard",
			shards:    corrupt(1),
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "checksum mismatch",
		},
		{
			name:      "mismatched shard lengths",
			shards:    mismatchedLength(),
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "does not match shard length",
		},
		{
			name:      "inconsistent total size",
			shards:    mismatchedTotalSize(),
			mode:      redundancy.ModeRS32,
			totalSize: totalSize,
			want:      "does not match expected",
		},
		{
			name:      "total size larger than decoded value",
			shards:    oversizedTotalSize(totalSize + 100),
			mode:      redundancy.ModeRS32,
			totalSize: totalSize + 100,
			want:      "smaller than total size",
		},
		{
			name:      "wrong mode",
			shards:    shards,
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "cannot decode mode",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := engine.Decode(tc.shards, tc.mode, tc.totalSize)
			if err == nil {
				t.Fatalf("Decode() = %q, want error", decoded)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Decode() error = %v, want %q", err, tc.want)
			}
			if decoded != nil {
				t.Fatalf("Decode() returned payload %q on error", decoded)
			}
		})
	}
}

func TestRS32EncodeRejectsWrongMode(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	if _, err := engine.Encode([]byte("value"), redundancy.ModeReplication); err == nil {
		t.Fatal("Encode(replication) on RS32Engine returned nil error")
	}
	if _, err := engine.Encode([]byte("value"), redundancy.Mode("triple")); err == nil {
		t.Fatal("Encode(invalid) on RS32Engine returned nil error")
	}
}
