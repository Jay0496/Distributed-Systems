package redundancy_test

import (
	"bytes"
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

func TestReplicationEncodeReturnsFiveFullCopies(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	value := []byte("replicated value")
	shards, err := engine.Encode(value, redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(shards) != redundancy.TotalShards {
		t.Fatalf("Encode returned %d copies, want %d", len(shards), redundancy.TotalShards)
	}

	for i, shard := range shards {
		if shard.Index != i {
			t.Errorf("copy %d has index %d", i, shard.Index)
		}
		if shard.IsParity {
			t.Errorf("copy %d is marked as parity", i)
		}
		if !bytes.Equal(shard.Data, value) {
			t.Errorf("copy %d differs from the original value", i)
		}
		if shard.TotalSize != int64(len(value)) {
			t.Errorf("copy %d TotalSize = %d, want %d", i, shard.TotalSize, len(value))
		}
		if err := shard.Verify(); err != nil {
			t.Errorf("copy %d failed verification: %v", i, err)
		}
	}
}

func TestReplicationRoundTrip(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	values := [][]byte{
		nil,
		{},
		[]byte("x"),
		[]byte("hello world"),
		bytes.Repeat([]byte("0123456789abcdef"), 1<<16), // 1 MiB
	}

	for _, value := range values {
		shards, err := engine.Encode(value, redundancy.ModeReplication)
		if err != nil {
			t.Fatalf("Encode(%d bytes): %v", len(value), err)
		}
		decoded, err := engine.Decode(shards, redundancy.ModeReplication, int64(len(value)))
		if err != nil {
			t.Fatalf("Decode(%d bytes): %v", len(value), err)
		}
		if !bytes.Equal(decoded, value) {
			t.Fatalf("round trip for %d bytes changed the value", len(value))
		}
	}
}

func TestReplicationDecodeFromEverySingleCopy(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	value := []byte("survives four lost copies")
	shards, err := engine.Encode(value, redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	for i, shard := range shards {
		decoded, err := engine.Decode([]redundancy.Shard{shard}, redundancy.ModeReplication, int64(len(value)))
		if err != nil {
			t.Fatalf("Decode(copy %d): %v", i, err)
		}
		if !bytes.Equal(decoded, value) {
			t.Fatalf("Decode(copy %d) = %q, want %q", i, decoded, value)
		}
	}
}

func TestReplicationSkipsCorruptCopiesWhenAValidCopyExists(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	value := []byte("prefer the good copy")
	shards, err := engine.Encode(value, redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	corruptData := append([]redundancy.Shard(nil), shards...)
	corruptData[0].Data = append([]byte(nil), corruptData[0].Data...)
	corruptData[0].Data[0] ^= 0xFF

	corruptChecksum := append([]redundancy.Shard(nil), shards...)
	corruptChecksum[0].Checksum = redundancy.Checksum([]byte("different"))

	for name, input := range map[string][]redundancy.Shard{
		"corrupt data":     corruptData,
		"corrupt checksum": corruptChecksum,
	} {
		t.Run(name, func(t *testing.T) {
			decoded, err := engine.Decode(input, redundancy.ModeReplication, int64(len(value)))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !bytes.Equal(decoded, value) {
				t.Fatalf("Decode = %q, want %q", decoded, value)
			}
		})
	}
}

func TestReplicationDecodeRejectsInvalidInput(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	value := []byte("hello")
	shards, err := engine.Encode(value, redundancy.ModeReplication)
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
	withTotalSize := func(size int64) []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		out[0].TotalSize = size
		return out
	}
	truncated := func() []redundancy.Shard {
		out := append([]redundancy.Shard(nil), shards...)
		out[0].Data = append([]byte(nil), out[0].Data[:len(out[0].Data)-1]...)
		out[0].Checksum = redundancy.Checksum(out[0].Data)
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
			name:      "no copies",
			shards:    nil,
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "no replication copies",
		},
		{
			name:      "duplicate index",
			shards:    []redundancy.Shard{shards[0], shards[0]},
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "duplicate shard index",
		},
		{
			name:      "index out of range",
			shards:    withIndex(redundancy.TotalShards),
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "out of range",
		},
		{
			name:      "corrupt data only",
			shards:    corrupt(0)[:1],
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "no valid replication copy",
		},
		{
			name:      "payload length mismatch",
			shards:    truncated()[:1],
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "want 5",
		},
		{
			name:      "declared total size mismatch",
			shards:    withTotalSize(totalSize + 1)[:1],
			mode:      redundancy.ModeReplication,
			totalSize: totalSize,
			want:      "declares total size",
		},
		{
			name:      "negative total size",
			shards:    shards[:1],
			mode:      redundancy.ModeReplication,
			totalSize: -1,
			want:      "negative total size",
		},
		{
			name:      "wrong mode",
			shards:    shards,
			mode:      redundancy.ModeRS32,
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

func TestReplicationEmptyModeUsesDefault(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	value := []byte("default mode")
	shards, err := engine.Encode(value, redundancy.Mode(""))
	if err != nil {
		t.Fatalf("Encode(default): %v", err)
	}
	decoded, err := engine.Decode(shards, redundancy.Mode(""), int64(len(value)))
	if err != nil {
		t.Fatalf("Decode(default): %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Fatalf("Decode(default) = %q, want %q", decoded, value)
	}
}

func TestReplicationEncodeRejectsWrongMode(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	if _, err := engine.Encode([]byte("value"), redundancy.ModeRS32); err == nil {
		t.Fatal("Encode(rs32) on ReplicationEngine returned nil error")
	}
	if _, err := engine.Encode([]byte("value"), redundancy.Mode("triple")); err == nil {
		t.Fatal("Encode(invalid) on ReplicationEngine returned nil error")
	}
}
