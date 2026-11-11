package redundancy_test

import (
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

func TestChecksumKnownVectors(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "empty",
			data: nil,
			want: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "hello world",
			data: []byte("hello world"),
			want: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redundancy.Checksum(tc.data); got != tc.want {
				t.Fatalf("Checksum(%q) = %s, want %s", tc.data, got, tc.want)
			}
		})
	}
}

func TestChecksumDistinguishesPayloads(t *testing.T) {
	if redundancy.Checksum([]byte("a")) == redundancy.Checksum([]byte("b")) {
		t.Fatal("different payloads produced the same checksum")
	}
}

func TestVerifyChecksum(t *testing.T) {
	data := []byte("payload")
	good := redundancy.Checksum(data)

	if err := redundancy.VerifyChecksum(data, good); err != nil {
		t.Fatalf("VerifyChecksum(valid) returned error: %v", err)
	}
	if err := redundancy.VerifyChecksum(data, redundancy.Checksum([]byte("other"))); err == nil {
		t.Fatal("VerifyChecksum(mismatch) returned nil")
	} else if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("VerifyChecksum(mismatch) error = %v, want checksum mismatch", err)
	}
	if err := redundancy.VerifyChecksum(data, ""); err == nil {
		t.Fatal("VerifyChecksum(missing checksum) returned nil")
	}
}

func TestShardVerify(t *testing.T) {
	data := []byte("shard-data")
	valid := redundancy.Shard{
		Index:     2,
		Data:      data,
		Checksum:  redundancy.Checksum(data),
		TotalSize: int64(len(data)),
	}

	cases := []struct {
		name  string
		shard redundancy.Shard
		want  string
	}{
		{name: "valid", shard: valid},
		{name: "negative index", shard: redundancy.Shard{Index: -1, Data: data, Checksum: redundancy.Checksum(data)}, want: "out of range"},
		{name: "index too large", shard: redundancy.Shard{Index: 5, Data: data, Checksum: redundancy.Checksum(data)}, want: "out of range"},
		{name: "negative total size", shard: redundancy.Shard{Index: 0, Data: data, Checksum: redundancy.Checksum(data), TotalSize: -1}, want: "negative total size"},
		{name: "checksum mismatch", shard: redundancy.Shard{Index: 0, Data: data, Checksum: redundancy.Checksum([]byte("other"))}, want: "checksum mismatch"},
		{name: "missing checksum", shard: redundancy.Shard{Index: 0, Data: data}, want: "missing checksum"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.shard.Verify()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Verify() returned error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Verify() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateShards(t *testing.T) {
	shard := func(index int, data string, totalSize int64) redundancy.Shard {
		return redundancy.Shard{
			Index:     index,
			Data:      []byte(data),
			Checksum:  redundancy.Checksum([]byte(data)),
			TotalSize: totalSize,
		}
	}

	cases := []struct {
		name      string
		shards    []redundancy.Shard
		mode      redundancy.Mode
		totalSize int64
		want      string
	}{
		{
			name:      "valid",
			shards:    []redundancy.Shard{shard(0, "abc", 9), shard(1, "def", 9), shard(2, "ghi", 9)},
			mode:      redundancy.ModeRS32,
			totalSize: 9,
		},
		{
			name:      "zero total size treated as unspecified",
			shards:    []redundancy.Shard{shard(0, "abc", 0), shard(1, "def", 0)},
			mode:      redundancy.ModeRS32,
			totalSize: 9,
		},
		{
			name:      "duplicate index",
			shards:    []redundancy.Shard{shard(0, "abc", 9), shard(0, "def", 9)},
			mode:      redundancy.ModeRS32,
			totalSize: 9,
			want:      "duplicate shard index 0",
		},
		{
			name:      "invalid mode",
			shards:    []redundancy.Shard{shard(0, "abc", 9)},
			mode:      "triple",
			totalSize: 9,
			want:      "invalid redundancy mode",
		},
		{
			name:      "negative total size",
			shards:    []redundancy.Shard{shard(0, "abc", 9)},
			mode:      redundancy.ModeRS32,
			totalSize: -1,
			want:      "negative total size",
		},
		{
			name:      "no shards",
			shards:    nil,
			mode:      redundancy.ModeRS32,
			totalSize: 9,
			want:      "no shards",
		},
		{
			name:      "inconsistent total size",
			shards:    []redundancy.Shard{shard(0, "abc", 9), shard(1, "def", 8)},
			mode:      redundancy.ModeRS32,
			totalSize: 9,
			want:      "does not match expected",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := redundancy.ValidateShards(tc.shards, tc.mode, tc.totalSize)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("ValidateShards() returned error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateShards() error = %v, want %q", err, tc.want)
			}
		})
	}
}
