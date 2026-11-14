package consensus

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

func commandTestPlacements(mode string) []storage.ShardPlacement {
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	placements := make([]storage.ShardPlacement, 0, redundancy.TotalShards)
	for i, node := range nodes {
		placements = append(placements, storage.ShardPlacement{
			ShardIndex: i,
			NodeID:     node,
			IsParity:   mode == string(redundancy.ModeRS32) && i >= redundancy.RSDataShards,
		})
	}
	return placements
}

func commandTestValue(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func commandTestMeta(key, stripe, mode string, version int64, value []byte) storage.KeyMeta {
	return storage.KeyMeta{
		Key:       key,
		StripeID:  stripe,
		Mode:      mode,
		Size:      int64(len(value)),
		Checksum:  commandTestValue(value),
		Version:   version,
		Placement: commandTestPlacements(mode),
	}
}

func TestCommandValidation(t *testing.T) {
	value := []byte("metadata-only")
	valid := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, value)

	tests := []struct {
		name string
		cmd  Command
	}{
		{name: "valid put_meta rs32", cmd: Command{Op: OpPutMeta, Meta: &valid}},
		{name: "valid put_meta replication", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeReplication), 1, value)
			return &meta
		}()}},
		{name: "valid set_meta_status", cmd: Command{Op: OpSetMetaStatus, Key: "alpha", StripeID: "stripe-1", Version: 1, Degraded: false}},
		{name: "valid get_meta", cmd: Command{Op: OpGetMeta, Key: "alpha"}},
		{name: "valid delete_meta", cmd: Command{Op: OpDeleteMeta, Key: "alpha", Version: 1}},
		{name: "valid delete_meta unconditional", cmd: Command{Op: OpDeleteMeta, Key: "alpha"}},
		{name: "valid set_config", cmd: Command{Op: OpSetConfig, Config: &storage.ClusterConfig{DefaultMode: string(redundancy.ModeRS32)}}},
		{name: "valid get_config", cmd: Command{Op: OpGetConfig}},

		{name: "unknown op", cmd: Command{Op: "unknown"}},
		{name: "put_meta missing metadata", cmd: Command{Op: OpPutMeta, Key: "alpha"}},
		{name: "put_meta mismatched key", cmd: Command{Op: OpPutMeta, Key: "beta", Meta: &valid}},
		{name: "put_meta missing key", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Key = ""
			return &meta
		}()}},
		{name: "put_meta missing stripe", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.StripeID = ""
			return &meta
		}()}},
		{name: "put_meta empty mode", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Mode = ""
			return &meta
		}()}},
		{name: "put_meta invalid mode", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Mode = "three-copies"
			return &meta
		}()}},
		{name: "put_meta negative size", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Size = -1
			return &meta
		}()}},
		{name: "put_meta short checksum", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Checksum = "abcd"
			return &meta
		}()}},
		{name: "put_meta non-hex checksum", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Checksum = strings.Repeat("z", 64)
			return &meta
		}()}},
		{name: "put_meta zero version", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Version = 0
			return &meta
		}()}},
		{name: "put_meta too few placements", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Placement = meta.Placement[:4]
			return &meta
		}()}},
		{name: "put_meta duplicate shard index", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Placement = append([]storage.ShardPlacement(nil), valid.Placement...)
			meta.Placement[1].ShardIndex = 0
			return &meta
		}()}},
		{name: "put_meta shard index out of range", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Placement = append([]storage.ShardPlacement(nil), valid.Placement...)
			meta.Placement[1].ShardIndex = 5
			return &meta
		}()}},
		{name: "put_meta duplicate node", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Placement = append([]storage.ShardPlacement(nil), valid.Placement...)
			meta.Placement[1].NodeID = meta.Placement[0].NodeID
			return &meta
		}()}},
		{name: "put_meta empty node", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Placement = append([]storage.ShardPlacement(nil), valid.Placement...)
			meta.Placement[1].NodeID = ""
			return &meta
		}()}},
		{name: "put_meta wrong parity", cmd: Command{Op: OpPutMeta, Meta: func() *storage.KeyMeta {
			meta := valid
			meta.Placement = append([]storage.ShardPlacement(nil), valid.Placement...)
			meta.Placement[3].IsParity = false
			return &meta
		}()}},
		{name: "set_meta_status missing key", cmd: Command{Op: OpSetMetaStatus, StripeID: "stripe-1", Version: 1}},
		{name: "set_meta_status missing stripe", cmd: Command{Op: OpSetMetaStatus, Key: "alpha", Version: 1}},
		{name: "set_meta_status zero version", cmd: Command{Op: OpSetMetaStatus, Key: "alpha", StripeID: "stripe-1"}},
		{name: "get_meta missing key", cmd: Command{Op: OpGetMeta}},
		{name: "delete_meta missing key", cmd: Command{Op: OpDeleteMeta}},
		{name: "delete_meta negative version", cmd: Command{Op: OpDeleteMeta, Key: "alpha", Version: -1}},
		{name: "set_config missing config", cmd: Command{Op: OpSetConfig}},
		{name: "set_config empty mode", cmd: Command{Op: OpSetConfig, Config: &storage.ClusterConfig{}}},
		{name: "set_config invalid mode", cmd: Command{Op: OpSetConfig, Config: &storage.ClusterConfig{DefaultMode: "three-copies"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantErr := !strings.HasPrefix(tc.name, "valid ")
			err := tc.cmd.Validate()
			if wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error")
			}
			if !wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestCommandJSONRoundTrip(t *testing.T) {
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 2, []byte("value"))
	cmd := Command{Op: OpPutMeta, Meta: &meta}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Command
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Op != cmd.Op || decoded.Meta == nil || decoded.Meta.Key != meta.Key {
		t.Fatalf("decoded = %+v, want %+v", decoded, cmd)
	}
	if decoded.Meta.StripeID != meta.StripeID || decoded.Meta.Checksum != meta.Checksum ||
		decoded.Meta.Version != meta.Version || len(decoded.Meta.Placement) != redundancy.TotalShards {
		t.Fatalf("decoded metadata = %+v, want %+v", decoded.Meta, meta)
	}
}

func TestMetadataCommandSizeGuard(t *testing.T) {
	value := []byte("small-value")
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, value)
	// A single oversized placement is enough to exceed the metadata limit.
	meta.Placement[0].NodeID = strings.Repeat("n", metadataMaxBytes+1)
	cmd := Command{Op: OpPutMeta, Meta: &meta}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) <= metadataMaxBytes {
		t.Fatalf("test command is only %d bytes", len(data))
	}
	if err := cmd.Validate(); err == nil {
		t.Fatal("oversized metadata command was accepted")
	}
	if err := validateMetadataSize(cmd, data); err == nil {
		t.Fatal("oversized metadata command passed the raw size guard")
	}
}

func TestMetadataCommandHasNoPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("payload-bytes-"), 1<<16) // ~1 MiB
	meta := commandTestMeta("large", "stripe-large", string(redundancy.ModeRS32), 1, payload)
	cmd := Command{Op: OpPutMeta, Meta: &meta}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) > metadataMaxBytes {
		t.Fatalf("metadata command is %d bytes, limit is %d", len(data), metadataMaxBytes)
	}
	if bytes.Contains(data, []byte("payload-bytes-")) {
		t.Fatal("metadata command contains payload bytes")
	}
	if !bytes.Contains(data, []byte(meta.Checksum)) {
		t.Fatal("metadata command is missing the checksum")
	}
}

func TestCommandHasNoPayloadField(t *testing.T) {
	commandType := reflect.TypeOf(Command{})
	for _, field := range []string{"Value", "Legacy"} {
		if _, ok := commandType.FieldByName(field); ok {
			t.Fatalf("Command must not expose a %s field", field)
		}
	}

	// Every command the FSM accepts is metadata-only and therefore covered by
	// the size guard.
	for _, op := range []string{OpPutMeta, OpSetMetaStatus, OpGetMeta, OpDeleteMeta, OpSetConfig, OpGetConfig} {
		if !IsMetadata(op) {
			t.Fatalf("%q is not classified as a metadata operation", op)
		}
	}
}
