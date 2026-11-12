package consensus_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

func placements(mode string) []storage.ShardPlacement {
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	out := make([]storage.ShardPlacement, 0, redundancy.TotalShards)
	for i, node := range nodes {
		out = append(out, storage.ShardPlacement{
			ShardIndex: i,
			NodeID:     node,
			IsParity:   mode == string(redundancy.ModeRS32) && i >= redundancy.RSDataShards,
		})
	}
	return out
}

func checksum(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func meta(key, stripe, mode string, version int64, value []byte) storage.KeyMeta {
	return storage.KeyMeta{
		Key:       key,
		StripeID:  stripe,
		Mode:      mode,
		Size:      int64(len(value)),
		Checksum:  checksum(value),
		Version:   version,
		Placement: placements(mode),
	}
}

func TestCommandValidation(t *testing.T) {
	value := []byte("metadata-only")
	valid := meta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, value)

	invalid := map[string]consensus.Command{
		"unknown op":            {Op: "unknown"},
		"missing metadata":      {Op: consensus.OpPutMeta, Key: "alpha"},
		"mismatched key":        {Op: consensus.OpPutMeta, Key: "beta", Meta: &valid},
		"missing stripe":        putMeta(func(m *storage.KeyMeta) { m.StripeID = "" }, valid),
		"invalid mode":          putMeta(func(m *storage.KeyMeta) { m.Mode = "three-copies" }, valid),
		"zero version":          putMeta(func(m *storage.KeyMeta) { m.Version = 0 }, valid),
		"negative size":         putMeta(func(m *storage.KeyMeta) { m.Size = -1 }, valid),
		"short checksum":        putMeta(func(m *storage.KeyMeta) { m.Checksum = "abcd" }, valid),
		"non-hex checksum":      putMeta(func(m *storage.KeyMeta) { m.Checksum = strings.Repeat("z", 64) }, valid),
		"four placements":       putMeta(func(m *storage.KeyMeta) { m.Placement = m.Placement[:4] }, valid),
		"duplicate shard index": putMeta(func(m *storage.KeyMeta) { m.Placement[1].ShardIndex = 0 }, valid),
		"shard out of range":    putMeta(func(m *storage.KeyMeta) { m.Placement[1].ShardIndex = 5 }, valid),
		"duplicate node":        putMeta(func(m *storage.KeyMeta) { m.Placement[1].NodeID = m.Placement[0].NodeID }, valid),
		"wrong parity":          putMeta(func(m *storage.KeyMeta) { m.Placement[3].IsParity = false }, valid),
		"set_meta_status":       {Op: consensus.OpSetMetaStatus, Key: "alpha", StripeID: "stripe-1"},
		"get_meta missing key":  {Op: consensus.OpGetMeta},
		"delete_meta negative":  {Op: consensus.OpDeleteMeta, Key: "alpha", Version: -1},
		"set_config empty mode": {Op: consensus.OpSetConfig, Config: &storage.ClusterConfig{}},
		"set_config bad mode":   {Op: consensus.OpSetConfig, Config: &storage.ClusterConfig{DefaultMode: "triple"}},
	}

	validCommands := map[string]consensus.Command{
		"put_meta":              {Op: consensus.OpPutMeta, Meta: &valid},
		"set_meta_status":       {Op: consensus.OpSetMetaStatus, Key: "alpha", StripeID: "stripe-1", Version: 1},
		"get_meta":              {Op: consensus.OpGetMeta, Key: "alpha"},
		"delete_meta":           {Op: consensus.OpDeleteMeta, Key: "alpha", Version: 1},
		"delete_meta unguarded": {Op: consensus.OpDeleteMeta, Key: "alpha"},
		"set_config":            {Op: consensus.OpSetConfig, Config: &storage.ClusterConfig{DefaultMode: string(redundancy.ModeRS32)}},
	}

	for name, cmd := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := cmd.Validate(); err == nil {
				t.Fatal("Validate accepted an invalid command")
			}
		})
	}
	for name, cmd := range validCommands {
		t.Run(name, func(t *testing.T) {
			if err := cmd.Validate(); err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
		})
	}
}

func putMeta(mutate func(*storage.KeyMeta), valid storage.KeyMeta) consensus.Command {
	clone := valid
	clone.Placement = append([]storage.ShardPlacement(nil), valid.Placement...)
	mutate(&clone)
	return consensus.Command{Op: consensus.OpPutMeta, Meta: &clone}
}

func TestCommandJSONRoundTrip(t *testing.T) {
	value := []byte("value")
	expected := meta("alpha", "stripe-1", string(redundancy.ModeRS32), 2, value)
	cmd := consensus.Command{Op: consensus.OpPutMeta, Meta: &expected}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded consensus.Command
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Op != cmd.Op || decoded.Meta == nil {
		t.Fatalf("decoded = %+v", decoded)
	}
	if decoded.Meta.StripeID != expected.StripeID || decoded.Meta.Checksum != expected.Checksum ||
		decoded.Meta.Version != expected.Version || len(decoded.Meta.Placement) != redundancy.TotalShards {
		t.Fatalf("decoded metadata = %+v, want %+v", decoded.Meta, expected)
	}
}

func TestMetadataCommandSizeGuard(t *testing.T) {
	value := []byte("small-value")
	cmd := putMeta(func(m *storage.KeyMeta) {
		m.Placement[0].NodeID = strings.Repeat("n", 64*1024+1)
	}, meta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, value))

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) <= 64*1024 {
		t.Fatalf("test command is only %d bytes", len(data))
	}
	if err := cmd.Validate(); err == nil {
		t.Fatal("oversized metadata command was accepted")
	}
}

func TestMetadataCommandHasNoPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("payload-bytes-"), 1<<16) // ~1 MiB
	cmd := consensus.Command{
		Op:   consensus.OpPutMeta,
		Meta: ptr(meta("large", "stripe-large", string(redundancy.ModeRS32), 1, payload)),
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) > 64*1024 {
		t.Fatalf("metadata command is %d bytes", len(data))
	}
	if bytes.Contains(data, []byte("payload-bytes-")) {
		t.Fatal("metadata command contains payload bytes")
	}
	if !bytes.Contains(data, []byte(cmd.Meta.Checksum)) {
		t.Fatal("metadata command is missing the checksum")
	}
}

func TestCommandHasNoTopLevelValueField(t *testing.T) {
	commandType := reflect.TypeOf(consensus.Command{})
	if _, ok := commandType.FieldByName("Value"); ok {
		t.Fatal("Command must not expose a top-level Value field")
	}
	if _, ok := commandType.FieldByName("Legacy"); !ok {
		t.Fatal("Command must expose the transitional Legacy field")
	}
	legacyType := reflect.TypeOf(consensus.LegacyOp{})
	if _, ok := legacyType.FieldByName("Value"); !ok {
		t.Fatal("LegacyOp must carry the transitional payload")
	}
}

func ptr(meta storage.KeyMeta) *storage.KeyMeta {
	return &meta
}
