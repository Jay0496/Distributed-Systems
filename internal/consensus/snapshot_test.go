package consensus

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

func persistFSMSnapshot(t *testing.T, fsm *FSM) []byte {
	t.Helper()

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	return append([]byte(nil), sink.buf.Bytes()...)
}

func TestFSMSnapshotVersion2Metadata(t *testing.T) {
	sourceFSM, _ := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 3, []byte("value"))
	requireResult(t, applyCommandAt(t, sourceFSM, 1, Command{Op: OpPutMeta, Meta: &meta}))
	requireResult(t, applyCommandAt(t, sourceFSM, 2, Command{
		Op:     OpSetConfig,
		Config: &storage.ClusterConfig{DefaultMode: string(redundancy.ModeReplication)},
	}))

	data := persistFSMSnapshot(t, sourceFSM)

	var envelope fsmSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.SchemaVersion != fsmSnapshotSchemaVersion {
		t.Fatalf("schema version = %d, want %d", envelope.SchemaVersion, fsmSnapshotSchemaVersion)
	}
	if len(envelope.Metadata) == 0 {
		t.Fatal("version 2 snapshot has no metadata section")
	}
	if len(envelope.Legacy) == 0 {
		t.Fatal("transitional version 2 snapshot has no legacy section")
	}

	var payload struct {
		Keys             []storage.KeyMeta      `json:"keys"`
		Config           *storage.ClusterConfig `json:"config"`
		LastAppliedIndex uint64                 `json:"last_applied_index"`
	}
	if err := json.Unmarshal(envelope.Metadata, &payload); err != nil {
		t.Fatalf("decode metadata payload: %v", err)
	}
	if payload.LastAppliedIndex != 2 {
		t.Fatalf("snapshot applied index = %d, want 2", payload.LastAppliedIndex)
	}
	if len(payload.Keys) != 1 || payload.Keys[0].StripeID != "stripe-1" {
		t.Fatalf("snapshot keys = %+v", payload.Keys)
	}
	if payload.Config == nil || payload.Config.DefaultMode != string(redundancy.ModeReplication) {
		t.Fatalf("snapshot config = %+v", payload.Config)
	}

	targetFSM, targetStore := newSQLiteFSM(t)
	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	restored, err := targetStore.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if restored.StripeID != "stripe-1" || restored.Version != 3 || restored.Checksum != meta.Checksum {
		t.Fatalf("restored metadata = %+v, want %+v", restored, meta)
	}
	cfg, err := targetStore.GetConfig()
	if err != nil || cfg.DefaultMode != string(redundancy.ModeReplication) {
		t.Fatalf("restored config = %+v, %v", cfg, err)
	}
	applied, err := targetStore.LastAppliedIndex()
	if err != nil || applied != 2 {
		t.Fatalf("restored applied index = %d, %v", applied, err)
	}
	stats, err := targetStore.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.FragmentCount != 0 {
		t.Fatalf("snapshot restored fragments: %+v", stats)
	}
}

func TestFSMSnapshotEmptyState(t *testing.T) {
	sourceFSM, _ := newSQLiteFSM(t)
	data := persistFSMSnapshot(t, sourceFSM)

	targetFSM, targetStore := newSQLiteFSM(t)
	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	keys, err := targetStore.ListKeys()
	if err != nil || len(keys) != 0 {
		t.Fatalf("ListKeys = %+v, %v", keys, err)
	}
	applied, err := targetStore.LastAppliedIndex()
	if err != nil || applied != 0 {
		t.Fatalf("applied index = %d, %v", applied, err)
	}
}

func TestFSMSnapshotVersion1LegacyCompatibility(t *testing.T) {
	_, sourceStore := newSQLiteFSM(t)
	if err := sourceStore.Put("legacy", []byte("value"), false); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v1, err := sourceStore.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	targetFSM, targetStore := newSQLiteFSM(t)
	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(v1))); err != nil {
		t.Fatalf("restore version 1 legacy: %v", err)
	}
	value, err := targetStore.Get("legacy")
	if err != nil || string(value) != "value" {
		t.Fatalf("Get(legacy) = %q, %v", value, err)
	}
}

func TestFSMSnapshotVersion1MetadataCompatibility(t *testing.T) {
	_, sourceStore := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))
	if _, err := sourceStore.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}
	v1, err := sourceStore.SnapshotMetadata()
	if err != nil {
		t.Fatalf("SnapshotMetadata: %v", err)
	}

	targetFSM, targetStore := newSQLiteFSM(t)
	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(v1))); err != nil {
		t.Fatalf("restore version 1 metadata: %v", err)
	}
	restored, err := targetStore.GetKey("alpha")
	if err != nil || restored.StripeID != "stripe-1" {
		t.Fatalf("GetKey = %+v, %v", restored, err)
	}
}

func TestFSMSnapshotRejectsUnknownVersion(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)
	unknown := []byte(`{"schema_version":99,"metadata":{},"legacy":{}}`)
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(unknown))); err == nil {
		t.Fatal("unknown snapshot version was accepted")
	}
}

func TestFSMSnapshotRejectsCorruption(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)
	if err := fsm.Restore(io.NopCloser(strings.NewReader("{not-json"))); err == nil {
		t.Fatal("corrupt snapshot was accepted")
	}
}

func TestFSMSnapshotRestoreReplacesState(t *testing.T) {
	sourceFSM, sourceStore := newSQLiteFSM(t)
	if err := sourceStore.Put("kept", []byte("value"), false); err != nil {
		t.Fatalf("Put: %v", err)
	}
	data := persistFSMSnapshot(t, sourceFSM)

	targetFSM, targetStore := newSQLiteFSM(t)
	if err := targetStore.Put("stale", []byte("old"), false); err != nil {
		t.Fatalf("Put stale: %v", err)
	}
	staleMeta := commandTestMeta("stale-meta", "stripe-stale", string(redundancy.ModeRS32), 1, []byte("old"))
	if _, err := targetStore.ApplyPutMeta(1, staleMeta); err != nil {
		t.Fatalf("ApplyPutMeta stale: %v", err)
	}

	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := targetStore.Get("stale"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stale legacy key survived restore: %v", err)
	}
	if _, err := targetStore.GetKey("stale-meta"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stale metadata survived restore: %v", err)
	}
	value, err := targetStore.Get("kept")
	if err != nil || string(value) != "value" {
		t.Fatalf("Get(kept) = %q, %v", value, err)
	}
}

func TestFSMSnapshotRestoreIsAtomic(t *testing.T) {
	_, sourceStore := newSQLiteFSM(t)
	goodMeta := commandTestMeta("new", "stripe-new", string(redundancy.ModeRS32), 1, []byte("value"))
	if _, err := sourceStore.ApplyPutMeta(1, goodMeta); err != nil {
		t.Fatalf("ApplyPutMeta new: %v", err)
	}
	validMetadata, err := sourceStore.SnapshotMetadataPayload()
	if err != nil {
		t.Fatalf("SnapshotMetadataPayload: %v", err)
	}

	targetFSM, targetStore := newSQLiteFSM(t)
	keepMeta := commandTestMeta("keep", "stripe-keep", string(redundancy.ModeRS32), 1, []byte("keep"))
	if _, err := targetStore.ApplyPutMeta(3, keepMeta); err != nil {
		t.Fatalf("ApplyPutMeta keep: %v", err)
	}
	if err := targetStore.Put("keep-legacy", []byte("keep"), false); err != nil {
		t.Fatalf("Put keep-legacy: %v", err)
	}

	// The metadata section references a stripe that does not exist, so the
	// foreign key fails after the legacy section has been written in the same
	// transaction. Both sections must roll back.
	badMetadata := []byte(`{"stripes":[],"keys":[{"key":"broken","stripe_id":"missing",` +
		`"mode":"rs32","size":1,"checksum":"` + strings.Repeat("a", 64) + `","version":1,` +
		`"degraded":false,"placement":[]}],"last_applied_index":9}`)
	legacyValue := base64.StdEncoding.EncodeToString([]byte("new"))
	legacy := []byte(`[{"key":"new-legacy","value":"` + legacyValue + `"}]`)

	data, err := json.Marshal(fsmSnapshotEnvelope{
		SchemaVersion: fsmSnapshotSchemaVersion,
		Metadata:      badMetadata,
		Legacy:        legacy,
	})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err == nil {
		t.Fatal("invalid snapshot was accepted")
	}

	// The previously valid metadata payload is unused here, but referencing it
	// keeps the fixture honest about the payload shape used by the FSM.
	if len(validMetadata) == 0 {
		t.Fatal("source metadata payload is empty")
	}
	if _, err := targetStore.GetKey("keep"); err != nil {
		t.Fatalf("metadata was not rolled back: %v", err)
	}
	if _, err := targetStore.Get("new-legacy"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("legacy section was not rolled back: %v", err)
	}
	value, err := targetStore.Get("keep-legacy")
	if err != nil || string(value) != "keep" {
		t.Fatalf("legacy state changed after failed restore: %q, %v", value, err)
	}
	applied, err := targetStore.LastAppliedIndex()
	if err != nil || applied != 3 {
		t.Fatalf("applied index changed after failed restore: %d, %v", applied, err)
	}
}

func TestFSMSnapshotNonSnapshotter(t *testing.T) {
	fsm := NewFSM(storeWithoutSnapshot{})
	data := persistFSMSnapshot(t, fsm)
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore on non-snapshot store: %v", err)
	}
}
