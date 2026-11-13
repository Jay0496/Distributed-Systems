package consensus

import (
	"bytes"
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

func TestFSMSnapshotVersion3MetadataOnly(t *testing.T) {
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
	if envelope.SchemaVersion != 3 {
		t.Fatalf("schema version = %d, want 3", envelope.SchemaVersion)
	}
	if len(envelope.Metadata) == 0 {
		t.Fatal("version 3 snapshot has no metadata section")
	}
	if len(envelope.Legacy) != 0 {
		t.Fatal("version 3 snapshot must not contain a legacy section")
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

func TestFSMSnapshotVersion1EmptyLegacyCompatibility(t *testing.T) {
	targetFSM, targetStore := newSQLiteFSM(t)
	stale := commandTestMeta("stale", "stripe-stale", string(redundancy.ModeRS32), 1, []byte("old"))
	if _, err := targetStore.ApplyPutMeta(1, stale); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}

	empty := []byte(`{"schema_version":1,"kind":"legacy","payload":[]}`)
	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(empty))); err != nil {
		t.Fatalf("restore empty version 1 legacy snapshot: %v", err)
	}
	if _, err := targetStore.GetKey("stale"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty legacy restore did not clear metadata: %v", err)
	}
}

func TestFSMSnapshotVersion1NonEmptyLegacyFails(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)
	legacy := []byte(`{"schema_version":1,"kind":"legacy","payload":[{"key":"legacy","value":"dmFsdWU="}]}`)
	err := fsm.Restore(io.NopCloser(bytes.NewReader(legacy)))
	if err == nil {
		t.Fatal("non-empty version 1 legacy snapshot was accepted")
	}
	if !strings.Contains(err.Error(), "export") {
		t.Fatalf("error = %v, want an export instruction", err)
	}
}

func TestFSMSnapshotVersion2EmptyLegacyRestoresMetadata(t *testing.T) {
	_, sourceStore := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))
	if _, err := sourceStore.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}
	metadata, err := sourceStore.SnapshotMetadataPayload()
	if err != nil {
		t.Fatalf("SnapshotMetadataPayload: %v", err)
	}

	data, err := json.Marshal(fsmSnapshotEnvelope{
		SchemaVersion: 2,
		Metadata:      metadata,
		Legacy:        json.RawMessage(`[]`),
	})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	targetFSM, targetStore := newSQLiteFSM(t)
	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("restore version 2 metadata: %v", err)
	}
	restored, err := targetStore.GetKey("alpha")
	if err != nil || restored.StripeID != "stripe-1" {
		t.Fatalf("GetKey = %+v, %v", restored, err)
	}
}

func TestFSMSnapshotVersion2NonEmptyLegacyFails(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)
	data := []byte(`{"schema_version":2,"metadata":{},"legacy":[{"key":"legacy","value":"dmFsdWU="}]}`)
	err := fsm.Restore(io.NopCloser(bytes.NewReader(data)))
	if err == nil {
		t.Fatal("non-empty version 2 legacy snapshot was accepted")
	}
	if !strings.Contains(err.Error(), "export") {
		t.Fatalf("error = %v, want an export instruction", err)
	}
}

func TestFSMSnapshotRejectsUnknownVersion(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)
	unknown := []byte(`{"schema_version":99,"metadata":{}}`)
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
	sourceFSM, _ := newSQLiteFSM(t)
	keptMeta := commandTestMeta("kept", "stripe-kept", string(redundancy.ModeRS32), 1, []byte("value"))
	requireResult(t, applyCommandAt(t, sourceFSM, 1, Command{Op: OpPutMeta, Meta: &keptMeta}))
	data := persistFSMSnapshot(t, sourceFSM)

	targetFSM, targetStore := newSQLiteFSM(t)
	staleMeta := commandTestMeta("stale-meta", "stripe-stale", string(redundancy.ModeRS32), 1, []byte("old"))
	if _, err := targetStore.ApplyPutMeta(1, staleMeta); err != nil {
		t.Fatalf("ApplyPutMeta stale: %v", err)
	}

	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := targetStore.GetKey("stale-meta"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stale metadata survived restore: %v", err)
	}
	restored, err := targetStore.GetKey("kept")
	if err != nil || restored.StripeID != "stripe-kept" {
		t.Fatalf("GetKey(kept) = %+v, %v", restored, err)
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

	// The metadata section references a stripe that does not exist, so the
	// foreign key fails and the previous state must roll back.
	badMetadata := []byte(`{"stripes":[],"keys":[{"key":"broken","stripe_id":"missing",` +
		`"mode":"rs32","size":1,"checksum":"` + strings.Repeat("a", 64) + `","version":1,` +
		`"degraded":false,"placement":[]}],"last_applied_index":9}`)

	data, err := json.Marshal(fsmSnapshotEnvelope{
		SchemaVersion: fsmSnapshotSchemaVersion,
		Metadata:      badMetadata,
	})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	if err := targetFSM.Restore(io.NopCloser(bytes.NewReader(data))); err == nil {
		t.Fatal("invalid snapshot was accepted")
	}

	if len(validMetadata) == 0 {
		t.Fatal("source metadata payload is empty")
	}
	if _, err := targetStore.GetKey("keep"); err != nil {
		t.Fatalf("metadata was not rolled back: %v", err)
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
