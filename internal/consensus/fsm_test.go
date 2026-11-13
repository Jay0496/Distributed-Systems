package consensus

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	"github.com/hashicorp/raft"
)

// memSink is a raft.SnapshotSink backed by memory for tests.
type memSink struct {
	buf      bytes.Buffer
	closed   bool
	canceled bool
}

func (m *memSink) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *memSink) Close() error                { m.closed = true; return nil }
func (m *memSink) ID() string                  { return "mem-sink" }
func (m *memSink) Cancel() error               { m.canceled = true; return nil }

func applyJSON(t *testing.T, fsm *FSM, cmd Command) interface{} {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return fsm.Apply(&raft.Log{Data: data})
}

func TestFSMBadJSON(t *testing.T) {
	fsm := NewFSM(newTestStore())

	ret := fsm.Apply(&raft.Log{Data: []byte("{not-json")})
	result, ok := ret.(*Result)
	if !ok || result.Err != ResultErrInvalid {
		t.Fatalf("expected invalid result for bad JSON, got %#v", ret)
	}
}

func TestFSMUnknownOp(t *testing.T) {
	fsm := NewFSM(newTestStore())

	ret := applyJSON(t, fsm, Command{Op: "delete", Key: "k"})
	result, ok := ret.(*Result)
	if !ok || result.Err != ResultErrInvalid {
		t.Fatalf("unknown op returned %#v, want invalid result", ret)
	}
}

func TestFSMSnapshotWithNonSnapshotter(t *testing.T) {
	// A store without metadata snapshot support must still produce a valid
	// envelope and accept a restore as a no-op.
	fsm := NewFSM(storeWithoutSnapshot{})

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	sink := &memSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	var envelope fsmSnapshotEnvelope
	if err := json.Unmarshal(sink.buf.Bytes(), &envelope); err != nil {
		t.Fatalf("decode snapshot envelope: %v", err)
	}
	if envelope.SchemaVersion != fsmSnapshotSchemaVersion {
		t.Fatalf("schema version = %d, want %d", envelope.SchemaVersion, fsmSnapshotSchemaVersion)
	}
	if len(envelope.Metadata) != 0 || len(envelope.Legacy) != 0 {
		t.Fatalf("non-snapshot store produced sections: %+v", envelope)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes()))); err != nil {
		t.Fatalf("Restore should ignore non-snapshot stores: %v", err)
	}
}

type storeWithoutSnapshot struct {
	storage.Store
}

func newSQLiteFSM(t *testing.T) (*FSM, *storage.SQLiteStore) {
	t.Helper()

	store, err := storage.NewSQLiteStore(filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewFSM(store), store
}

func applyCommandAt(t *testing.T, fsm *FSM, index uint64, cmd Command) interface{} {
	t.Helper()

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	return fsm.Apply(&raft.Log{Index: index, Data: data})
}

func requireResult(t *testing.T, ret interface{}) *Result {
	t.Helper()

	result, ok := ret.(*Result)
	if !ok {
		t.Fatalf("expected *Result, got %T", ret)
	}
	return result
}

func TestFSMPutAndGetMeta(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))

	put := requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))
	if put.Err != "" || !put.Found || put.Meta == nil || put.Meta.StripeID != meta.StripeID {
		t.Fatalf("put_meta result = %+v", put)
	}
	if put.OldMeta != nil {
		t.Fatalf("new key returned old metadata: %+v", put.OldMeta)
	}

	get := requireResult(t, applyCommandAt(t, fsm, 2, Command{Op: OpGetMeta, Key: "alpha"}))
	if get.Err != "" || !get.Found || get.Meta == nil {
		t.Fatalf("get_meta result = %+v", get)
	}
	if get.Meta.Checksum != meta.Checksum || get.Meta.Version != meta.Version ||
		len(get.Meta.Placement) != redundancy.TotalShards {
		t.Fatalf("get_meta metadata = %+v, want %+v", get.Meta, meta)
	}

	// Reads must not advance the applied index.
	applied, err := store.LastAppliedIndex()
	if err != nil {
		t.Fatalf("LastAppliedIndex: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied index = %d, want 1", applied)
	}
}

func TestFSMGetMetaMissing(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)

	result := requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpGetMeta, Key: "absent"}))
	if result.Err != "" || result.Found || result.Meta != nil {
		t.Fatalf("missing metadata result = %+v", result)
	}
}

func TestFSMPutMetaOverwriteReturnsOldMeta(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	first := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("one"))
	second := commandTestMeta("alpha", "stripe-2", string(redundancy.ModeReplication), 2, []byte("two"))

	requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &first}))

	result := requireResult(t, applyCommandAt(t, fsm, 2, Command{Op: OpPutMeta, Meta: &second}))
	if result.Err != "" || !result.Found || result.Meta.StripeID != "stripe-2" {
		t.Fatalf("overwrite result = %+v", result)
	}
	if result.OldMeta == nil || result.OldMeta.StripeID != "stripe-1" {
		t.Fatalf("old metadata = %+v, want stripe-1", result.OldMeta)
	}

	current, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if current.StripeID != "stripe-2" || current.Version != 2 {
		t.Fatalf("stored metadata = %+v", current)
	}
}

func TestFSMPutMetaEqualVersionIsIdempotent(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))

	requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))
	result := requireResult(t, applyCommandAt(t, fsm, 2, Command{Op: OpPutMeta, Meta: &meta}))
	if result.Err != "" || !result.Found || result.Meta.StripeID != meta.StripeID {
		t.Fatalf("idempotent result = %+v", result)
	}
	if result.OldMeta != nil {
		t.Fatalf("idempotent retry exposed old metadata for GC: %+v", result.OldMeta)
	}

	applied, err := store.LastAppliedIndex()
	if err != nil {
		t.Fatalf("LastAppliedIndex: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied index = %d, want 2", applied)
	}
}

func TestFSMPutMetaVersionConflict(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	newer := commandTestMeta("alpha", "stripe-2", string(redundancy.ModeRS32), 2, []byte("new"))
	older := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("old"))

	requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &newer}))
	result := requireResult(t, applyCommandAt(t, fsm, 2, Command{Op: OpPutMeta, Meta: &older}))
	if result.Err != ResultErrConflict {
		t.Fatalf("stale write result = %+v, want conflict", result)
	}

	current, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if current.Version != 2 || current.StripeID != "stripe-2" {
		t.Fatalf("stale write changed metadata: %+v", current)
	}
}

func TestFSMSetMetaStatusPreservesVersion(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 4, []byte("value"))
	meta.Degraded = true

	requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))
	result := requireResult(t, applyCommandAt(t, fsm, 2, Command{
		Op:       OpSetMetaStatus,
		Key:      "alpha",
		StripeID: "stripe-1",
		Version:  4,
		Degraded: false,
	}))
	if result.Err != "" || result.Meta == nil {
		t.Fatalf("set_meta_status result = %+v", result)
	}
	if result.Meta.Version != 4 || result.Meta.Degraded || result.Meta.Checksum != meta.Checksum {
		t.Fatalf("set_meta_status changed visible metadata: %+v", result.Meta)
	}

	current, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if current.Version != 4 || current.Degraded {
		t.Fatalf("stored metadata = %+v", current)
	}
}

func TestFSMSetMetaStatusMissingKey(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)

	result := requireResult(t, applyCommandAt(t, fsm, 1, Command{
		Op:       OpSetMetaStatus,
		Key:      "absent",
		StripeID: "stripe-1",
		Version:  1,
	}))
	if result.Err != ResultErrNotFound {
		t.Fatalf("set_meta_status missing key = %+v, want not_found", result)
	}
}

func TestFSMOlderIndexReplayIsNoOp(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	first := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("one"))
	second := commandTestMeta("alpha", "stripe-2", string(redundancy.ModeRS32), 2, []byte("two"))

	requireResult(t, applyCommandAt(t, fsm, 5, Command{Op: OpPutMeta, Meta: &first}))
	result := requireResult(t, applyCommandAt(t, fsm, 4, Command{Op: OpPutMeta, Meta: &second}))
	if result.Err != "" || result.Meta == nil || result.Meta.StripeID != "stripe-1" {
		t.Fatalf("replay result = %+v, want current stripe-1", result)
	}
	if result.OldMeta != nil {
		t.Fatalf("replay exposed old metadata for GC: %+v", result.OldMeta)
	}

	applied, err := store.LastAppliedIndex()
	if err != nil {
		t.Fatalf("LastAppliedIndex: %v", err)
	}
	if applied != 5 {
		t.Fatalf("applied index = %d, want 5", applied)
	}
	current, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if current.StripeID != "stripe-1" || current.Version != 1 {
		t.Fatalf("replay mutated metadata: %+v", current)
	}
}

func TestFSMDeleteMeta(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))
	requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))

	result := requireResult(t, applyCommandAt(t, fsm, 2, Command{Op: OpDeleteMeta, Key: "alpha", Version: 1}))
	if result.Err != "" || !result.Found || result.OldMeta == nil || result.OldMeta.StripeID != "stripe-1" {
		t.Fatalf("delete_meta result = %+v", result)
	}

	if _, err := store.GetKey("alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted key still present: %v", err)
	}
}

func TestFSMDeleteMetaVersionMismatch(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 2, []byte("value"))
	requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))

	result := requireResult(t, applyCommandAt(t, fsm, 2, Command{Op: OpDeleteMeta, Key: "alpha", Version: 1}))
	if result.Err != ResultErrConflict {
		t.Fatalf("delete version mismatch = %+v, want conflict", result)
	}
	if _, err := store.GetKey("alpha"); err != nil {
		t.Fatalf("key was deleted despite conflict: %v", err)
	}
}

func TestFSMDeleteMissingMetaSucceeds(t *testing.T) {
	fsm, _ := newSQLiteFSM(t)

	result := requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpDeleteMeta, Key: "absent"}))
	if result.Err != "" || result.Found {
		t.Fatalf("missing delete result = %+v", result)
	}
}

func TestFSMConfigDefaultAndSet(t *testing.T) {
	fsm, store := newSQLiteFSM(t)

	def := requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpGetConfig}))
	if def.Err != "" || def.Config == nil || def.Config.DefaultMode != string(redundancy.ModeRS32) {
		t.Fatalf("default config = %+v", def)
	}
	if def.Found {
		t.Fatal("default config reported as stored")
	}

	set := requireResult(t, applyCommandAt(t, fsm, 2, Command{
		Op:     OpSetConfig,
		Config: &storage.ClusterConfig{DefaultMode: string(redundancy.ModeReplication)},
	}))
	if set.Err != "" || !set.Found || set.Config.DefaultMode != string(redundancy.ModeReplication) {
		t.Fatalf("set_config result = %+v", set)
	}

	got := requireResult(t, applyCommandAt(t, fsm, 3, Command{Op: OpGetConfig}))
	if got.Err != "" || !got.Found || got.Config.DefaultMode != string(redundancy.ModeReplication) {
		t.Fatalf("get_config result = %+v", got)
	}

	applied, err := store.LastAppliedIndex()
	if err != nil {
		t.Fatalf("LastAppliedIndex: %v", err)
	}
	if applied != 2 {
		t.Fatalf("applied index = %d, want 2 (reads must not advance)", applied)
	}
}

func TestFSMInvalidMetadataMode(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", "bogus", 1, []byte("value"))

	result := requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))
	if result.Err != ResultErrInvalid {
		t.Fatalf("invalid mode result = %+v, want invalid", result)
	}
	if _, err := store.GetKey("alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("invalid command mutated the store: %v", err)
	}
}

func TestFSMMetadataCommandSizeGuard(t *testing.T) {
	fsm, store := newSQLiteFSM(t)
	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))
	meta.Placement[0].NodeID = strings.Repeat("n", metadataMaxBytes+1)

	result := requireResult(t, applyCommandAt(t, fsm, 1, Command{Op: OpPutMeta, Meta: &meta}))
	if result.Err != ResultErrInvalid {
		t.Fatalf("oversized command result = %+v, want invalid", result)
	}
	if _, err := store.GetKey("alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("oversized command mutated the store: %v", err)
	}
}
