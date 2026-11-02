package consensus

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"distributed-kv-store/storage"

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

func TestFSMPutAndGet(t *testing.T) {
	store := storage.NewMockStore()
	fsm := NewFSM(store)

	if ret := applyJSON(t, fsm, Command{Op: "put", Mode: "rs32", Key: "k", Value: []byte("v")}); ret != nil {
		t.Fatalf("put returned %v", ret)
	}

	ret := applyJSON(t, fsm, Command{Op: "get", Key: "k"})
	result, ok := ret.(*GetResult)
	if !ok {
		t.Fatalf("expected *GetResult, got %T", ret)
	}
	if !result.Found || string(result.Value) != "v" {
		t.Fatalf("unexpected get result: %+v", result)
	}
}

func TestFSMGetMissingKey(t *testing.T) {
	fsm := NewFSM(storage.NewMockStore())

	ret := applyJSON(t, fsm, Command{Op: "get", Key: "absent"})
	result, ok := ret.(*GetResult)
	if !ok {
		t.Fatalf("expected *GetResult, got %T", ret)
	}
	if result.Found {
		t.Fatalf("expected Found=false, got %+v", result)
	}
}

func TestFSMBadJSON(t *testing.T) {
	fsm := NewFSM(storage.NewMockStore())

	ret := fsm.Apply(&raft.Log{Data: []byte("{not-json")})
	if _, ok := ret.(error); !ok {
		t.Fatalf("expected an error for invalid JSON, got %T", ret)
	}
}

func TestFSMUnknownOp(t *testing.T) {
	fsm := NewFSM(storage.NewMockStore())

	if ret := applyJSON(t, fsm, Command{Op: "delete", Key: "k"}); ret != nil {
		t.Fatalf("unknown op returned %v, want nil", ret)
	}
}

func TestFSMSnapshotRestoreRoundTrip(t *testing.T) {
	store := storage.NewMockStore()
	fsm := NewFSM(store)

	applyJSON(t, fsm, Command{Op: "put", Key: "a", Value: []byte("1")})
	applyJSON(t, fsm, Command{Op: "put", Key: "b", Value: []byte("2")})

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	sink := &memSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	if !sink.closed {
		t.Fatal("snapshot sink was not closed")
	}
	if sink.canceled {
		t.Fatal("snapshot sink was canceled")
	}

	restoredStore := storage.NewMockStore()
	restoredFSM := NewFSM(restoredStore)
	if err := restoredFSM.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes()))); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := restoredStore.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestFSMSnapshotWithNonSnapshotter(t *testing.T) {
	// A store that only implements KVStore must still produce a valid snapshot.
	fsm := NewFSM(storeWithoutSnapshot{})

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	sink := &memSink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes()))); err != nil {
		t.Fatalf("Restore should ignore non-snapshot stores: %v", err)
	}
}

type storeWithoutSnapshot struct{}

func (storeWithoutSnapshot) Put(string, []byte, bool) error { return nil }
func (storeWithoutSnapshot) Get(string) ([]byte, error)     { return nil, nil }
