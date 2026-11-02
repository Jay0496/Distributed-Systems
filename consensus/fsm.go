package consensus

import (
	"encoding/json"
	"io"

	"distributed-kv-store/storage"

	"github.com/hashicorp/raft"
)

// Command represents an operation that will be serialized into the Raft log
// and later applied to the state machine.
type Command struct {
	Op    string // "put" or "get"
	Mode  string // "replication" or "rs32" (empty means default)
	Key   string
	Value []byte
}

// GetResult is returned by the FSM for "get" commands so that reads can be
// served from the committed log (linearizable reads).
type GetResult struct {
	Value []byte
	Found bool
	Err   string
}

// FSM is the Finite State Machine that Raft uses to apply committed log entries.
// It is the bridge between the Raft log and the actual key-value store.
type FSM struct {
	store storage.KVStore
}

// NewFSM creates a new FSM backed by the given key-value store.
func NewFSM(store storage.KVStore) *FSM {
	return &FSM{store: store}
}

// Apply is called by Raft on every node once a log entry has been committed
// by a quorum. It must apply the command to the local store.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command

	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case "put":
		return f.store.Put(cmd.Key, cmd.Value, false)
	case "get":
		value, err := f.store.Get(cmd.Key)
		if err != nil {
			return &GetResult{Found: false}
		}
		return &GetResult{Value: value, Found: true}
	default:
		return nil
	}
}

// Snapshot returns a snapshot of the current state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	var data []byte
	if snapshotter, ok := f.store.(storage.Snapshotter); ok {
		var err error
		data, err = snapshotter.Snapshot()
		if err != nil {
			return nil, err
		}
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore restores the state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	snapshotter, ok := f.store.(storage.Snapshotter)
	if !ok {
		return nil
	}
	return snapshotter.Restore(data)
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
