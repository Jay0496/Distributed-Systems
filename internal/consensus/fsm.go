package consensus

import (
	"encoding/json"
	"io"

	"distributed-kv-store/internal/storage"

	"github.com/hashicorp/raft"
)

// FSM is the Finite State Machine that Raft uses to apply committed log entries.
// It is the bridge between the Raft log and the actual key-value store.
type FSM struct {
	store storage.Store
}

// NewFSM creates a new FSM backed by the given store.
func NewFSM(store storage.Store) *FSM {
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
	case OpPut:
		// Deprecated: the legacy full-value path remains for compatibility
		// until task 06 switches the write path to metadata commands.
		if cmd.Legacy == nil {
			return &Result{Err: ResultErrInvalid}
		}
		return f.store.Put(cmd.Legacy.Key, cmd.Legacy.Value, false)
	case OpGet:
		// Deprecated: see OpPut.
		key := cmd.Key
		if cmd.Legacy != nil {
			key = cmd.Legacy.Key
		}
		value, err := f.store.Get(key)
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
