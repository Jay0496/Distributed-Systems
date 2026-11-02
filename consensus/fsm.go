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
	Op    string // "put"
	Key   string
	Value []byte
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
	default:
		return nil
	}
}

// Snapshot returns a snapshot of the current state. (Skeleton implementation.)
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.store}, nil
}

// Restore restores the state from a snapshot. (Skeleton implementation.)
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return nil
}

type fsmSnapshot struct {
	store storage.KVStore
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
