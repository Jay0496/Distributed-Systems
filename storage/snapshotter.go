package storage

// Snapshotter is an optional interface for stores that can serialize and
// restore their entire contents. The Raft FSM uses it to compact its log.
//
// KVStore intentionally does not include these methods so that storage
// implementations can adopt snapshotting independently.
type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}
