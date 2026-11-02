package storage

// KVStore is the interface that both the real SQLite store and the MockStore
// implement. The Raft FSM only depends on this interface, not on the concrete
// storage implementation.
type KVStore interface {
	Put(key string, value []byte, isParity bool) error
	Get(key string) ([]byte, error)
}
