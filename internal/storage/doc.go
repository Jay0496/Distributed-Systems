// Package storage defines the local key-value fragment store used by the Raft
// FSM. It provides the KVStore interface, the optional Snapshotter interface,
// and the concrete implementations.
//
// MockStore is an in-memory development and test double. It remains in this
// package temporarily and is removed from production wiring by the SQLite
// production-store task; new code should depend on KVStore, with SQLiteStore as
// the production implementation.
package storage
