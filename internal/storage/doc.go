// Package storage defines the local persistence layer used by the Raft FSM.
// It provides the Store interface and the SQLite production implementation.
//
// The store has no full-value surface: a committed key is metadata plus
// node-local fragments. Metadata snapshots never contain fragment payloads.
// Fragments are durable on their owner nodes and are repaired from peers when
// missing.
//
// Test doubles live under tests/support; production packages must not import
// them.
package storage
