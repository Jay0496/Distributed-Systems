// Package consensus implements the metadata Raft group: the Raft node wrapper,
// the finite state machine, the command envelope, and snapshot encoding.
//
// Raft entries carry metadata only. The metadata commands are put_meta,
// set_meta_status, get_meta, delete_meta, set_config, and get_config. They
// install or read storage.KeyMeta records, repair status, and the singleton
// cluster configuration; they never carry fragment payloads. Apply validates
// every metadata command, enforces a 64 KiB encoded-size limit, and returns
// typed Result values with stable error codes instead of Go errors.
//
// The FSM skips entries at or before the store's last applied index, so a
// restart replay cannot overwrite newer SQLite state. Snapshots use the FSM's
// version 3 metadata-only envelope. Fragment payloads are deliberately absent
// from snapshots; they live on their owner nodes and are repaired from peers.
// Version 1 and 2 snapshots are still readable when they carry no legacy
// values; a non-empty legacy section fails with an export instruction.
//
// The transport is injected by the caller, so the same package backs both the
// real TCP deployment and in-memory tests.
package consensus
