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
// version 2 envelope with a metadata section and, during the transitional
// full-value path, a legacy section. Fragment payloads are deliberately absent
// from snapshots; they live on their owner nodes and are repaired from peers.
//
// The legacy put/get commands remain for compatibility until the write and read
// paths are migrated. Their payload is isolated in LegacyOp and they are exempt
// from the metadata size guard only during the transition.
//
// The transport is injected by the caller, so the same package backs both the
// real TCP deployment and in-memory tests.
package consensus
