// Package redundancy implements Reed-Solomon RS(3,2) erasure coding, five-copy
// replication, shard checksums, and deterministic five-node placement.
//
// This package is storage-agnostic: it turns values into shards and back, and
// maps shard indices to node IDs. The write path is responsible for moving the
// shards to their owners and committing metadata through Raft.
package redundancy
