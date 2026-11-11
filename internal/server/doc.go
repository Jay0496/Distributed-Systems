// Package server implements the gRPC front end for a storage node. It serves
// the StorageService API, forwards client requests to the current Raft leader,
// and hosts the out-of-band fragment transport used by the redundancy layer.
//
// # Write coordinator
//
// Put delegates to the leader-side coordinator in write.go. The coordinator
// validates the request, resolves the effective mode (request, then cluster
// config, then rs32), allocates the next metadata version with a get_meta Raft
// read, and derives a unique stripe ID from the key, version, and random bytes.
//
// It then encodes five shards (RS(3,2) data and parity, or five full replication
// copies), places them on five distinct nodes with the deterministic placer, and
// uploads them concurrently. The leader writes its own shard through the local
// Store; remote owners receive the versioned fragment stream. Uploads are not
// cancelled on the first failure because three successes are enough.
//
// A write needs three stored fragments. With fewer, the successful uploads are
// removed and the response is success=false. With three or four, metadata
// commits with degraded=true. The put_meta command carries only key, stripe ID,
// mode, size, checksum, version, degraded flag, and placements; it never carries
// payload bytes. A definite commit rejection deletes the new stripe's fragments.
// An ambiguous failure retains them for the orphan sweep because the entry may
// have committed.
//
// After a successful overwrite, the coordinator asynchronously deletes the
// previous stripe's fragments on their owners. The put_meta apply already
// removed the old stripe and placement metadata rows, so garbage collection is
// fragment-only, runs after the new metadata commits, and never touches the
// stripe the write just created.
//
// Expected application failures return PutResponse{Success:false, Message}.
// gRPC status errors are reserved for transport-level failures such as a broken
// forward, which preserves the existing response shape for clients.
//
// # Fragment transport v2
//
// Fragments are addressed by the pair (stripe_id, shard_index). The stripe ID
// is opaque to this package; shard indices are global across redundancy modes:
//
//	index 0, 1, 2   RS(3,2) data shards, or full replication copies
//	index 3, 4      RS(3,2) parity shards, or full replication copies
//
// Because the transport does not know a stripe's mode, indices 0-2 may never
// be marked parity, while indices 3-4 carry the sender's is_parity flag. The
// legacy key, index, and is_parity request fields remain on the wire but are
// ignored by upload and download.
//
// Upload state machine
//
//  1. The first chunk must carry a non-empty stripe_id, shard_index in 0..4, a
//     SHA-256 checksum, a non-negative total_size no larger than
//     MAX_FRAGMENT_SIZE, and offset 0.
//  2. Chunks are appended to a staging buffer keyed by (stripe_id,
//     shard_index). Every chunk must repeat the same identity, total size,
//     checksum, replace flag, and is_parity flag. The offset must equal the
//     current buffer length; gaps, overlaps, and duplicate offsets are
//     rejected with InvalidArgument.
//  3. A second concurrent upload for the same identity is rejected with
//     Aborted. A node stages at most 64 fragments at once and rejects further
//     uploads with ResourceExhausted.
//  4. On last=true the buffer length must equal total_size and its SHA-256
//     digest must match the checksum. Only then is the fragment committed with
//     PutFragment, which upserts atomically. Staging state is always released.
//  5. An upload that closes without last=true is rejected. If the fragment
//     already exists with the same checksum the upload succeeds without a
//     rewrite. A different checksum requires replace=true (repair only) and
//     otherwise returns AlreadyExists.
//
// A complete fragment may be split across any number of chunks, each no larger
// than MAX_MESSAGE_SIZE; clients use UPLOAD_CHUNK_SIZE as their preferred
// chunk size.
//
// # Download
//
// Download requires (stripe_id, shard_index), verifies the stored checksum,
// and streams the payload in UPLOAD_CHUNK_SIZE pieces. Every message repeats
// the total size and checksum and carries its offset; last=true marks the final
// message. A zero-length fragment produces exactly one message with offset 0,
// total_size 0, the checksum of the empty string, and last=true. Missing and
// corrupt fragments map to NotFound and Internal respectively.
//
// # Maintenance operations
//
// DeleteFragment is idempotent. DeleteStripeFragments removes every local
// fragment for a stripe and is used by overwrite garbage collection.
// ListFragments streams metadata references (optionally filtered by stripe) and
// StatFragment reports size and checksum without loading the payload. The
// transport never decides whether a stripe is current or superseded; that is
// metadata garbage collection's job.
//
// # Limits and timeouts
//
// The fragment limits come from internal/config: MAX_FRAGMENT_SIZE (64 MiB),
// MAX_MESSAGE_SIZE (32 MiB), UPLOAD_CHUNK_SIZE (1 MiB), and FRAGMENT_TIMEOUT
// (10 s per operation). The process applies MAX_MESSAGE_SIZE to the gRPC server
// and to forwarding dials so a single oversized message is rejected before it
// reaches a handler.
package server
