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
// # Read coordinator
//
// Get delegates to the leader-side coordinator in read.go. The leader applies a
// get_meta command through Raft, so the metadata it reads is linearizable, then
// fetches fragments from the deterministic owners recorded in that metadata.
// Followers forward reads to the leader and never serve local replicas.
//
// Fetches run concurrently with READ_PARALLELISM workers and a READ_TIMEOUT
// deadline per owner. The leader reads its own placement directly from SQLite.
// RS reads try owners in randomized order and stop after three valid shards;
// replication reads try index order and stop after the first copy that matches
// the value checksum. A degraded stripe fetches all five placements so repair
// can see every missing or corrupt shard.
//
// Every shard's SHA-256 is verified before decode. RS(3,2) decodes from any
// three-shard subset, and replication takes one copy. The decoded value must
// match metadata.Size and metadata.Checksum. If it does not, the leader fetches
// any remaining placements and tries different subsets; an RS shard that never
// participates in a successful subset is treated as corrupt. Unverified bytes
// are never returned.
//
// After a successful read, failed placements are enqueued on a bounded,
// non-blocking read-repair queue. The worker re-reads metadata, drops the task
// if the key now points at another stripe or version, re-encodes the verified
// value, and uploads missing shards or replaces corrupt copies only on their
// intended owners. Read repair never clears metadata.degraded; task 09 owns the
// final status check and the shared repair queue.
//
// Read failures are status errors because GetResponse has no message field:
// NotFound for a missing key, FailedPrecondition for insufficient RS shards or
// no replication copy, DataLoss when enough shards arrived but no subset
// verified, DeadlineExceeded for a Raft read timeout, and Internal for store
// failures. ReadVerified exposes the same gather/decode/verify path to the task
// 09 repairer without enqueuing repair or writing metadata.
//
// # Admin surface
//
// GetConfig, SetConfig, Delete, and ClusterHealth are leader-coordinated.
// Followers forward through the same one-hop guard as Put and Get. GetConfig
// applies get_config through Raft so it observes every committed configuration
// change. SetConfig validates a concrete rs32 or replication mode, commits
// set_config, and returns the committed value. A request mode still overrides
// the global default.
//
// Delete commits delete_meta with the optional expected version atomically.
// Metadata removal always happens before fragment deletion; the returned old
// Placement then drives best-effort DeleteStripeFragments calls on every owner.
// A missing key is idempotent (success=true, found=false), and a version
// mismatch is Aborted.
//
// Stats is local-only and may be answered by any node. It reports key, stripe,
// and fragment counts from the store, SQLite+WAL+SHM bytes, the locally applied
// mode, the leader flag, and uptime. Repair counters and queue depth are zero
// until task 09 installs the shared repair queue.
//
// ClusterHealth is leader-only because only the leader has the complete Raft
// configuration. It probes every voter in parallel with NodeStatus and Stats
// under CLUSTER_PROBE_TIMEOUT (default 1s). A node is healthy only when both
// queries succeed. The response is degraded when any node is unhealthy or any
// committed key is marked degraded. This on-demand probing is intentionally
// simple; task 09 replaces it with a cached background detector and this
// handler will read the cache.
//
// AUTH_TOKEN is enforced by UnaryAuthInterceptor and StreamAuthInterceptor,
// which require "authorization: Bearer <token>" on every method except the two
// grpc.health.v1.Health methods. CheckAuth refuses to start an authenticated
// server when the process did not install those interceptors. Internal clients
// use internal/rpc.DialOptions to attach the bearer token and the configured
// message limit.
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
