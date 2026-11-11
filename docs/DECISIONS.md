# Architecture Decision Records

| ID | Title | Status |
| --- | --- | --- |
| ADR-001 | Use raw TCP for Raft traffic instead of gRPC | Accepted |
| ADR-002 | Serve linearizable reads through the Raft log | Accepted |
| ADR-003 | Make TLS optional and off by default | Accepted |
| ADR-004 | Static full-cluster bootstrap plus a dynamic `Join` RPC | Accepted |
| ADR-005 | Domain-oriented `cmd/` and `internal/` package layout | Accepted |
| ADR-006 | Use RS(3,2) and five-copy replication on a five-node cluster | Accepted |
| ADR-007 | Keep Raft metadata-only and move fragments out of band | Accepted |
| ADR-008 | Write fragments before metadata and require a three-shard quorum | Accepted |
| ADR-009 | Leader-coordinated verified reads with asynchronous read repair | Accepted |

---

## ADR-001: Use raw TCP for Raft traffic instead of gRPC

**Date:** 2026-11-01
**Status:** Accepted

### Context

The store needs a consensus layer and an external API. gRPC already carries client
traffic (`Put`, `Get`, `Join`, fragments, health). `hashicorp/raft` accepts a
pluggable `raft.Transport`, so consensus could either reuse the gRPC server or
open its own socket.

### Decision

Raft uses `raft.NewTCPTransport` on `RAFT_PORT` (60051-60055). gRPC remains the
only client-facing protocol on `PORT` (50051-50055).

### Rationale

- `raft.Transport` is byte-oriented: `AppendEntries`, `RequestVote`, and
  `InstallSnapshot` are already framed efficiently by the library. Wrapping them
  in protobuf adds encode/decode work on every heartbeat and batch without
  changing semantics.
- The library owns connection management, deadlines, and backoff. Re-implementing
  those semantics over gRPC streams is error-prone (see the "send timed out"
  behaviour when a peer disappears).
- Separate ports make failures easy to classify: gRPC healthy + Raft unhealthy
  points directly at consensus connectivity.

### Consequences

**Positive**

- Minimal glue code; the transport is a single constructor call.
- Client API changes never risk the consensus wire format.
- Consensus traffic is isolated from client load.

**Negative**

- One additional exposed port per node, plus firewall/Compose configuration.
- The Raft link is plaintext; there is no TLS or authentication on it. Acceptable
  only because the cluster runs on a private Docker network.
- `PEERS` duplicates addressing information already implied by the Raft
  configuration (it maps server ID to the *gRPC* address used for forwarding).

### Alternatives considered

- **Implement `raft.Transport` over gRPC.** Rejected: more code, more latency per
  heartbeat, and protobuf does not map cleanly onto the transport's
  request/response callback model.
- **Use a third-party Raft-over-gRPC transport.** Rejected: extra dependency and
  still no TLS benefit without additional provisioning.

---

## ADR-002: Serve linearizable reads through the Raft log

**Date:** 2026-11-01
**Status:** Accepted

### Context

Followers can lag. Serving `Get` from local state would return stale values, which
breaks the intuitive contract of a distributed KV store. The alternatives were a
leader lease, the read-index protocol, or committing the read itself.

### Decision

`Get` applies a metadata-only `get_meta` command through Raft, then gathers and
verifies fragments from the owners recorded in that metadata. Followers forward
reads to the leader; they never answer from local state. Reads are linearizable
because the metadata that selects the stripe and placements is produced by an
entry that committed after every previously committed write, and the returned
bytes must match that metadata's size and SHA-256 checksum.

### Rationale

- Correctness is obvious and easy to test: a read that returns has passed through
  the same metadata commit path as a write, then verified the payload against
  the committed checksum.
- No clock assumptions, no lease renewal, no extra `ReadIndex` RPC to implement.
- Metadata reads are small and carry no payload, so the log and snapshots stay
  bounded even when values are large.

### Consequences

**Positive**

- Strongest practical consistency guarantee for a course project.
- Symmetric with writes: one leader, one metadata commit path, one set of
  timeouts.
- Payload bytes stay out of Raft; reads gather only the shards or copies they
  need and can repair failed placements afterwards.

**Negative**

- Every read appends a small metadata entry: extra disk I/O and replication
  traffic, bounded by snapshots.
- Read latency includes a Raft round trip plus fragment fetches, even for hot
  keys.
- The leader coordinates all reads for now. A future read-index or lease read
  can replace this without changing the public API.

### Alternatives considered

- **Leader lease + local read.** Faster, but relies on synchronized clocks and
  careful lease handling.
- **Read-index (confirm leadership, then read locally).** Correct and cheaper,
  but more moving parts: followers must ask the leader for a read index and wait
  for the FSM to reach it.

Either can replace this decision later without changing the public API; the
`Get` handler is the only place that needs to change.

---

## ADR-003: Make TLS optional and off by default

**Date:** 2026-11-01
**Status:** Accepted

### Context

The project runs on a private Docker network for evaluation, but the API is also
meant to demonstrate production concerns. Requiring certificates would make
`docker compose up --build` fail out of the box and complicate the health check.

### Decision

TLS is controlled by `TLS_ENABLED`. When enabled, the server loads
`TLS_CERT_FILE`/`TLS_KEY_FILE`, and forwarding clients verify peers with
`TLS_CA_FILE`. When disabled (the default), both sides use insecure credentials.

### Rationale

- Zero-friction cluster startup for grading and local development.
- The code path for TLS exists and is exercised by configuration, so enabling it
  is an operational step rather than a code change.
- Keeps a single binary for both modes.

### Consequences

**Positive**

- `docker compose up --build` works without provisioning secrets.
- No accidental dependency on host-specific certificate paths.

**Negative**

- The default deployment is plaintext; traffic on the Docker bridge is readable
  by anything with access to the network namespace.
- The Compose health check dials with insecure credentials, so enabling TLS in
  Compose also requires updating `cmd/healthcheck` or the health check command.
- Certificates are not issued or rotated automatically.

### Alternatives considered

- **TLS always on with a self-signed cert baked into the image.** Rejected:
  baking private keys into an image is a bad pattern, and hostname verification
  (`node1`, `node2`, …) still needs SAN planning.
- **TLS terminated by a proxy.** Rejected: adds another component to operate.

---

## ADR-004: Static full-cluster bootstrap plus a dynamic `Join` RPC

**Date:** 2026-11-01
**Status:** Accepted

### Context

Nodes must agree on the initial Raft configuration. Bootstrapping node1 alone and
adding everyone else at runtime is fragile: until a node joins, it is not part of
the cluster, and a bootstrapped single-node cluster that later loses its volume
can diverge from the rest.

### Decision

- `BOOTSTRAP=true` on node1 supplies the **full** five-server configuration via
  `BOOTSTRAP_SERVERS=id=raft-addr,...`.
- Bootstrapping happens only when `raft.HasExistingState` is false, so a restart
  never rewrites membership.
- `Join(node_id, raft_addr, grpc_addr)` remains available for adding a node later;
  the leader calls `AddVoter`, and a follower answers with the current
  `leader_id` so the caller can retry against the right node.
- `JOIN_ADDR` makes a node attempt `Join` with ten retries, two seconds apart, so
  container start order does not matter.

### Rationale

- The full-configuration bootstrap avoids a window where node1 believes it is a
  single-node cluster and could commit writes that later nodes never see.
- Idempotent bootstrapping keeps the persisted membership authoritative across
  restarts and volume reuse.
- `Join` covers scale-out and is the natural place for future rebalancing.

### Consequences

**Positive**

- Deterministic startup with five nodes and no manual steps.
- Adding a sixth node needs only `JOIN_ADDR` and the peer/service definition.
- Membership changes survive restarts because they live in BoltDB.

**Negative**

- `BOOTSTRAP_SERVERS` must match the real topology and be identical on every node
  that eventually bootstraps; a typo produces a cluster that cannot elect a
  leader.
- Exactly one node may bootstrap a given cluster. Two independent bootstraps create
  two clusters with the same IDs.
- `Join` is only as good as `PEERS`: forwarding still depends on a correct
  ID-to-gRPC-address map, and `Join` does not update it.
- No automatic rebalancing or removal of dead voters yet.

### Alternatives considered

- **Bootstrap node1 alone, join the rest dynamically.** Rejected: creates a
  single-node commitment window and makes fresh-cluster startup order-sensitive.
- **External service discovery (Consul, etcd, DNS SRV).** Rejected: unnecessary
  infrastructure for a five-node Compose deployment.

---

## ADR-005: Domain-oriented `cmd/` and `internal/` package layout

**Date:** 2026-11-10
**Status:** Accepted

### Context

The first phase accumulated domain logic in the root `main` package, kept
`consensus/`, `storage/`, and `gateway/` at the top level, and mixed the gateway
CLI with its router. That makes ownership unclear, blocks black-box tests, and
forces every later feature stack to touch the same root files.

### Decision

Reorganize the repository into process entrypoints under `cmd/` and domain
packages under `internal/`:

- `cmd/server`, `cmd/gateway`, `cmd/healthcheck`, `cmd/bench` hold process
  wiring and flag/argument parsing only.
- `internal/config`, `internal/server`, `internal/consensus`, `internal/storage`,
  `internal/redundancy`, `internal/gateway`, `internal/cluster` hold domain code.
- Tests that use exported APIs move under `tests/`; tests that need unexported
  internals stay next to the package and are listed in `tests/README.md`.
- `internal/` is appropriate because this is an application, not a published
  library; the interfaces inside it remain the extension points.

### Rationale

- A feature stack (SQLite, redundancy, fragment transport) can change one
  package without conflicting with the others.
- The import graph becomes enforceable: domain packages never import the server
  or the gateway, and `cmd/*` contains no business logic.
- Process startup/signal handling is separated from handler behaviour, so
  handlers can be tested over `bufconn` without running a process.

### Consequences

**Positive**

- Clear ownership and smaller, reviewable diffs for every later task.
- Import cycles are easy to spot and reject in review.
- Test seams are documented instead of implicit.

**Negative**

- One large behaviour-preserving move before feature work starts.
- Build commands and documentation must switch to `./cmd/...` paths.
- White-box tests remain temporarily next to their packages until the test-suite
  task finishes the move.

### Alternatives considered

- **Keep the root `main` package and add subpackages incrementally.** Rejected:
  it preserves the import-cycle risk and leaves process wiring mixed with
  handlers.
- **Move everything to `pkg/`.** Rejected: nothing here is intended for external
  consumption, so `internal/` communicates the boundary more accurately.

---

## ADR-006: Use RS(3,2) and five-copy replication on a five-node cluster

**Date:** 2026-11-10
**Status:** Accepted

### Context

The cluster has exactly five storage nodes, and Raft needs a quorum of three to
make progress. The redundancy layer can either erasure-code each value or keep
full copies. A 3x replication scheme is common in larger clusters but leaves two
of the five nodes without a fragment, so a stripe would not use the whole
cluster and the failure story would not line up with the five-node topology.

### Decision

Support exactly two modes, both of which produce five fragments per stripe:

- `rs32`: Reed-Solomon with three data shards and two parity shards, using the
  global indices `0,1,2` for data and `3,4` for parity.
- `replication`: five full copies, global indices `0..4`, every index data.

Each stripe places its five fragments on five distinct nodes with deterministic
hash rotation. The cluster default mode is `rs32`; an empty per-request mode
resolves to the mode stored in cluster configuration, falling back to `rs32`
when no configuration exists. Do not implement a 3x replication mode.

### Rationale

- Five fragments on five nodes means any three live nodes hold enough fragments
  to satisfy both the Raft quorum and stripe reconstruction, so two node
  failures are survivable after a healthy write.
- RS(3,2) stores `5 * ceil(size/3)` bytes, about 1.67x the value (about 67%
  extra), while five-copy replication stores `5 * size` bytes (400% extra).
  RS is the economical mode; replication is the simple, fast mode used for
  comparison and for cases where every node should hold a complete value.
- Indices are global and mode-aware, so metadata can say whether a fragment is
  parity without re-reading the mode. In replication mode indices 3 and 4 are
  data, not parity.
- Both modes benefit from the same checksum and placement rules, which keeps
  repair and read repair simple: a fragment is either valid against its SHA-256
  checksum and owner, or it is missing/corrupt.

### Consequences

**Positive**

- The redundancy degree matches the five-node cluster and the 3/5 Raft quorum.
- Storage measurements have a clear baseline: RS is ~1.67x total, replication
  is 5x total.
- A deterministic placer makes fragment ownership reproducible across leaders
  and restarts.

**Negative**

- RS adds encode/decode CPU and a dependency on `klauspost/reedsolomon`.
- A degraded RS write may store only three shards; those three do not
  necessarily survive two further failures, so repair is urgent after a degraded
  write.
- An empty value is a storage-math exception: RS uses five one-byte shards
  (5 bytes) because Reed-Solomon cannot encode zero-length shards, while
  replication uses zero bytes.

### Alternatives considered

- **3x replication.** Rejected: it uses only three of five nodes, leaves no
  fragment on two nodes, and does not match the five-node failure model.
- **RS(2,1) or RS(2,2).** Rejected: the task specifies RS(3,2), and five shards
  map cleanly one-per-node.
- **Per-shard Raft groups or multi-Raft.** Out of scope for the course project;
  metadata-only Raft plus out-of-band fragment transfer is the agreed design.

---

## ADR-007: Keep Raft metadata-only and move fragments out of band

**Date:** 2026-11-10
**Status:** Accepted

### Context

The original write path put the full value in `consensus.Command` and stored it
on every node. That made the system behave like accidental 5x replication,
inflated the Raft log and snapshots with payload bytes, and left no way to store
only one RS shard per node. The project needs a five-node Raft quorum and a
redundancy layer that places fragments independently.

### Decision

The Raft log carries metadata only:

- `put_meta` installs a `storage.KeyMeta` for a key: stripe ID, mode, size,
  checksum, version, degraded flag, and five placements.
- `set_meta_status` updates only the degraded flag after a repair.
- `get_meta`, `delete_meta`, `set_config`, and `get_config` read or mutate
  metadata and cluster configuration.
- The leader encodes values, pushes fragments to their owners out of band, and
  commits the metadata entry after the fragments are durable. Reads gather
  enough fragments and verify the checksum before returning a value.

Every metadata command is validated at the consensus boundary. The FSM enforces
a 64 KiB encoded-size guard, a non-empty key and stripe ID, a concrete mode, a
64-character SHA-256 checksum, a version of at least 1, and exactly five
distinct placements whose parity flags match the mode. Apply returns typed
`Result` values (`conflict`, `invalid`, `not_found`, `internal`) rather than Go
errors so a rejected command can never look like a Raft failure.

The transitional `put`/`get` commands still carry values, but the payload is
isolated in a nested `LegacyOp` so metadata commands have no value-shaped field.
They are exempt from the size guard only while the transition lasts. The write
path stopped writing them in task 06; task 15 removes them and the legacy
snapshot section.

Snapshots use an FSM-owned envelope with `schema_version: 2`:

```json
{
  "schema_version": 2,
  "metadata": { "stripes": [], "keys": [], "config": {}, "last_applied_index": 0 },
  "legacy": []
}
```

The metadata section is the SQLite metadata payload, including
`last_applied_index`; the legacy section is the transitional KV payload. Restore
runs both sections in one SQLite transaction when the store supports it, rejects
unknown schema versions, and still reads version 1 snapshots for upgrade safety.
Metadata snapshots never include fragment payloads.

### Rationale

- The storage-economy claim is only true if the log and snapshots do not contain
  the values. This decision makes the claim testable: a 1 MiB write produces a
  metadata entry bounded by 64 KiB.
- Fragment placement is deterministic and independent of Raft membership. The
  leader can push shards to five owners without creating five Raft groups.
- Metadata commands are small, so log compaction, snapshot transfer, and replay
  are cheap compared with writing full-value entries on every node.
- Typed results and the applied-index guard make retries and restart replay
  idempotent, which is required when SQLite may already be ahead of a snapshot.

### Consequences

**Positive**

- Raft entries and snapshots contain no payload bytes.
- RS(3,2) can store one shard per node and replication can store one copy per
  node without changing consensus.
- Version checks and `put_meta` idempotency prevent stale writers from
  overwriting newer metadata.
- A future read-index or lease read can replace the current leader-coordinated
  read path without changing the metadata model.

**Negative**

- The leader coordinates writes and reads: it encodes, pushes fragments,
  gathers shards, decodes, and triggers repair. This is extra leader load and a
  single coordination point, acceptable for the course scope.
- Metadata and fragments are committed separately. A crash between fragment
  upload and metadata commit leaves orphan fragments for the local sweep; a
  crash after metadata commit but before all fragment uploads is recorded as
  `degraded` and repaired later.
- The version 2 snapshot temporarily carries both metadata and legacy sections,
  so the FSM must keep the compatibility path until the legacy commands are
  removed.

### Alternatives considered

- **Put full values in Raft and replicate everything.** Rejected: that is
  accidental 5x replication, does not support RS shard placement, and inflates
  the log and snapshots.
- **A Raft group per shard or per stripe.** Rejected: multi-Raft is explicitly
  out of scope and would multiply consensus overhead for no course benefit.
- **Commit metadata first and upload fragments afterwards.** Rejected as the
  only path: it would make every write visible before its data is durable.
  Uploading first and committing metadata second keeps the commit as the
  visibility point; failures degrade and repair.
- **Keep values in a separate Raft payload field with a size check.** Rejected:
  a top-level value field invites accidental payload writes and makes the
  metadata-only invariant hard to enforce. Nested `LegacyOp` is explicit and is
  removed with the transition.


---

## ADR-008: Write fragments before metadata and require a three-shard quorum

**Date:** 2026-11-11
**Status:** Accepted

### Context

Fragments and metadata live in different places: fragments are node-local SQLite
rows, while key metadata is committed through Raft. The write path must choose an
ordering and must decide how many stored fragments make a write successful. A
partial failure is normal on a five-node cluster: an owner can be unreachable
while the Raft quorum is healthy.

### Decision

The leader stores fragments first and commits `put_meta` last. A write succeeds
when at least `writeQuorum = 3` shards or copies are durably stored and the
metadata commit succeeds. Five stored fragments is a healthy write; three or four
is committed with `Degraded=true`.

Cleanup depends on what the commit outcome proves:

- A definite rejection (`conflict` or `invalid`) proves the entry was not
  applied. The new stripe's successfully uploaded fragments are deleted.
- An ambiguous failure (timeout, leadership loss, connection loss, or an internal
  apply error) may have committed. Fragments are retained; deleting them could
  destroy the live stripe the caller cannot see yet. The task 09 orphan sweep
  removes only fragments that no committed metadata references after a grace
  period.
- A successful overwrite garbage-collects the previous stripe's fragments
  asynchronously, after the new metadata commits, on the owners recorded in the
  old metadata. The apply has already removed the old stripe and placement rows.

### Rationale

- Metadata is the visibility point. Committing it before the data exists would
  let a read observe a key whose fragments were never stored. Fragments first
  means committed metadata always points at data that was at least attempted.
- Three matches the Raft quorum. With all five fragments stored, any three live
  nodes hold enough data for both consensus and reconstruction, so two failures
  are survivable. A degraded RS write that stored only three shards is readable
  now but needs repair before the strong two-failure claim applies; a degraded
  replication write remains readable through any surviving copy.
- Collecting all upload results instead of cancelling on the first error is what
  makes partial success usable. Cancellation would turn one slow owner into a
  failed write even though three healthy owners already stored the stripe.
- Asynchronous GC keeps the client latency independent of slow or dead old
  owners. Leaked fragments are invisible to reads and are counted by stats.

### Consequences

**Positive**

- Committed metadata never points at a stripe that had no chance to store data.
- One or two unreachable owners produce a successful, explicitly degraded write
  rather than an outage.
- Failed writes normally leave neither metadata nor fragments; ambiguous failures
  are the only case that intentionally leaves garbage, and the sweep owns it.

**Negative**

- A degraded RS write is not two-failure tolerant until repair completes.
- A crash between fragment upload and metadata commit leaves orphan fragments.
- GC is best effort; a crash or unreachable old owner leaves fragments for the
  task 09 sweep.
- Replication writes upload five full values, so bandwidth and storage are 5x by
  design.

### Alternatives considered

- **Commit metadata first, then upload fragments.** Rejected: readers could see a
  key whose fragments do not exist, and every write would need a second status
  update to become durable.
- **Require all five fragments.** Rejected: one unreachable owner would fail the
  write even though three copies are enough for the Raft quorum and for
  reconstruction. Degraded writes are the intended compromise.
- **Require only one fragment.** Rejected: a single stored shard does not satisfy
  the three-node failure model and would make nearly every partial failure look
  successful.
- **Synchronous GC before responding.** Rejected: a dead old owner would delay a
  write that is already durable and visible.

---

## ADR-009: Leader-coordinated verified reads with asynchronous read repair

**Date:** 2026-11-11
**Status:** Accepted

### Context

After the metadata-only write path, values live in node-local fragments rather
than in the Raft FSM or the legacy `store` table. A read must choose a stripe
from committed metadata, gather enough fragments from five independent owners,
verify them, and cope with missing or corrupt placements without returning
unverified bytes. Repair must not race an overwrite and resurrect a superseded
stripe.

### Decision

The leader coordinates reads:

1. Apply `get_meta` through Raft so the stripe, mode, placements, size, and
   checksum are linearizable.
2. Fetch fragments concurrently with bounded parallelism and a per-owner
   timeout. Read the leader's own placement from SQLite instead of dialing
   loopback.
3. For RS, accept any three valid shards; for replication, accept the first
   copy that matches the value checksum. A degraded stripe fetches all five
   placements so repair can see every failure.
4. Verify each shard's SHA-256 before decode and the original value's size and
   SHA-256 after decode. If decode does not verify, fetch any remaining
   placements and try other subsets. Never return unverified or padded bytes.
5. Enqueue failed placements on a bounded asynchronous read-repair queue. The
   worker re-reads metadata, drops the task unless the key still points at the
   same stripe ID and version, re-encodes the verified value, and uploads only
   the missing/corrupt shards to their intended owners. Corrupt copies use
   `replace=true`; missing shards use the ordinary upload.
6. Read repair does not clear `metadata.degraded` in this task; task 09 owns the
   final all-five-present check.

`ReadVerified(ctx, meta)` exposes the same verified gather path to the repair
subsystem without enqueuing repair or writing metadata. `internal/cluster` owns
the queue interface and never imports `internal/server`.

### Rationale

- Metadata is the visibility point and the checksum is the integrity contract.
  Reading metadata through Raft keeps reads linearizable without a lease or
  read-index implementation in this task.
- Three RS shards or one replication copy are sufficient by design, so one or
  two unreachable owners should degrade latency, not availability.
- Verifying the final value checksum is essential: three individually valid RS
  shards can still disagree. Trying more subsets is the only way to distinguish
  a bad shard from a bad metadata record, and `DataLoss` is safer than returning
  bytes that do not verify.
- Read repair is asynchronous because a successful read is already durable. A
  slow or dead owner must not turn a good read into a timeout, and a bounded
  queue prevents a failure storm from exhausting memory.
- Re-reading metadata before repair is the race-safety rule. Stripe IDs are
  immutable, so a task that still matches key, stripe, and version can safely
  write only the failed placements.

### Consequences

**Positive**

- Reads tolerate any two missing/corrupt shards after a healthy write.
- Corrupt or missing fragments are restored after a successful read without
  blocking the client.
- Raft stays metadata-only; read traffic does not put values in the log.
- The same verified gather path is reusable by the task 09 repairer.

**Negative**

- The leader carries all read coordination CPU and network load for now.
- Read repair is best effort: queue saturation, a crash, or a leader change can
  drop a task, leaving the next read to retry it. Task 09 adds scheduling,
  retries, and backoff.
- A degraded stripe still sets `degraded=true` after read repair until task 09
  performs the all-five-present check.
- Reads still append a small metadata entry; read-index or lease reads are future
  work.

### Alternatives considered

- **Read the local FSM value.** Rejected: values are no longer in the FSM, and a
  follower would return stale data.
- **Follower-local reads.** Rejected for this task: requires lease or read-index
  machinery and would complicate the failure story.
- **Synchronous read repair.** Rejected: a dead owner would delay a read whose
  value is already verified.
- **Repair every placement on every read.** Rejected: multiplies traffic and
  defeats the early-completion rule, except for degraded stripes where full
  inspection is required.
- **Clear `degraded` from the read path.** Rejected: it would create a metadata
  version/status race and duplicate task 09's all-five-present check.
