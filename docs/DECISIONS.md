# Architecture Decision Records

| ID | Title | Status |
| --- | --- | --- |
| ADR-001 | Use raw TCP for Raft traffic instead of gRPC | Accepted |
| ADR-002 | Serve linearizable reads through the Raft log | Accepted |
| ADR-003 | Make TLS optional and off by default | Accepted |
| ADR-004 | Static full-cluster bootstrap plus a dynamic `Join` RPC | Accepted |
| ADR-005 | Domain-oriented `cmd/` and `internal/` package layout | Accepted |
| ADR-006 | Use RS(3,2) and five-copy replication on a five-node cluster | Accepted |

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

`Get` is a Raft command (`{"op":"get","key":...}`) applied by the FSM, which
returns a `GetResult` to the caller. Followers forward reads to the leader; they
never answer from local state. Reads are linearizable because the returned value
is produced by an entry that committed after every previously committed write.

### Rationale

- Correctness is obvious and easy to test: a read that returns has passed through
  the same commit path as a write.
- No clock assumptions, no lease renewal, no extra `ReadIndex` RPC to implement.
- The FSM already needs a `get` code path for tests and internal inspection.

### Consequences

**Positive**

- Strongest practical consistency guarantee for a course project.
- Symmetric with writes: one code path, one failure model, one set of timeouts.

**Negative**

- Every read appends a log entry: extra disk I/O, extra replication traffic, and
  log growth that must be bounded by snapshots.
- Read latency includes a Raft round trip, even for hot keys.
- A single-node cluster still writes to BoltDB for each read.

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
hash rotation. The default mode is replication; an empty per-request mode
resolves to that default. Do not implement a 3x replication mode.

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

