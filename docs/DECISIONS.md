# Architecture Decision Records

| ID | Title | Status |
| --- | --- | --- |
| ADR-001 | Use raw TCP for Raft traffic instead of gRPC | Accepted |
| ADR-002 | Serve linearizable reads through the Raft log | Accepted |
| ADR-003 | Make TLS optional and off by default | Accepted |
| ADR-004 | Static full-cluster bootstrap plus a dynamic `Join` RPC | Accepted |

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
