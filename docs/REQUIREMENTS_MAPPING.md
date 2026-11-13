# Requirements Mapping

This document maps the course principles and the proposal components to the
code that implements them and to commands or tests that demonstrate them. Every
row points to a real package, file, test, or command in this repository. Rows
describe implemented behaviour only; the qualification notes at the end state
what the project does **not** claim.

## Distributed systems principles

| Principle | How the project applies it | Code | Evidence |
| --- | --- | --- | --- |
| Transparency | The gateway hides leader changes and the leader hides shard fan-out. A client calls any entry node; forwarding finds the leader and retries. | `internal/gateway/router.go`, `internal/gateway/client.go`, `internal/server/forward.go` | `docker compose run --rm gateway status`; write through a non-leader entry node; `tests/unit/gateway` |
| Fault tolerance | Raft tolerates two voter failures; RS(3,2) or five copies tolerate two failures after a healthy write or completed repair. Missing shards are reconstructed. | `internal/consensus/raft.go`, `internal/redundancy/rs32.go`, `internal/redundancy/replication.go`, `internal/cluster/repairer.go` | `docs/DEMO.md` two-node stop; `tests/chaos`; `tests/integration/read_path_test.go` |
| Consistency | Committed metadata is linearizable through Raft; a read commits `get_meta` first and returns only values matching the committed size and SHA-256. | `internal/consensus/fsm.go`, `internal/consensus/command.go`, `internal/server/read.go`, `internal/server/write.go` | `tests/integration/write_path_test.go`, `tests/integration/read_path_test.go`, `internal/server/read_test.go` |
| Scalability and overhead | RS(3,2) stores about 1.67x the logical payload versus 5.00x for five copies. Placement rotates ownership across the five nodes. | `internal/redundancy/rs32.go`, `internal/redundancy/replication.go`, `internal/redundancy/placement.go` | `./scripts/bench.ps1 -Runs 1 -Duration 15s`; `tests/unit/redundancy`; `tests/bench` |
| Observability | Health, per-node status, fragment and storage counters, degraded state, and repair counters are exposed without shelling into a node. | `internal/server/admin.go`, `internal/cluster/detector.go`, `internal/cluster/queue.go` | `gateway status`, `gateway health`, `gateway stats`; `tests/integration/admin_test.go` |
| Persistence | SQLite stores metadata and local fragments; Raft snapshots capture metadata only; fragments are repaired from peers. | `internal/storage/*`, `internal/consensus/snapshot.go`, `internal/consensus/fsm.go` | `tests/integration/persistence_test.go`, `internal/consensus/snapshot_test.go`; restart a node and `get` |
| Operability | One command builds and starts five healthy nodes; all commands run in Docker with no host Go; PowerShell and bash wrappers. | `docker-compose.yml`, `Dockerfile`, `scripts/up.ps1`, `scripts/up.sh`, `scripts/check.ps1`, `scripts/check.sh` | `./scripts/up.ps1`; `./scripts/check.ps1`; `./scripts/check-docs.ps1` |
| Security | A shared bearer token protects every RPC except the two standard health methods; internal clients propagate it. | `internal/server/auth.go`, `internal/rpc/credentials.go`, `cmd/server/main.go` | `tests/unit/server`, `tests/unit/rpc`, `tests/integration/auth_test.go`; `docs/SECURITY.md` |

## Proposal components

| Component | Implementation | Demo or verification command |
| --- | --- | --- |
| Five-node cluster | `docker-compose.yml` defines node1-node5 with one SQLite volume each; `cmd/server/main.go` wires every process. | `./scripts/up.ps1` then `docker compose ps` |
| gRPC API | `proto/storage.proto` is the contract; `internal/server/server.go` and the handler files implement it. | `docker compose run --rm gateway put demo-key hello --mode rs32` |
| Raft consensus | `internal/consensus/raft.go` runs Raft over raw TCP with BoltDB and file snapshots; `internal/consensus/fsm.go` applies metadata commands. | `docker compose run --rm gateway status` shows one leader and five voters |
| RS(3,2) erasure coding | `internal/redundancy/rs32.go` encodes three data shards and two parity shards; `internal/redundancy/placement.go` assigns one shard per node. | `docker compose run --rm gateway put demo-key hello --mode rs32`; `tests/unit/redundancy` |
| Five-copy replication | `internal/redundancy/replication.go` produces five full copies with indices 0-4. | `docker compose run --rm gateway put demo-key hello --mode replication`; compare the fragment table |
| Out-of-band fragments | The leader uploads through `internal/server/fragment_push.go`; `internal/server/fragments.go` implements the streaming `UploadFragment`/`DownloadFragment` transport. Raft carries metadata only. | Inspect `fragments` and `placements` in `node.db`; see the queries in `docs/OPERATIONS.md` |
| Failure detection | `internal/cluster/detector.go` probes `grpc.health.v1` and `NodeStatus` with hysteresis; only the leader probes. | `docker compose stop node4 node5`, then `gateway status`; `tests/unit/cluster`, `tests/chaos` |
| Automatic repair and returning-node refill | `internal/cluster/repairer.go` reconstructs missing shards; `internal/cluster/orphans.go` removes superseded local fragments. Membership stays fixed. | Stop/start nodes and watch `status`; `tests/chaos/restart_test.go` |
| SQLite persistence | `internal/storage/sqlite.go`, `schema.go`, `metadata.go`, and `fragments.go` implement the production store behind `storage.Store`. | Restart a node, then `gateway get` the key; `tests/integration/persistence_test.go` |
| Gateway and CLI | `cmd/gateway/main.go` wires configuration; `internal/gateway/cli.go` and `render.go` implement commands and JSON output. | `gateway put`, `get`, `delete`, `mode`, `status`, `health`, `stats`, `repair`; `tests/unit/gateway` |
| Benchmarks | `cmd/bench/main.go` and `internal/bench/*` implement load, storage, recovery, and reports; `scripts/bench.ps1` / `bench.sh` drive them. | `./scripts/bench.ps1 -Runs 1 -Duration 15s`; `tests/bench` |
| Tests | `tests/support`, `tests/unit`, `tests/integration`, `tests/chaos`, and `tests/bench` hold the suite; `scripts/check.ps1` / `check.sh` run the local gate. | `./scripts/check.ps1`; `./scripts/check-docs.ps1` |

## Course-principle evidence commands

| Goal | Command |
| --- | --- |
| Start clean and prove five healthy nodes | `./scripts/up.ps1` |
| Show both redundancy modes and storage overhead | `./scripts/demo.ps1` |
| Show a two-node failure, degraded write, and repair | `./scripts/demo.ps1` (failure section) |
| Inspect shard placement | `docker compose exec node1 sqlite3 /data/node.db "select stripe_id, shard_index, node_id from placements;"` |
| Show latency, throughput, and storage ratios | `./scripts/bench.ps1 -Runs 1 -Duration 15s` |
| Run the full local gate | `./scripts/check.ps1` |
| Check documentation links and stale strings | `./scripts/check-docs.ps1` |
| Prove auth enforcement | `./scripts/test.ps1` (unit and in-process integration) or `tests/integration/auth_test.go` |

## Scope notes and precise claims

These qualifications are part of the mapping. Do not drop them when quoting the
project.

### Fault tolerance

The two-failure guarantee applies to a **healthy write** (all five placements
stored) or a stripe that has been **repaired back to five placements**. Under
those conditions any three live nodes satisfy both the three-node Raft quorum
and RS reconstruction or replication. A **degraded RS write** that stored only
three shards is readable immediately but does not survive two further failures
until repair completes. A degraded replication write remains readable through
any surviving copy.

### Gateway routing

The consistent-hash ring selects an **entry node** for a request. That entry
node forwards to the Raft leader, and the leader coordinates shard placement and
fan-out. The gateway does not write directly to a placement owner, and the ring
does not determine the data layout.

### Rebalancing

Placement is deterministic from the sorted five node IDs. "Rebalancing" in this
project means (a) refilling a returning node's missing placements through the
repairer and (b) deleting orphan fragments with the node-local sweep. The project
does **not** implement dynamic membership, key migration, or general
rebalancing to a changed node set.

### Storage accounting

RS(3,2) stores about 1.67x the logical payload, which is about **67% extra**, not
67% of the original. Five-copy replication stores 5.00x, or **400% extra**. Five
full copies is the requested course scenario even though RS is more economical.

### Out of scope

The project deliberately does not implement:

- dynamic cluster membership beyond the fixed five nodes;
- per-shard Raft groups or multi-Raft;
- multi-language clients;
- encryption at rest;
- per-user authentication or authorization (the bearer token is a single shared
  credential with full API access);
- audit logging or request attribution;
- automatic TLS certificate provisioning;
- TLS or authentication on the Raft transport;
- a production-grade control plane or UI;
- a 3x replication mode.

## Related documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — design and request sequences.
- [DECISIONS.md](DECISIONS.md) — why these choices were made.
- [DEMO.md](DEMO.md) — the scripted evidence path.
- [TESTING.md](TESTING.md) and [../tests/README.md](../tests/README.md) — test
  layout and commands.
- [BENCHMARKS.md](BENCHMARKS.md) — measurement methodology and ratios.
