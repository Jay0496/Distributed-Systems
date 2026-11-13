# Distributed NoSQL Key-Value Store

A five-node distributed key-value store for the course project. Client traffic is
served over gRPC, every write is committed through a Raft consensus group, and
each node keeps its own persistent Raft log on disk.

> **No local Go installation is required.** Every build, test, and tool command
> in this repository is run inside the `golang:1.22` Docker image.

## Current status

| Area | Owner | State |
| --- | --- | --- |
| Raft consensus, snapshots, forwarding, gRPC surface | Jay Savaliya | Complete for this phase |
| CLI / routing gateway | Khalid | Complete (`cmd/gateway/`, `internal/gateway/`) |
| SQLite persistence | Parth | Complete for this task (`internal/storage/`, `node.db`) |
| Failure management / Reed-Solomon redundancy | Dhruvin | Engines and placement implemented (`internal/redundancy`); write-path integration in task 06 |

The server opens the production SQLite `storage.Store` before Raft starts and
closes it after Raft and gRPC stop. The database lives at `STORE_PATH`
(default `${DATA_DIR}/node.db`) and holds the transitional legacy KV rows plus
the metadata, placement, fragment, config, and applied-index tables used by
later tasks. The in-memory test double now lives only in `tests/support`;
production packages do not import it. See
[internal/storage/README.md](internal/storage/README.md).

## Architecture at a glance

```
client ──gRPC──► any node ──forward──► leader ──Raft (TCP)──► other nodes
                    │                     │
                    │                     └─ FSM.Apply ─► Store
                    └─ health, fragments, status
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed request path,
state layout, and recovery behaviour. Design trade-offs are recorded in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Components

| Path | Purpose |
| --- | --- |
| `cmd/server/` | Process entrypoint: config loading, Raft/gRPC startup, TLS, health, signals |
| `cmd/gateway/` | CLI entrypoint: gateway configuration, client construction, command dispatch |
| `cmd/healthcheck/` | Tiny binary used by Docker Compose health checks |
| `cmd/bench/` | Benchmark harness: load generator, storage ratios, recovery timing, reports |
| `scripts/` | PowerShell and bash wrappers for up, down, reset, demo, test, and bench |
| `internal/config/` | Environment configuration parsing |
| `internal/server/` | gRPC handlers: `Put`, `Get`, `Heartbeat`, `Join`, `NodeStatus`, fragments, repair, admin, auth |
| `internal/server/forward.go` | Leader discovery, cached client connections, one-hop forwarding, retry and loop guard |
| `internal/server/fragments.go` | `UploadFragment`, `DownloadFragment`, `Repair` handlers |
| `internal/redundancy/` | RS(3,2), five-copy replication, shard checksums, deterministic five-node placement |
| `internal/consensus/raft.go` | Raft node: BoltDB log/stable store, file snapshots, bootstrap, `AddVoter`, `Close` |
| `internal/consensus/fsm.go` | FSM: put/get commands, real snapshot and restore |
| `internal/storage/` | `Store` interface, SQLite metadata/fragment/config repositories, snapshots |
| `internal/gateway/` | Consistent-hash entry ring, retrying client, CLI commands, table/JSON rendering |
| `internal/cluster/` | Failure detection, degraded-mode reporting, repair scheduling (created now, populated later) |
| `proto/storage.proto` | gRPC contract (generated files are committed) |
| `tests/` | Unit, integration, chaos, and benchmark suites (see `tests/README.md`) |

## Ports

| Node | gRPC | Raft | Data volume |
| --- | --- | --- | --- |
| node1 | 50051 | 60051 | `node1-data` |
| node2 | 50052 | 60052 | `node2-data` |
| node3 | 50053 | 60053 | `node3-data` |
| node4 | 50054 | 60054 | `node4-data` |
| node5 | 50055 | 60055 | `node5-data` |

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_ID` | `node1` | Unique Raft server ID; also the advertised hostname in Compose |
| `PORT` | `50051` | gRPC listen port |
| `RAFT_PORT` | `60051` | Raft TCP listen port |
| `DATA_DIR` | `/data` | Directory for `node.db`, `raft.db`, and `snapshots/` |
| `STORE_PATH` | `${DATA_DIR}/node.db` | SQLite database file |
| `STORE_BUSY_TIMEOUT` | `5s` | SQLite lock wait |
| `BOOTSTRAP` | `false` | `true` on the seeding node only |
| `BOOTSTRAP_SERVERS` | empty | `id=raft-addr,...` for the full cluster; used only when `BOOTSTRAP=true` and no local state exists |
| `JOIN_ADDR` | empty | Optional `host:port` of a live node used to add this node as a voter |
| `PEERS` | empty | `id=grpc-addr,...`; required for leader forwarding |
| `TLS_ENABLED` | `false` | Enable TLS on the gRPC server and forwarding clients |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | empty | Server certificate and key |
| `TLS_CA_FILE` | empty | CA used by forwarding clients to verify peers |
| `AUTH_TOKEN` | empty | Shared bearer token for all nodes and tools; empty disables auth with a startup warning |

## Quick start

Prerequisites: Docker Engine or Docker Desktop with the Compose v2 plugin.
Go is **not** required on the host. The bash scripts use `jq` when available and
fall back to `docker inspect` when it is not.

Windows PowerShell:

```powershell
./scripts/up.ps1
./scripts/demo.ps1
./scripts/down.ps1
```

Bash (WSL, Git Bash, macOS, Linux):

```bash
./scripts/up.sh
./scripts/demo.sh
./scripts/down.sh
```

`up` builds the image, starts node1-node5 in the background, waits for all five
health checks, and prints `docker compose ps` plus `gateway status`. It exits
non-zero if any node is unhealthy or the cluster stays degraded.

The raw Compose command still works:

```powershell
docker compose up --build -d
docker compose ps
```

All five containers should report `healthy`. Kill the cluster:

```powershell
docker compose down        # keeps data
docker compose down -v     # deletes data volumes
```

Prefer the scripts: `down` preserves volumes by default, while `reset`
explicitly deletes them.

## Deployment scripts

| Script | Purpose | Key flags |
| --- | --- | --- |
| `scripts/up.ps1` / `up.sh` | Build, start, wait for health, print status | `-Timeout` / `--timeout`, `-Help` / `--help` |
| `scripts/down.ps1` / `down.sh` | Stop the cluster; keep volumes by default | `-Volumes` / `--volumes` |
| `scripts/reset.ps1` / `reset.sh` | Destructive reset: containers, network, and all five volumes | `-Build` / `--build` |
| `scripts/demo.ps1` / `demo.sh` | End-to-end RS/replication, failure, repair, and delete demo | `-Mode`, `-SkipFailure`, `-Keep`, `-ValueSize` |
| `scripts/test.ps1` / `test.sh` | Unit, integration, chaos, and bench suites in Docker | `-Integration`, `-Chaos`, `-Bench`, `-Keep` |
| `scripts/bench.ps1` / `bench.sh` | Load, storage, recovery, and CPU benchmarks in Docker | `-Runs`, `-Duration`, `-Mode`, `-SkipLoad`, `-SkipStorage`, `-SkipRecovery` |

Every script supports `-Help` / `--help`, resolves the repository root from its
own location, and prints the command it runs. `test` and `demo` start the cluster
only when needed and tear down only a cluster they started; pass `-Keep` /
`--keep` to leave it running.

The Compose project name is pinned to `distributed-systems`, so the network is
always `distributed-systems_cluster-net` and the volumes are named
`distributed-systems_node1-data` through `distributed-systems_node5-data` (the
runtime alias is `node1-data` ... `node5-data` inside the project). If you used
an earlier checkout with a different project name, run `docker volume ls` and
either rename the old volumes or start clean with `./scripts/reset.ps1`.

The gateway and bench tools live in the Compose `tools` profile. They are not
started by `up`; call them through `docker compose run --rm`, the scripts, or:

```powershell
docker compose run --rm gateway status
docker compose run --rm bench --help
```

If `AUTH_TOKEN` is set on the host, Compose passes it to all five nodes, the
gateway, the bench tool, and the integration test container. The scripts print
only `auth: enabled` or `auth: disabled` and never echo the token.


## Gateway CLI

The CLI is a one-shot Compose tool, not a long-running service. It never uses a
hardcoded node address: `GATEWAY_NODES` is required and maps node IDs to gRPC
addresses. For each key the consistent-hash ring picks an **entry node**; that
node forwards to the Raft leader, and the leader performs shard placement and
fan-out. Consistent hashing spreads client connections and gives the retry loop a
deterministic fallback order, not a different data layout.

```powershell
# Store with a per-request mode override
docker compose run --rm gateway put demo-key "hello" --mode rs32

# Read it back (value on stdout, forwarding metadata on stderr)
docker compose run --rm gateway get demo-key

# Toggle the cluster-wide default; no argument reads it
docker compose run --rm gateway mode replication
docker compose run --rm gateway mode

# Operator views
docker compose run --rm gateway status
docker compose run --rm gateway health
docker compose run --rm gateway stats
docker compose run --rm gateway repair demo-key --force
```

Use `--json` on any command for machine-readable output, `--version N` for a
compare-and-swap write or delete, and `--value-file PATH` for large values.
`health` prints one line and exits non-zero unless every voter is healthy, a
leader is known, and no committed stripe is degraded.

| Variable | Default | Meaning |
| --- | --- | --- |
| `GATEWAY_NODES` | required | `id=host:port,...`; no localhost fallback |
| `GATEWAY_VNODES` | `100` | Virtual nodes per host on the hash ring |
| `GATEWAY_TIMEOUT` | `15s` | Deadline for one RPC attempt |
| `GATEWAY_RETRIES` | `3` | Retries after the first attempt (`0` disables retries) |
| `AUTH_TOKEN` | empty | Optional bearer token; empty disables auth with a startup warning |
| `MAX_MESSAGE_SIZE` | `32 MiB` | Client message limit, matched to the server |

Retries use 100 ms, 250 ms, and 500 ms steps with ±20% jitter. An unavailable or
timed-out node falls through to the next candidate; an `Aborted` write
re-resolves the leader and tries again; validation, auth, and not-found errors
return immediately. `put` is last-write-wins unless `--version` is supplied;
`delete` is idempotent. When `AUTH_TOKEN` is set, the client and every
server-internal client send `authorization: Bearer <token>` through the shared
`internal/rpc` helper. Only `/grpc.health.v1.Health/Check` and
`/grpc.health.v1.Health/Watch` bypass authentication; see
[docs/SECURITY.md](docs/SECURITY.md).

## Benchmarks

Measure put/get latency, throughput, RS versus replication storage overhead,
recovery time, and per-node CPU in one Docker-only command:

```powershell
./scripts/bench.ps1 -Runs 1 -Duration 15s
```

Bash equivalent: `./scripts/bench.sh --runs 1 --duration 15s`. Results are
written under `bench-results/<run-id>/` as raw CSVs, `meta.json`, `run.json`,
and `summary.md`. Storage measurement resets the five data volumes so the ratio
starts clean; use `-SkipStorage` for a non-destructive run. See
[docs/BENCHMARKS.md](docs/BENCHMARKS.md) for methodology, flags, and the
expected 1.67x RS and 5.00x replication payload ratios.

## API

| RPC | Purpose |
| --- | --- |
| `Put(key, value, mode, expected_version)` | Linearizable metadata write; followers forward to the leader |
| `Get(key)` | Linearizable read; followers forward |
| `Delete(key, expected_version)` | Idempotent metadata delete plus fragment GC; followers forward |
| `GetConfig()` / `SetConfig(default_mode)` | Read or commit the cluster-wide default mode through Raft |
| `ClusterHealth()` | Leader, mode, degraded state, and a probe of every voter |
| `Stats()` | Local storage, fragment-byte, and repair counters; any node may answer |
| `ForceSnapshot()` | Force a local Raft snapshot; benchmark-only, never forwards |
| `CheckpointStore()` | Truncate the local SQLite WAL and return the remaining size |
| `Heartbeat(node_id)` | Liveness probe kept for compatibility |
| `Join(node_id, raft_addr, grpc_addr)` | Ask the leader to add a voter; followers return a leader hint |
| `NodeStatus()` | Node ID, Raft state, leader, term, commit/applied/last index |
| `UploadFragment(stream)` / `DownloadFragment` | Chunk transport for the redundancy work |
| `Repair(key, stripe_id, force)` | Enqueue shard reconstruction on the leader |

`mode` accepts `""` (default), `"replication"`, or `"rs32"`; anything else is
rejected before it reaches Raft. Writes encode the value with the selected
engine, push five shards or copies out of band, then commit metadata-only state
through Raft. Reads gather enough verified shards to reconstruct the value. See
[internal/redundancy/README.md](internal/redundancy/README.md).

## Tests

Use the script runner so the cluster lifecycle and network name are handled for
you:

```powershell
./scripts/test.ps1                 # unit and component tests in Docker
./scripts/test.ps1 -Integration    # starts the cluster, waits, runs integration
./scripts/test.ps1 -Chaos          # chaos-tagged failure suite
./scripts/test.ps1 -Bench          # bench-tagged harness tests
./scripts/test.ps1 -Integration -Keep
```

Bash: `./scripts/test.sh --unit`, `--integration`, `--chaos`, `--bench`, `--keep`.

The underlying commands are still available. The fast suite covers unit tests
and the in-process five-node integration suite, so no cluster is needed:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race -count=1 ./...
```

The chaos suite runs in-process and is tagged:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 `
  go test -race -tags=chaos -count=1 ./tests/chaos/...
```

Compose-backed checks (the script starts and waits for the cluster):

```powershell
docker compose up -d --build
docker run --rm --network distributed-systems_cluster-net -e AUTH_TOKEN `
  -v ${PWD}:/app -w /app golang:1.22 `
  go test -tags=integration -count=1 ./tests/integration/...
```

`GATEWAY_NODES` may override the node addresses used by
`tests/integration/cluster_test.go`; when it is unset the five Compose defaults
are used. Only Compose-dependent files carry the `integration` tag, so the
default test command stays green without Docker Compose running. The layout,
build tags, shared harness, coverage matrix, and white-box exceptions are in
[tests/README.md](tests/README.md); the full guide is in
[docs/TESTING.md](docs/TESTING.md). Run `./scripts/check.ps1` (or
`./scripts/check.sh`) for the full local build, vet, test, chaos, and format
check suite; it runs entirely in Docker and needs no remote CI.

## Persistence

Each node stores three things under `DATA_DIR`:

```
/data/
├── node.db        # SQLite: metadata tables, local fragments, legacy KV rows
├── node.db-wal    # SQLite write-ahead log (while the node is running)
├── raft.db        # BoltDB: Raft log + stable state (term, vote, configuration)
└── snapshots/     # up to three FSM snapshots written by Raft
```

SQLite is opened before Raft starts and closed cleanly on SIGINT/SIGTERM; the
close checkpoints and releases the WAL. `Snapshot`/`Restore` currently cover the
legacy KV table used by the transitional write path, while
`SnapshotMetadata`/`RestoreMetadata` already implement the metadata-only format
for the next tasks. Fragment payloads are never included in metadata snapshots.

`docker compose down` (without `-v`) keeps the named volumes, so a cluster that
comes back up reopens `node.db`, replays its Raft log, and/or restores its latest
snapshot. Deleting the volumes resets the node completely. To reset only SQLite,
stop the node and remove `node.db` and its `-wal`/`-shm` files.

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `scripts/up.ps1` or `up.sh` times out | Run `docker compose ps` and `docker compose logs --tail 100 node1`; check for port conflicts and give startup more time with `-Timeout` / `--timeout` |
| `docker compose (v2) is unavailable` | Install the Compose plugin; scripts require `docker compose`, not `docker-compose` |
| Integration tests cannot resolve `node1` | Use `./scripts/test.ps1 -Integration` or run them on `distributed-systems_cluster-net` |
| Data disappeared after changing the checkout directory name | The project name is pinned; inspect `docker volume ls` and migrate or reset |
| `create bolt store: timeout` at startup | Another process still holds `/data/raft.db`; stop the old container before starting a new one |
| No leader, election loops | `BOOTSTRAP_SERVERS` must list the whole cluster and be identical on the bootstrapping node; only one node may set `BOOTSTRAP=true` |
| Writes return "not the leader" | `PEERS` is missing or has the wrong gRPC addresses, so the follower cannot forward |
| Followers never catch up | Check Raft connectivity on ports 60051-60055 and `docker compose logs nodeN` |
| Health check keeps failing | The process is not serving on the gRPC port; inspect container logs |
| `AUTH_TOKEN` errors after changing the host value | Restart the whole cluster and tools so every container sees the same token |

## Roadmap

- Complete the documentation set and requirements mapping.
- Final integration cleanup and release rehearsal.
- Add certificate provisioning for the optional TLS mode (future work).
