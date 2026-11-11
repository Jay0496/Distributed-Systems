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
| CLI / routing gateway | Khalid | In progress (`cmd/gateway/`, `internal/gateway/`) |
| SQLite persistence | Parth | Complete for this task (`internal/storage/`, `node.db`) |
| Failure management / Reed-Solomon redundancy | Dhruvin | Interface scaffolded (`internal/redundancy`, `internal/server/fragments.go`) |

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
| `cmd/gateway/` | CLI entrypoint: argument parsing and gRPC client calls |
| `cmd/healthcheck/` | Tiny binary used by Docker Compose health checks |
| `cmd/bench/` | Benchmark harness placeholder |
| `internal/config/` | Environment configuration parsing |
| `internal/server/` | gRPC handlers: `Put`, `Get`, `Heartbeat`, `Join`, `NodeStatus`, fragments, repair |
| `internal/server/forward.go` | Leader discovery, cached client connections, one-hop forwarding, retry and loop guard |
| `internal/server/fragments.go` | `UploadFragment`, `DownloadFragment`, `Repair` handlers |
| `internal/redundancy/` | `RedundancyEngine` seam (no-op today) and redundancy-mode validation |
| `internal/consensus/raft.go` | Raft node: BoltDB log/stable store, file snapshots, bootstrap, `AddVoter`, `Close` |
| `internal/consensus/fsm.go` | FSM: put/get commands, real snapshot and restore |
| `internal/storage/` | `Store` interface, SQLite metadata/fragment/config repositories, snapshots |
| `internal/gateway/` | Consistent-hash router |
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

## Quick start

```powershell
docker compose up --build -d
docker compose ps
```

All five containers should report `healthy`. Check the cluster:

```powershell
docker compose logs node1 | Select-String "entering leader state"
```

Kill the cluster:

```powershell
docker compose down        # keeps data
docker compose down -v     # deletes data volumes
```

## API

| RPC | Purpose |
| --- | --- |
| `Put(key, value, mode)` | Linearizable write; followers forward to the leader |
| `Get(key)` | Linearizable read from the committed log; followers forward |
| `Heartbeat(node_id)` | Liveness probe kept for compatibility |
| `Join(node_id, raft_addr, grpc_addr)` | Ask the leader to add a voter; followers return a leader hint |
| `NodeStatus()` | Node ID, Raft state, leader, term, commit/applied/last index |
| `UploadFragment(stream)` / `DownloadFragment` | Chunk transport for the redundancy work |
| `Repair(key)` | Scaffold for failure-management reconstruction |

`mode` accepts `""` (default), `"replication"`, or `"rs32"`; anything else is
rejected before it reaches Raft. `"rs32"` currently behaves like replication
until the redundancy engine lands.

## Tests

Unit tests (no cluster needed):

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race ./...
```

Integration tests (require a running Compose cluster):

```powershell
docker compose up -d --build
docker run --rm --network distributed-systems_cluster-net -v ${PWD}:/app -w /app golang:1.22 `
  go test -tags=integration -v ./tests/...
```

The integration package is excluded from `go test ./...` by a build tag, so the
default test command stays green without Docker Compose running. Details, the
target layout, and the white-box exceptions are in
[tests/README.md](tests/README.md); the full test matrix is in
[docs/TESTING.md](docs/TESTING.md).

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
| Integration tests cannot resolve `node1` | Run them on `distributed-systems_cluster-net`: `docker run --rm --network distributed-systems_cluster-net ...` |
| `create bolt store: timeout` at startup | Another process still holds `/data/raft.db`; stop the old container before starting a new one |
| No leader, election loops | `BOOTSTRAP_SERVERS` must list the whole cluster and be identical on the bootstrapping node; only one node may set `BOOTSTRAP=true` |
| Writes return "not the leader" | `PEERS` is missing or has the wrong gRPC addresses, so the follower cannot forward |
| Followers never catch up | Check Raft connectivity on ports 60051-60055 and `docker compose logs nodeN` |
| Health check keeps failing | The process is not serving on the gRPC port; inspect container logs |

## Roadmap

- Wire the CLI/gateway routing layer to the gRPC API.
- Move the Raft log to metadata-only commands and integrate the fragment write/read path.
- Implement Reed-Solomon `rs32` encoding in `internal/redundancy` and real repair in `internal/server/fragments.go`.
- Add certificate provisioning for the optional TLS mode.
