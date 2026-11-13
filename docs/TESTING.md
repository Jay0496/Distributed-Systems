# Testing Guide

The suite has four layers: fast unit tests, in-process integration tests,
Compose-backed integration checks, and destructive chaos scenarios. All commands
run from the repository root inside Docker; Go is not required on the host.

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 <command>
```

On macOS/Linux replace `${PWD}` with `$(pwd)`. The layout, build tags, and
white-box exception list live in [tests/README.md](../tests/README.md).

## Quick commands

| Goal | Command |
| --- | --- |
| Unit + in-process integration | `go test -race -count=1 ./...` |
| Unit only | `go test -race -count=1 ./tests/unit/...` |
| In-process integration only | `go test -race -count=1 ./tests/integration/...` |
| Chaos | `go test -race -tags=chaos -count=1 ./tests/chaos/...` |
| Bench harness checks | `go test -race -tags=bench -count=1 ./tests/bench/...` |
| Compose checks | `go test -tags=integration -count=1 ./tests/integration/...` |
| Flakiness check | `go test -race -count=3 ./tests/...` |
| Full local check suite | `./scripts/check.ps1` or `./scripts/check.sh` |
| Documentation check | `./scripts/check-docs.ps1` or `./scripts/check-docs.sh` |

The script runner wraps cluster startup, health waiting, the pinned network name,
and `AUTH_TOKEN`:

```powershell
./scripts/test.ps1                 # fast suite
./scripts/test.ps1 -Integration    # starts Compose when needed
./scripts/test.ps1 -Chaos          # chaos suite
./scripts/test.ps1 -Bench          # bench-tagged harness checks
./scripts/test.ps1 -Integration -Keep
```

```bash
./scripts/test.sh                  # fast suite
./scripts/test.sh --integration
./scripts/test.sh --chaos
./scripts/test.sh --bench
```

`test` starts the cluster only when fewer than five nodes are healthy and tears
down only a cluster it started. Integration containers use
`distributed-systems_cluster-net`; scripts pass `AUTH_TOKEN` without printing it.

Static checks:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go build ./...
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go vet ./...
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go vet -tags=integration ./...
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go vet -tags=chaos ./...
```

Use `./scripts/check.ps1` or `./scripts/check.sh` for the format check; a raw
`gofmt -l .` reports CRLF false positives on a Windows checkout.
`go vet ./...` skips files excluded by build tags, so the tagged vet commands are
part of the acceptance run.

## Build tags

- **No tag:** unit tests and the in-process five-node integration suite. These
  run on a clean checkout with no cluster.
- **`integration`:** Compose-backed checks such as
  `tests/integration/cluster_test.go`. Never required by `go test ./...`.
- **`chaos`:** destructive multi-node failure, partition, restart, and repair
  scenarios.
- **`bench`:** benchmark harness checks that measure storage ratios and run load
  against the in-process cluster.

Rule: `go test ./...` must pass with no Docker Compose deployment running.

## Shared harness

`tests/support` owns every reusable double and harness:

| File | Purpose |
| --- | --- |
| `store.go` | in-memory `storage.Store` with metadata, fragment, config, and stats methods |
| `cluster.go` | five-node Raft + SQLite + bufconn deployment; stop/start, traffic drops, auth |
| `cluster_helpers.go` | leader wait, raw apply, Get helpers |
| `listener.go` | controllable bufconn listener close/reopen |
| `dialer.go` | registry-backed fake gRPC dialer with optional token |
| `probe.go` | controllable health prober for detector tests |
| `clock.go` | manually advanced clock for detector and orphan tests |
| `fixtures.go` | deterministic seed, keys, and values |

`StartCluster` uses one `raft.NewInmemTransport` per node connected as a mesh,
one temp SQLite file per node, and the production gRPC handlers on `bufconn`.
Cleanup closes gRPC, Raft, SQLite, connections, and listeners. `WaitFor`
converges on observable state without a fixed sleep. `StopNode` closes a node's
gRPC/Raft/SQLite but keeps its data; `StartNode` reopens the same directory.
`SetUnreachable`, `Partition`, and `DropTraffic` inject transport failures
without stopping Raft.

`StartClusterWithOptions(ClusterOptions{AuthToken: token})` installs the
production bearer-token interceptors and attaches the token to harness clients.
`DialRaw` and `DialerWithToken` exercise missing and wrong credentials.

## Coverage map

| Area | Primary tests |
| --- | --- |
| Config and gateway config parsing | `tests/unit/config` |
| SQLite schema, repository, snapshots, sentinel errors | `tests/unit/storage`, `internal/storage/sqlite_test.go` |
| RS(3,2), replication, placement, storage math | `tests/unit/redundancy` |
| Metadata command validation and JSON guards | `tests/unit/consensus`, `internal/consensus/command_test.go` |
| FSM apply, idempotency, snapshots | `internal/consensus/fsm_test.go`, `snapshot_test.go` |
| Raft election, failover, log replay, SQLite snapshot | `internal/consensus/raft_test.go` |
| Detector hysteresis and fake clock | `tests/unit/cluster` |
| Queue priorities, dedupe, overflow, retries | `tests/unit/cluster` |
| Orphan grace period and guards | `tests/unit/cluster`, chaos |
| Repair and rebalance | `internal/cluster/repairer_test.go`, integration, chaos |
| Auth interceptors, health exemption, token propagation | `tests/unit/server`, `tests/unit/rpc`, `tests/integration/auth_test.go` |
| Fragment stream, stat, list, delete | `tests/integration/fragments_test.go`, `internal/server/fragments_test.go` |
| Write path, degraded writes, GC | `tests/integration/write_path_test.go` |
| Read path, fallback, read repair | `tests/integration/read_path_test.go` |
| Config/status/delete/stats/health | `tests/integration/admin_test.go` |
| Gateway CLI, routing, retry, token | `tests/unit/gateway`, `tests/integration/gateway_test.go` |
| Persistence, snapshot restore, repair | `tests/integration/persistence_test.go` |
| Concurrent clients | `tests/integration/concurrent_test.go` |
| Failure, partition, restart | `tests/chaos` |
| Compose cluster | `tests/integration/cluster_test.go` |

## Unit and in-process integration

```powershell
# Everything fast, race detector on
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race -count=1 ./...

# One package, verbose
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race -v ./tests/unit/cluster

# One test
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race -run TestDetectorHysteresisWithFakeClock ./tests/unit/cluster
```

The SQLite-backed suites use `t.TempDir()`; they never touch `/data`:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race ./tests/unit/storage ./internal/storage
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race ./internal/consensus -run TestRaftSQLiteSnapshotRestore -v
```

### How the in-process cluster works

`tests/support.StartCluster` builds:

1. five meshed in-memory Raft transports;
2. five real SQLite stores under `t.TempDir()`;
3. five gRPC servers on `bufconn` listeners with the production handlers;
4. an injected dial function mapping node IDs to current listeners.

`StopNode` closes the listener and Raft instance but keeps SQLite and the Raft
log on disk. `StartNode` reopens them. `SetUnreachable` drops cached dials so a
connection opened before an injected failure cannot bypass it.

Auth tests start the same harness with a token and use raw dials to prove the
missing/wrong-token and health-exemption paths.

### Read-path and repair tests

`tests/integration/read_path_test.go` seeds real stripes, makes two owners
unreachable, corrupts a shard with a matching local checksum, and waits for read
repair to restore the committed shard. `internal/server/read_test.go` exercises
gather/decode/verify and queue saturation with a fake apply seam.

### Fragment transport tests

`tests/integration/fragments_test.go` drives the real streaming API:
multi-chunk upload, download reassembly, stat, list, checksum rejection, delete,
and per-stripe deletion. `internal/server/fragments_test.go` covers the lower
level state machine: gaps, overlap, duplicate offsets, idempotent retry,
`AlreadyExists` versus replace, size limits, cancellation, and concurrency.

## Compose-backed integration

```powershell
./scripts/test.ps1 -Integration
```

Raw equivalent:

```powershell
docker compose up -d --build
docker compose ps                      # five healthy nodes

docker run --rm --network distributed-systems_cluster-net -e AUTH_TOKEN `
  -v ${PWD}:/app -w /app golang:1.22 `
  go test -tags=integration -v -count=1 ./tests/integration/...
```

Prerequisites:

- all five containers healthy (the healthcheck calls `grpc.health.v1`);
- exactly one leader; tests fail fast if the cluster cannot agree;
- run from the repository root so the volume mount is correct;
- the network name is pinned to `distributed-systems_cluster-net`;
- when `AUTH_TOKEN` is set, the test container receives the same value.

The Compose file covers heartbeat, leader agreement, and write-through-follower.
`GATEWAY_NODES` can point the same tests at a renamed project or host mapping.

## Chaos suite

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 `
  go test -race -tags=chaos -count=1 ./tests/chaos/...
```

The suite runs in-process, not against Compose, and uses short detector
intervals plus `WaitFor` state convergence. It covers:

- one and two unreachable nodes while reads and writes continue;
- a degraded write with two owners missing, then repair to five shards;
- losing quorum with three nodes stopped, with explicit failures;
- leader kill and re-election, then old-leader rejoin;
- full and one-way partitions, health degradation, and recovery;
- node stop/restart with missing fragments and automatic refill;
- corrupt-shard replacement through forced `Repair`;
- metadata-only restore followed by repair;
- orphan grace period on a real SQLite store.

No fixed sleeps are used for synchronization; polling helpers and `WaitFor`
replace them. Deterministic payloads use `support.DefaultSeed`.

## Determinism rules

1. Seed randomized data from `support.DefaultSeed` and log the seed on failure.
2. Poll for state (`WaitFor`) instead of sleeping.
3. Use `support.FakeClock` for detector and orphan timestamps.
4. Close every node, store, queue, detector, listener, and connection in
   `t.Cleanup`.
5. Use unique keys for tests that mutate the live Compose cluster.
6. Run with `-race`; the suite is race-clean.

## Adding a unit test

1. Prefer `tests/unit/<package>` and exported constructors.
2. If the test needs unexported internals, keep it next to the code and add it
   to the exception table in [tests/README.md](../tests/README.md).
3. Do not export internals just to relocate a test.
4. Use `t.TempDir()` for any data directory.
5. Always close Raft nodes and SQLite stores so BoltDB and WAL locks are
   released.
6. Prefer table-driven validation cases.

## Manual verification checklist

Automated tests do not cover every operator workflow. Before a demo:

```powershell
./scripts/up.ps1
./scripts/demo.ps1
./scripts/demo.ps1 -Mode replication -SkipFailure

docker compose stop node4 node5
docker compose run --rm gateway status
docker compose run --rm gateway get demo-key
docker compose start node4 node5

docker compose restart node1
./scripts/down.ps1            # keeps volumes
./scripts/up.ps1
./scripts/reset.ps1           # destructive
```

After `up`, confirm one leader, five healthy nodes, and that previously written
keys survive the restart.

## Regenerating protobuf code

Generated files are committed and pinned to `protoc` v24.4,
`protoc-gen-go` v1.33.0, and `protoc-gen-go-grpc` v1.3.0:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 bash -c '
  apt-get update -qq && apt-get install -y -qq curl unzip >/dev/null &&
  curl -sSLo /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v24.4/protoc-24.4-linux-x86_64.zip &&
  unzip -q /tmp/protoc.zip -d /usr/local && chmod +x /usr/local/bin/protoc &&
  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0 &&
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0 &&
  PATH=$PATH:/root/go/bin protoc -I . --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/storage.proto'
```

Do not hand-edit `proto/storage.pb.go` or `proto/storage_grpc.pb.go`; keep proto
changes additive.

## Local check suite

The project does not use a GitHub-hosted workflow. GitHub Actions is free for
public repositories, but an account-level billing lock prevents jobs from
starting, so all checks run locally in Docker:

```powershell
./scripts/check.ps1
./scripts/check.ps1 -SkipChaos
```

```bash
./scripts/check.sh
./scripts/check.sh --skip-chaos
```

The checks are build, the three vet tag sets, the fast race suite, the chaos
suite, and a `gofmt` check. The format check copies the working tree into the
container and normalizes CRLF first, so a Windows checkout does not report every
untouched file. CGO is required for `mattn/go-sqlite3`; the `golang` image
includes `gcc`, so do not disable it. The local checks do not start Compose.

## Documentation checks

`scripts/check-docs.ps1` and `check-docs.sh` are the guard against stale docs.
They do not run tests or start Docker:

```powershell
./scripts/check-docs.ps1
```

```bash
./scripts/check-docs.sh
```

They scan every Markdown file outside `.git` and `.opencode` and:

1. resolve every relative Markdown link (fragments are ignored, external URLs
   are skipped);
2. print each fenced `powershell` or `bash` block tagged with
   `<!-- runnable -->`, so the acceptance run can execute them manually;
3. fail on removed constructor names, the old single-file entrypoint command,
   the retired router constructor, unfinished-work markers, and the obsolete
   legacy table name.

The check is deterministic and intentionally small. It reports `file:line` for
each problem and exits non-zero. Run it before committing documentation changes
and as part of the Task 14 acceptance run. On Windows, the PowerShell script is
first-class; the bash script assumes an LF checkout.

## Known costs and troubleshooting

- `TestRaftLeaderFailover` takes about 5 seconds because it waits for a real
  election.
- The race detector roughly doubles the runtime of the consensus and
  integration suites.
- If a test hangs with `create bolt store: timeout`, a previous Raft node was
  not closed and still holds `raft.db`.
- `database is locked` means a SQLite store was not closed before restart.
- `node1` failing to resolve means the test was not run on
  `distributed-systems_cluster-net`.
- `gcc not found` means the container lacks build tools; keep CGO enabled.
- If the local format check reports files, run `gofmt -w` on them. On a Windows
  checkout, use `scripts/check.ps1` rather than a raw `gofmt -l .`: the script
  normalizes CRLF before checking.
