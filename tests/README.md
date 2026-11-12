# Test Layout and Runbook

This directory is the first-class test suite. Production code lives under
`cmd/` and `internal/`; tests that can use exported APIs live here. Tests that
genuinely need unexported internals stay next to the code and are listed as
exceptions below.

The repository is Docker-first. No host Go installation is required and no
paid CI service is used. `scripts/check.ps1` / `scripts/check.sh` run the full
local check suite in Docker.

## Layout

```text
tests/
  README.md
  support/      shared doubles and harnesses
    store.go            in-memory storage.Store
    cluster.go          five-node in-process deployment
    cluster_helpers.go  leader/Get/Apply helpers
    listener.go         controllable bufconn lifecycle
    dialer.go           registry-backed fake gRPC dialer
    probe.go            controllable health prober
    clock.go            manually advanced clock
    fixtures.go         deterministic seeds, keys, and values
    doc.go
  unit/         black-box unit tests, one directory per package under test
    cluster/ consensus/ config/ gateway/ proto/ redundancy/ rpc/ server/ storage/
  integration/  in-process multi-node suites plus Compose-backed checks
    cluster_test.go     Compose cluster (build tag `integration`)
    write_path_test.go  read_path_test.go  admin_test.go  metadata_test.go
    gateway_test.go     auth_test.go        fragments_test.go
    persistence_test.go concurrent_test.go
  chaos/        destructive failure, partition, and restart scenarios (tag `chaos`)
    failure_test.go     partition_test.go   restart_test.go   helpers_test.go
  bench/        benchmark harness placeholder until task 13
```

Everything under `tests/` is inside the module, so it may import
`distributed-kv-store/internal/...`. Production packages must never import
`tests/...`; that rule is enforced by review and by the package boundary.

The in-process harness uses real Raft over `raft.NewInmemTransport`, real SQLite
files under `t.TempDir()`, and the production gRPC handlers on `bufconn`
listeners. `StartCluster` waits for a leader and registers cleanup that closes
gRPC, Raft, SQLite, and listeners.

`StartClusterWithOptions(ClusterOptions{AuthToken: "..."})` additionally installs
the production bearer-token interceptors on every node and attaches the token to
the harness's client and internal dials. `DialRaw` and `DialerWithToken` exercise
missing and wrong credentials.

## Build tags

| Tag | Scope | Included by `go test ./...` |
| --- | --- | --- |
| none | unit tests and the in-process integration suite | yes |
| `integration` | Compose-backed tests that need a live five-node deployment | no |
| `chaos` | destructive multi-node failure simulation | no |
| `bench` | benchmark-only helpers and result fixtures | no |

`go test ./...` must stay green on a clean checkout with no cluster running.
Anything that needs Docker Compose carries `-tags=integration`.

`go vet ./...` does not compile files excluded by a build tag, so run
`go vet -tags=integration ./...` and `go vet -tags=chaos ./...` as well.

## Commands

All commands run from the repository root. On Windows PowerShell, `${PWD}`
expands to the checkout; on macOS/Linux use `"$(pwd)"`.

```powershell
# Fast suite: unit plus in-process integration, race detector on
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race -count=1 ./...

# Fast suite three times to catch flakiness
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race -count=3 ./tests/...

# Unit only
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race -count=1 ./tests/unit/...

# In-process integration only (no Compose)
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race -count=1 ./tests/integration/...

# Static checks
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go build ./...
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go vet ./...
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go vet -tags=integration ./...
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go vet -tags=chaos ./...

# Everything above plus tests, chaos, and a CRLF-safe gofmt check
./scripts/check.ps1
./scripts/check.sh
```

The script runner wraps cluster lifecycle and auth:

```powershell
./scripts/check.ps1                # local build/vet/test/format checks
./scripts/check.ps1 -SkipChaos     # faster local check
./scripts/test.ps1                 # fast suite
./scripts/test.ps1 -Integration    # starts Compose, runs tagged checks
./scripts/test.ps1 -Chaos          # chaos-tagged suite
./scripts/test.ps1 -Bench          # benchmark placeholder
```

Bash equivalents are `./scripts/test.sh --unit`, `--integration`, `--chaos`,
`--bench`, and `--keep`.

### Compose-backed integration

```powershell
docker compose up -d --build
docker compose ps                      # wait for five healthy nodes

docker run --rm --network distributed-systems_cluster-net -e AUTH_TOKEN `
  -v "${PWD}:/app" -w /app golang:1.22 `
  go test -tags=integration -v -count=1 ./tests/integration/...
```

`tests/integration/cluster_test.go` reads `GATEWAY_NODES` when set and otherwise
uses `node1:50051` through `node5:50055`. The Compose project name is pinned to
`distributed-systems`; run the test container on
`distributed-systems_cluster-net`. When `AUTH_TOKEN` is set on the host, pass the
same value to the test container.

Run only the Compose checks with
`-run 'TestClusterHeartbeat|TestClusterLeaderAgreement|TestClusterWriteThroughFollower'`.

### Chaos

```powershell
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 `
  go test -race -tags=chaos -count=1 ./tests/chaos/...
```

The chaos suite is in-process, deterministic, and does not need Compose. It
starts five nodes with a leader-gated detector, short hysteresis thresholds, and
the production repairer. It uses `WaitFor` state convergence rather than fixed
sleeps. Covered scenarios:

- one unreachable node, then recovery;
- two unreachable RS owners, degraded write, repair back to five shards;
- three stopped nodes: writes and reads fail explicitly without quorum;
- leader kill and re-election, then old-leader rejoin;
- full and one-way partition degradation and recovery;
- stop/restart with local fragment loss and automatic refill;
- corrupt-shard replacement through the forced `Repair` RPC;
- metadata-only restore followed by repair;
- orphan grace period on a real SQLite store.

## Coverage matrix

| Area | Unit | Integration | Chaos |
| --- | --- | --- | --- |
| Config parsing | `tests/unit/config` | `tests/integration/auth_test.go` gateway auth | — |
| SQLite schema, CRUD, snapshots | `tests/unit/storage`, `internal/storage/sqlite_test.go` | `persistence_test.go` | — |
| RS(3,2) encode/decode, loss, corruption | `tests/unit/redundancy` | read/write paths | corrupt-shard repair |
| Replication engine | `tests/unit/redundancy` | write/read paths | — |
| Placement determinism | `tests/unit/redundancy` | write path | — |
| Metadata commands, idempotency | `tests/unit/consensus`, `internal/consensus/fsm_test.go` | `metadata_test.go` | — |
| Metadata-only snapshot restore | `internal/consensus/snapshot_test.go` | `persistence_test.go` | `TestMetadataOnlySnapshotRestoreRefillsFragments` |
| Snapshot restore plus fragment repair | — | `persistence_test.go` | `restart_test.go` |
| Orphan grace period and in-flight writes | `tests/unit/cluster` | — | `TestOrphanSweepRespectsGracePeriod` |
| Fragment transport | — | `fragments_test.go` | — |
| Write path quorum, GC, degraded | `internal/server/write_test.go` | `write_path_test.go` | two failures after healthy write |
| Read path fallback and repair | `internal/server/read_test.go` | `read_path_test.go` | two failures after repair |
| Config/status/delete/stats | `internal/server/admin_test.go` | `admin_test.go` | health recovery |
| Auth (server, gateway, internal propagation, health exemption) | `tests/unit/server`, `tests/unit/rpc` | `auth_test.go` | — |
| Gateway routing and retries | `tests/unit/gateway` | `gateway_test.go` | leader change via failure tests |
| Detector hysteresis | `tests/unit/cluster` (fake clock) | — | partition |
| Repair and rebalance | `internal/cluster/repairer_test.go` | `persistence_test.go` | restart/partition |
| Concurrent clients | `tests/unit/cluster` queue races | `concurrent_test.go` (50 pairs) | — |

## White-box exceptions

These tests stay next to their package because they use unexported constructors,
seams, or schema helpers. Do not export internals just to move a test. Every new
exception must be added to this table with the reason.

| File | Why it stays |
| --- | --- |
| `internal/server/server_test.go` | `forwardedMetadataKey`, the `dialFunc` seam, fragment key helpers, and in-process wiring |
| `internal/server/write_test.go` | write coordinator, apply seam, version allocation, commit/GC ordering |
| `internal/server/read_test.go` | read gatherer and repair queue internals, including queue saturation |
| `internal/server/fragment_push_test.go` | concurrent upload and local-owner fast path |
| `internal/server/fragments_test.go` | streaming upload state machine and staging internals |
| `internal/server/admin_test.go` | config/delete/stats/health handler seams and `newTestCluster` |
| `internal/server/auth_test.go` | `dialFuncFor`, `newTestStore`, and fragment fetch/push internals |
| `internal/server/repair_test.go` | repair handler queue seam |
| `internal/server/forward_test.go` | loop-guard and forwarding internals |
| `internal/server/teststore_test.go` | package-local `storage.Store` double |
| `internal/consensus/command_test.go` | `metadataMaxBytes` and `validateMetadataSize` |
| `internal/consensus/fsm_test.go` | FSM apply/snapshot helpers and `metadataMaxBytes` size guard |
| `internal/consensus/snapshot_test.go` | versioned snapshot envelope and compatibility internals |
| `internal/consensus/raft_test.go` | in-memory Raft fixture and restart internals |
| `internal/consensus/sqlite_test.go` | SQLite snapshot restore and package fixture |
| `internal/consensus/teststore_test.go` | package-local `storage.Store` double |
| `internal/storage/sqlite_test.go` | `db` handle and the private schema/helpers |
| `internal/gateway/router_test.go` | `Router.nodes`, `Router.ring`, `lookup`, and virtual-node fields |
| `internal/gateway/client_test.go` | retry backoff, sleep seam, ring candidate ordering, and `moveFirst` |
| `internal/cluster/repairer_test.go` | `enqueuePlacedOn`, `enqueueDegraded`, and placement internals |

`tests/unit/config`, `tests/unit/redundancy`, `tests/unit/storage`,
`tests/unit/proto`, `tests/unit/consensus`, `tests/unit/cluster`,
`tests/unit/gateway`, `tests/unit/rpc`, and `tests/unit/server` are black box:
they import only exported APIs and `tests/support` leaf helpers.

### Import-cycle rule

A package-internal test (`package foo`) cannot import `tests/support` when that
support package imports `foo`; the result is an import cycle. Use a test-local
double, or a leaf support package such as `tests/support/memstore`, which has no
dependency on the package under test. External tests (`package foo_test`) may
import `tests/support` freely.

## Determinism and cleanup

- Fixed seeds come from `support.DefaultSeed`; `support.LogSeed` prints the seed
  on failure. `support.RandomValue`, `support.Key`, and `support.Keys` generate
  reproducible fixtures.
- Use `support.WaitFor` or a local polling helper for convergence. Do not add a
  fixed `time.Sleep` to make a test pass; fake clocks belong in detector and
  orphan-sweep tests.
- Every node, Raft instance, SQLite store, listener, connection, queue, and
  detector is closed in `t.Cleanup`. A leaked `raft.db` lock makes the next
  restart hang.
- Tests create unique keys, so repeated runs do not collide.

## Local checks

There is no GitHub-hosted workflow. GitHub Actions is normally free for public
repositories, but an account-level billing lock prevents jobs from starting, so
the project keeps its checks local and free:

```powershell
./scripts/check.ps1
./scripts/check.ps1 -SkipChaos
```

```bash
./scripts/check.sh
./scripts/check.sh --skip-chaos
```

The scripts run in `golang:1.22` Docker and perform:

1. build;
2. vet the default, `integration`, and `chaos` tag sets;
3. fast tests with `-race`;
4. chaos tests with `-race`;
5. a `gofmt` check over a CRLF-normalized copy of the working tree, so Windows
   checkouts do not produce false failures.

CGO and `gcc` are included in the `golang` image; do not disable CGO for SQLite
tests. The scripts do not start Compose. Use `scripts/test.ps1 -Integration`
when a live cluster is available.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `go test ./...` tries to reach `node1` | You added a file without the `integration` tag or ran the Compose test without a cluster. In-process tests must not dial fixed hostnames. |
| Integration test cannot resolve `node1` | Run the container on `distributed-systems_cluster-net` or use `./scripts/test.ps1 -Integration`. |
| `create bolt store: timeout` | A previous run left Raft running. Ensure `Node.Close()`/`Cluster` cleanup ran; delete the temp directory or stop the old container. |
| `database is locked` | A SQLite store was not closed before restart. Always close stores in `t.Cleanup`; the harness does this. |
| `cgo: C compiler "gcc" not found` | Install `gcc` in the container/host; do not set `CGO_ENABLED=0` for SQLite tests. |
| Auth tests fail with `Unauthenticated` | Pass the same `AUTH_TOKEN` to the test container and the cluster, or use the in-process auth harness. |
| Local format check fails | Run `gofmt -w` on the files it lists; generated protobuf files are already formatted. |
