# Test Layout

This directory holds the test suite. Production code lives under `cmd/` and
`internal/`; tests that can use exported APIs live here. Tests that genuinely
need unexported internals stay next to the code as listed exceptions below.

## Target layout

```text
tests/
  support/      shared doubles and harnesses (in-memory store, cluster
                harness, fake dialers, fake detectors)
  unit/         black-box unit tests using exported seams
                (subdirectories named after the package under test)
  integration/  in-process multi-node and Compose integration tests
  chaos/        destructive Docker scenarios (stop/start/partition)
  bench/        benchmark harnesses and result fixtures
```

Everything under `tests/` is inside the module, so it may import
`distributed-kv-store/internal/...` because the tests are part of the same
application. Production packages must never import `tests/...`.

## Build tags

| Tag | Scope | Included by default |
| --- | --- | --- |
| none | unit and component tests | yes |
| `integration` | `tests/cluster_test.go` and `tests/integration` | no |
| `chaos` | `tests/chaos` | no |
| `bench` | benchmark-only files | no |

`go vet ./...` does not compile files excluded by build tags, so run
`go vet -tags=integration ./...` (and the other tags when they exist) as well.

## Commands

All commands run from the repository root inside Docker; no host Go is required.

```powershell
# Unit and component tests, race detector on
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race -count=1 ./...

# Static checks
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go build ./...
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go vet ./...
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go vet -tags=integration ./...

# Compose integration suite (cluster must be up)
docker compose up -d --build
docker run --rm --network distributed-systems_cluster-net -v "${PWD}:/app" -w /app golang:1.22 `
  go test -tags=integration -v -count=1 ./tests/...
```

## Current test map

| Location | Level | Package style | Notes |
| --- | --- | --- | --- |
| `tests/unit/config/` | unit | black box (`config_test`) | Env-driven config parsing |
| `tests/unit/storage/` | unit | black box (`storage_test`) | Schema, metadata, fragments, stats, snapshots, factory, support `MockStore` |
| `tests/unit/redundancy/` | unit | black box (`redundancy_test`) | RS(3,2) round trips, every two-shard loss, replication copies, corruption, placement, storage math |
| `tests/integration/write_path_test.go` | integration | black box (`integration`) | Five-node in-process write path over real SQLite; RS shards, replication copies, degraded writes, quorum failure, overwrite GC |
| `tests/support/harness.go` | support | harness | Five-node in-process cluster with controllable gRPC reachability |
| `tests/cluster_test.go` | integration | black box (`tests`) | Five-node Compose cluster; moves under `tests/integration` in task 12 |
| `internal/server/server_test.go` | component | white box (`server`) | In-process Raft + `bufconn`; forwarding, metadata writes, mode validation, status, health, join |
| `internal/server/write_test.go` | component | white box (`server`) | Validation, mode resolution, version allocation, upload quorum, commit outcomes, GC ordering |
| `internal/server/fragment_push_test.go` | component | white box (`server`) | Concurrent shard upload, local-owner fast path, unreachable owners, checksum rejection |
| `internal/consensus/fsm_test.go` | unit | white box (`consensus`) | FSM command handling and snapshots |
| `internal/consensus/raft_test.go` | component | white box (`consensus`) | In-memory Raft cluster fixture and SQLite snapshot restore |
| `internal/storage/sqlite_test.go` | unit | white box (`storage`) | SQLite persistence, parity flags, and concurrency |
| `internal/gateway/router_test.go` | unit | white box (`gateway`) | Inspects ring and virtual-node fields directly |

## White-box exceptions

These tests stay next to their package because they need unexported internals.
They are moved or rewritten in task 12; do not export internals just to relocate
a test.

| File | Unexported access |
| --- | --- |
| `internal/server/server_test.go` | `forwardedMetadataKey`, the `dialFunc` seam, and fragment key helpers |
| `internal/server/write_test.go` | Write coordinator internals: validation, version allocation, commit and GC seams |
| `internal/server/fragment_push_test.go` | Concurrent upload, placement, and local-owner fast path |
| `internal/server/teststore_test.go` | Package-local `storage.Store` double for server component tests |
| `internal/consensus/fsm_test.go` | FSM command and snapshot types |
| `internal/consensus/raft_test.go` | `Node` internals and the in-memory cluster fixture |
| `internal/consensus/teststore_test.go` | Package-local `storage.Store` double for consensus tests |
| `internal/storage/sqlite_test.go` | SQLite helper functions `InitDB`, `SaveChunk`, `GetChunk` |
| `internal/gateway/router_test.go` | `Router.nodes` and `Router.ring` |

`tests/unit/config` and `tests/unit/redundancy` are deliberately black box: they
exercise only exported seams (`config.Load`/`config.Config` and the redundancy
engines, shard, checksum, and placer APIs).

## Temporary production exceptions

- The legacy full-value `store` table remains until task 07 moves reads to
  metadata plus fragments; the write path stopped using it in task 06, and task
  15 removes it. The in-memory `MockStore` lives only under `tests/support` and
  is not imported by production packages.
