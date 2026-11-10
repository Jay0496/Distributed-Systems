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
| `tests/cluster_test.go` | integration | black box (`tests`) | Five-node Compose cluster; moves under `tests/integration` in task 12 |
| `internal/server/server_test.go` | component | white box (`server`) | In-process Raft + `bufconn`; uses unexported forwarding/fragment helpers |
| `internal/consensus/fsm_test.go` | unit | white box (`consensus`) | FSM command handling and snapshots |
| `internal/consensus/raft_test.go` | component | white box (`consensus`) | In-memory Raft cluster fixture |
| `internal/storage/mock_store_test.go` | unit | white box (`storage`) | In-memory store |
| `internal/storage/sqlite_test.go` | unit | white box (`storage`) | SQLite persistence and concurrency |
| `internal/gateway/router_test.go` | unit | white box (`gateway`) | Inspects ring and virtual-node fields directly |

## White-box exceptions

These tests stay next to their package because they need unexported internals.
They are moved or rewritten in task 12; do not export internals just to relocate
a test.

| File | Unexported access |
| --- | --- |
| `internal/server/server_test.go` | `forwardedMetadataKey`, the `dialFunc` seam, and fragment key helpers |
| `internal/consensus/fsm_test.go` | FSM command and snapshot types |
| `internal/consensus/raft_test.go` | `Node` internals and the in-memory cluster fixture |
| `internal/storage/mock_store_test.go` | `MockStore` mutation and defensive-copy behaviour |
| `internal/storage/sqlite_test.go` | SQLite helper functions `InitDB`, `SaveChunk`, `GetChunk` |
| `internal/gateway/router_test.go` | `Router.nodes` and `Router.ring` |

`tests/unit/config` is deliberately black box: it exercises only `config.Load`
and `config.Config`, which are the exported seam.

## Temporary production exceptions

- `internal/storage.MockStore` is still used by `cmd/server` and the FSM tests.
  It is a development double, clearly documented in the `storage` package doc,
  and is removed from production wiring by the SQLite production-store task.
- `internal/redundancy.NoopRedundancy` is a pass-through placeholder. It is
  replaced by the real Reed-Solomon/replication engine in the redundancy task.
