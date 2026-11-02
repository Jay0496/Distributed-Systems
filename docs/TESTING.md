# Testing Guide

All commands run inside Docker; Go is not required on the host. From the
repository root on Windows PowerShell:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 <command>
```

On macOS/Linux, replace `${PWD}` with `$(pwd)`.

## Test matrix

| File | Level | What it covers | Needs Compose? |
| --- | --- | --- | --- |
| `storage/mock_store_test.go` | unit | put/get, missing keys, defensive copying, snapshot/restore, 64 concurrent writers/readers | no |
| `consensus/fsm_test.go` | unit | put, get, missing key, bad JSON, unknown op, snapshot persistence and restore, stores without `Snapshotter` | no |
| `consensus/raft_test.go` | component | 3-node election and replication, leader failover, BoltDB log replay after restart, snapshot restore after restart | no (in-memory transports) |
| `server_test.go` | component | gRPC handlers over `bufconn`: leader writes, follower forwarding, linearizable reads, `NodeStatus` agreement, health, mode validation, fragments, repair, loop guard, `Join` | no (in-process Raft + bufconn) |
| `tests/cluster_test.go` | integration | five-node heartbeat, leader agreement, write through a follower, read from every node | yes (`-tags=integration`) |

The integration file carries `//go:build integration`, so the default
`go test ./...` is green even when no cluster is running.

## Unit and component tests

```powershell
# Everything, with the race detector
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race -count=1 ./...

# One package, verbose
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -race -v ./consensus/...

# One test
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go test -run TestRaftLeaderFailover ./consensus/
```

Static checks:

```powershell
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go build ./...
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go vet ./...
docker run --rm -v ${PWD}:/app -w /app golang:1.22 go vet -tags=integration ./...
docker run --rm -v ${PWD}:/app -w /app golang:1.22 gofmt -l .
```

`go vet -tags=integration` is important: `go vet ./...` does not compile files
excluded by build tags, so integration code can rot silently.

### How the in-process gRPC tests work

`server_test.go` builds a real three-node Raft cluster with
`raft.NewInmemTransport`, then puts a gRPC server in front of each node on a
`bufconn` listener. Each node is constructed with an injected dial function:

```go
srv := newServer(cfg, store, node, dial) // dial resolves "node2" to its bufconn listener
```

That injection is the reason `forward.go` takes a `dialFunc` rather than calling
`grpc.Dial` directly: forwarding is exercised without opening sockets, and the
tests stay fast and deterministic.

Useful helpers in that file:

| Helper | Purpose |
| --- | --- |
| `newTestCluster(t, n)` | Starts `n` meshed Raft nodes with gRPC front ends; registers cleanup |
| `waitLeader` / `waitFollower` | Wait for a stable role |
| `mustPut` | Asserts a successful write |
| `waitForValue` | Polls every node until all return the expected value |
| `ctxWithTimeout` | Context helper with `t.Helper()` |

The consensus tests have their own `newTestCluster`, `applyCommand`, and
`waitForValue` helpers in `consensus/raft_test.go`.

### Adding a unit test

1. Put it next to the code under test (`storage/foo_test.go`, etc.).
2. Prefer table-driven cases for validation logic.
3. Use `t.TempDir()` for any data directory so BoltDB files never leak between
   tests.
4. If you start a Raft node, always `defer node.Close()` (or use the cluster
   helper). `Close` shuts Raft down **and** releases the BoltDB file lock;
   skipping it makes the next `StartRaft` on the same directory block.
5. Run with `-race`; the suite is designed to be race-clean.

## Integration tests

They exercise the real Compose deployment over the Docker network, so the node
names (`node1`, …) must resolve.

```powershell
docker compose up -d --build
docker compose ps                      # wait for all five to be healthy

docker run --rm --network distributed-systems_cluster-net -v ${PWD}:/app -w /app golang:1.22 `
  go test -tags=integration -v -count=1 ./tests/...
```

Prerequisites:

- All five containers healthy (`/app/healthcheck` calls `grpc.health.v1`).
- Exactly one leader; the tests fail fast if the cluster cannot agree.
- Run from the repository root so the volume mount path is correct.
- The network name is `<compose-project>_cluster-net`. The default project name
  is `distributed-systems`; if you rename the project, adjust the network.

What the tests assert:

1. **Heartbeat** – every node answers.
2. **Leader agreement** – all five report the same `leader_id`, exactly one is
   in state `Leader`, and the term is non-zero.
3. **Write via follower** – a write sent to a non-leader succeeds (proving
   forwarding), and every node then returns the value through a linearizable
   read.

## Manual verification checklist

The checks below cover behaviour the automated tests cannot easily express
(killing containers, restarting nodes, preserving volumes):

```powershell
# one leader, five healthy nodes
docker compose ps
docker compose logs node1 node2 | Select-String "entering leader state"

# quorum with two nodes down
docker compose stop node4 node5
# send a Put to a surviving node (e.g. with the gateway or a small gRPC client)
# and confirm it succeeds, then read it back from node1-node3
docker compose start node4 node5

# leader restart, then full restart with data retention
docker compose restart node1
docker compose down          # no -v
docker compose up -d
```

After the final `up`, use `NodeStatus` (or the integration tests) to confirm the
cluster elects a leader and still serves previously written keys.

## Regenerating the protobuf code

The generated files are committed and pinned to a specific toolchain
(`protoc` v24.4, `protoc-gen-go` v1.33.0, `protoc-gen-go-grpc` v1.3.0):

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

Do not hand-edit `proto/storage.pb.go` or `proto/storage_grpc.pb.go`. Keep proto
changes additive so the gateway and storage tracks keep compiling.

## Notes and known costs

- `TestRaftLeaderFailover` takes about 5 s because it waits for a real election.
- `TestRaftPersistenceCatchUp` and `TestRaftSnapshotRestore` run in a few
  seconds; if either hangs, a previous test failed to call `node.Close()` and is
  still holding the BoltDB lock.
- The race detector roughly doubles the runtime of the consensus suite.
- The integration suite mutates the live cluster with unique keys
  (`integration-<unix-nano>`), so repeated runs do not collide.
