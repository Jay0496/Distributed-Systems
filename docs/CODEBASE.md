# Codebase Orientation

This guide is for a reader who needs to change the code. It describes what each
package owns, how a request moves through the system, where the test seams are,
and how to add a new RPC without creating an import cycle.

## Tree and responsibilities

```text
cmd/
  server/main.go        process wiring: config, SQLite, Raft, gRPC, auth
                        interceptors, failure management, signals
  gateway/main.go       CLI wiring: load gateway config, build client, dispatch
  healthcheck/main.go   tiny grpc.health.v1 probe used by Compose
  bench/main.go         benchmark command dispatch

internal/
  config/               environment parsing, defaults, validation
  server/               gRPC front end and coordination
    server.go           Server struct, dependencies, RPC surface
    forward.go          leader lookup, cached connections, forwarding, loop guard
    validate.go         key, value, mode, and version validation
    write.go            encode/upload/commit/GC write coordinator
    read.go             metadata read, parallel fetch, decode, verify
    read_repair.go      asynchronous queueing of failed placements
    repair.go           Repair RPC handler
    fragment_push.go    upload a shard to a local or remote owner
    fragment_fetch.go   download and verify a shard from an owner
    fragments.go        UploadFragment/DownloadFragment/Delete/List/Stat
    fragment_staging.go bounded streaming upload state machine
    gc.go               old-stripe fragment garbage collection
    admin.go            GetConfig, SetConfig, Delete, ClusterHealth, Stats
    auth.go             bearer-token interceptors and health exemption
    errors.go           gRPC status mapping helpers
  consensus/            Raft and the metadata FSM
    raft.go             Raft node, BoltDB log, file snapshots, bootstrap, Join
    fsm.go              Apply, Get, snapshot/restore
    command.go          command envelope, metadata validation, size guard
    snapshot.go         versioned snapshot envelope
  storage/              persistence boundary
    store.go            Store interface
    sqlite.go           SQLite open, WAL, connection pool, transactions
    schema.go           additive schema and indexes
    metadata.go         keys, stripes, placements, cluster config
    fragments.go        local fragment CRUD and stripe deletion
    snapshot.go         metadata-only snapshot export/import
    stats.go            keys, stripes, fragments, bytes
    errors.go           sentinel errors such as ErrNotFound
  redundancy/           data layout and encoding
    mode.go             Mode parsing and resolution
    engine.go           Engine interface
    rs32.go             Reed-Solomon RS(3,2) encode/decode
    replication.go      five-copy replication encode/decode
    placement.go        deterministic five-node placement
    shard.go            shard identity, checksums, sizes
  cluster/              failure management
    detector.go         leader-gated health probing with hysteresis
    queue.go            bounded, prioritized, deduplicated repair queue
    repairer.go         reconstruction worker and retry policy
    orphans.go          node-local orphan sweep
    interfaces.go       seams for metadata, fragments, value reads
  gateway/              client side
    router.go           consistent-hash entry ring
    client.go           attempts, candidate order, retries, backoff
    cli.go              command parsing and dispatch
    render.go           text and JSON output
  bench/                load, storage, recovery, report logic
  rpc/credentials.go    shared dial options, bearer metadata, message size

proto/storage.proto     gRPC contract; generated files are committed
scripts/                PowerShell and bash wrappers; no host Go
tests/                  support, unit, integration, chaos, bench (see tests/README.md)
docs/                   this documentation set
```

## Package dependency rules

- `cmd/*` contains process wiring and flag parsing only. It may import any
  `internal/` package, but domain packages never import `cmd/*`.
- `internal/server` may import `internal/consensus`, `internal/storage`,
  `internal/redundancy`, `internal/config`, `internal/cluster`, and
  `internal/rpc`.
- `internal/cluster` defines its own interfaces for metadata, fragments, and
  verified values. It must never import `internal/server`; the wiring in
  `cmd/server` supplies the server-backed implementations.
- `internal/redundancy` and `internal/storage` do not import `internal/server`.
- `tests/support` is test-only. Production packages never import `tests/...`.
- A package-internal white-box test must not import a support package that
  imports its own package; use a test-local double or a leaf support package.

## Request lifecycle

### Startup

1. `cmd/server/main.go` loads configuration (`internal/config`).
2. It opens the production SQLite store (`internal/storage`), runs additive
   schema setup, and enables `PRAGMA foreign_keys = ON`, WAL, and a bounded
   connection pool.
3. It constructs the FSM (`internal/consensus`) on top of the store, then the
   Raft node with BoltDB and the file snapshot store.
4. It builds the gRPC server (`internal/server`), installs bearer-token
   interceptors when `AUTH_TOKEN` is set, and starts the health service.
5. After leadership is available it starts the leader-gated detector and
   repairer, and every node starts the local orphan sweep.
6. SIGINT/SIGTERM stops gRPC and Raft, then closes SQLite so the WAL
   checkpoints cleanly.

### Write

`Server.Put` -> `validate.go` -> `forward.go` (leader) -> `write.go`:
resolve mode, read current metadata, allocate version and stripe ID, encode
shards/copies (`internal/redundancy`), compute placement, upload concurrently
(`fragment_push.go`), require three stored, then `put_meta` through Raft.
After the commit, `gc.go` deletes the previous stripe. Read the full sequence in
[ARCHITECTURE.md](ARCHITECTURE.md#write).

### Read

`Server.Get` -> `forward.go` (leader) -> `read.go`:
`get_meta` through Raft, parallel fragment fetches (`fragment_fetch.go`),
per-shard checksum verification, decode, final size/checksum verification, then
`read_repair.go` enqueues failed placements on the `internal/cluster` queue.
`Server.ReadVerified` exposes the same path to the repairer without enqueueing
another repair.

### Repair

`internal/cluster.Repairer` consumes `(stripe_id, shard_index, owner, replace)`
tasks. It re-reads metadata, drops stale tasks, reads a verified value through
its `ValueReader` seam, re-encodes, uploads only the missing/corrupt shard, and
clears `degraded` through `set_meta_status` when all five placements verify.
Sources are read repair, node-up refill, degraded stripes, the `Repair` RPC, and
the periodic sweep. The orphan sweep is separate and node-local.

## Where to add a new RPC

1. **Proto.** Add the message and `rpc` to `proto/storage.proto`; keep changes
   additive and comment every RPC. Regenerate with the command in
   [TESTING.md](TESTING.md#regenerating-protobuf-code); do not hand-edit
   generated files.
2. **Handler.** Add a method on `internal/server.Server` in the most specific
   file (`admin.go` for operations, `fragments.go` for fragment transport, and
   so on). Validate inputs before forwarding. If the RPC mutates cluster state,
   forward to the leader with the existing `forward.go` machinery.
3. **Auth.** Do nothing for auth: the unary/stream interceptors in
   `internal/server/auth.go` apply to every method except the two
   `grpc.health.v1` methods. If the RPC is internal, dial through
   `internal/rpc.DialOptions` so the token and message limit propagate.
4. **Metadata.** If the RPC changes metadata, add a command and validation in
   `internal/consensus/command.go` and apply it in `fsm.go`. Never put payload
   bytes in a metadata command.
5. **Store.** If it needs new persisted state, add an additive schema change in
   `internal/storage/schema.go` and a repository method on the `Store`
   interface. Enable foreign keys and keep migrations additive.
6. **Gateway.** Add the call to `internal/gateway.Service`, the command in
   `cli.go`, and text/JSON rendering in `render.go`. Add `--json` support and
   extend `Usage()`.
7. **Tests.** Add a black-box test under `tests/unit/<package>` or
   `tests/integration/`. If it needs unexported internals, keep it next to the
   package and add it to the exception table in `tests/README.md`.
8. **Docs.** Update [USAGE.md](USAGE.md), [ARCHITECTURE.md](ARCHITECTURE.md),
   and [REQUIREMENTS_MAPPING.md](REQUIREMENTS_MAPPING.md) if the command or
   capability is user-visible, then run `./scripts/check-docs.ps1`.

## Where test doubles live

`tests/support` owns the reusable harness:

| File | Purpose |
| --- | --- |
| `store.go` | in-memory `storage.Store` with metadata, fragments, config, stats |
| `cluster.go` | five-node Raft + SQLite + bufconn deployment |
| `listener.go` | controllable bufconn listener |
| `dialer.go` | registry-backed fake gRPC dialer |
| `probe.go` | controllable health prober |
| `clock.go` | manually advanced clock |
| `cluster_helpers.go` | leader wait, raw apply, Get helpers |
| `fixtures.go` | deterministic seeds, keys, values |
| `doc.go` | package documentation |

White-box tests that need unexported fields stay next to the package. The
authoritative exception list, with the reason for every entry, is
[tests/README.md](../tests/README.md#white-box-exceptions). Do not export an
internal just to move a test.

## Build tags

| Tag | Contents |
| --- | --- |
| none | unit tests and the in-process five-node integration suite |
| `integration` | Compose-backed checks in `tests/integration/cluster_test.go` |
| `chaos` | destructive failure, partition, and restart scenarios |
| `bench` | benchmark harness checks and fixtures |

`go test ./...` must stay green without Compose running. Run
`go vet -tags=integration ./...` and `go vet -tags=chaos ./...` because plain
`go vet ./...` does not compile tagged files.

## Conventions to preserve

- Errors use sentinel values such as `storage.ErrNotFound`; do not collapse
  every storage failure into "not found".
- Contexts and deadlines propagate through forwarding, fragment transfer, and
  repair.
- Raft entries stay metadata-only. A top-level value field in the command
  envelope is forbidden.
- Metadata is the visibility point: fragments are uploaded first, then
  `put_meta` commits. Repair must never call `put_meta`; use
  `set_meta_status`.
- Placement is deterministic from the sorted five node IDs and the stripe ID
  hash. Do not change it without a migration plan; fixed membership is a
  deliberate constraint.
- Exported types and functions have doc comments, and every internal package has
  a package comment.
- Every commit builds and passes the tests that exist at that point. The local
  gate is `./scripts/check.ps1` or `./scripts/check.sh`.
