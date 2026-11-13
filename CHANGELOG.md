# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
semantic versioning.

## v1.0.0 — 2026-11-13

First complete release of the five-node distributed key-value store.

### Added

- Five-node Raft cluster whose log entries carry metadata only.
- Reed-Solomon RS(3,2) mode with three data shards and two parity shards, one
  shard per node, and five-copy replication mode.
- Out-of-band fragment upload and download with chunking, size limits,
  checksums, and context deadlines.
- Leader-owned failure detector, degraded-mode reporting, read repair,
  returning-node refill, periodic repair sweep, and node-local orphan cleanup.
- SQLite persistence for metadata and local fragments, with WAL,
  `schema_migrations`, and guarded removal of the transitional `store` table.
- Gateway CLI with a consistent-hash entry ring, retries, leader discovery,
  per-request mode override, bearer-token auth, and JSON output.
- Structured logging with `log/slog`, JSON output for containers, request IDs
  propagated across forwarding and fragment calls, and no payload or token
  fields.
- Separate liveness and readiness signals on the standard gRPC health service,
  plus a wire-compatible `Heartbeat` RPC that reports readiness.
- Graceful shutdown that drains gRPC, stops detector and repair workers, closes
  cached connections, releases the Raft storage lock, and closes SQLite.
- Docker-first scripts for startup, teardown, reset, demo, tests, checks, and
  benchmarks, with no host Go requirement.
- Local documentation guard (`scripts/check-docs.ps1` / `check-docs.sh`).
- Release versioning through `internal/version` and the Docker build's
  `-ldflags` stamp.
- `docs/DEMO.md` clean-machine checklist and `CHANGELOG.md`.

### Changed

- Raft snapshots are metadata-only at `schema_version: 3`. Fragment payloads are
  never snapshotted; owners repair missing shards from peers.
- The gateway and bench tools run only through the Compose `tools` profile or
  the scripts; there is no hardcoded localhost fallback.
- Request paths derive outbound deadlines from the incoming context and cap
  them with configuration.
- Health checks distinguish liveness (empty service name) from readiness
  (`storage.StorageService`).
- `scripts/test.ps1 -All` and `scripts/test.sh --all` run the full unit,
  integration, chaos, and benchmark matrix.
- The in-memory test double is named `MemoryStore` and remains confined to
  `tests/support`.

### Fixed

- A canceled client request no longer waits on a blocked forward, fragment
  upload, download, or repair worker.
- A repeated shutdown signal is safe; SQLite and Raft closers are idempotent.
- A restarted node reopens `node.db` and `raft.db` immediately after graceful
  shutdown without a lock error.
- Startup fails with an export instruction when a previous database still has
  values in the transitional `store` table, instead of dropping them.

### Removed

- The legacy full-value Raft commands (`put`, `get`), their nested payload,
  `GetResult`, and the `KVStore` surface.
- Production references to in-memory test storage.
- Unused config, comments, and scaffolding that described the transition.

### Known limitations

- Membership is fixed at five nodes. There is no dynamic join/leave migration
  of existing stripes.
- Snapshot version 1 and 2 data that still contains legacy full-value rows
  cannot be migrated automatically. Export and rewrite those keys before
  upgrading; startup fails rather than dropping them.
- A degraded RS write that stored only three shards is readable immediately but
  does not tolerate two further failures until repair completes.
- Raft and fragment traffic is protected by an optional shared bearer token;
  there is no per-node identity, TLS client authentication, or encryption at
  rest.
- The project targets a course demonstration, not a production control plane.

### Release verification

The release matrix was run from the repository root with:

- Docker Engine `29.8.1`
- Docker Compose `v5.5.1`
- `golang:1.22` image, Go `1.22.12 linux/amd64`

See [docs/TESTING.md](docs/TESTING.md) and [docs/DEMO.md](docs/DEMO.md) for the
commands and the clean-machine rehearsal.
