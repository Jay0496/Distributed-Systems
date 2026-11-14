# SQLite Persistence

`internal/storage` is the production persistence layer for one storage node.
Each node owns its own SQLite database, created at `STORE_PATH` (default
`${DATA_DIR}/node.db`). The store is opened before Raft starts and closed after
Raft and gRPC have stopped, so a container restart releases the file cleanly.

## Schema

The schema is additive and created idempotently by `storage.Open` /
`storage.InitDB`:

| Table | Purpose |
| --- | --- |
| `schema_migrations` | Applied schema versions; records the release cleanup. |
| `stripes` | One row per immutable stripe: key, mode, size, checksum. |
| `keys` | Current metadata per key: stripe, mode, size, checksum, version, degraded. |
| `placements` | Deterministic owner (`node_id`) for each global shard index `0..4`. |
| `fragments` | Local shard/copy payloads with checksum and `created_at` for the orphan sweep. |
| `cluster_config` | Singleton row holding the default redundancy mode. |
| `raft_state` | Singleton row holding `last_applied_index`. |

Indexes cover `keys(stripe_id)`, `fragments(stripe_id)`, `fragments(created_at)`,
and `placements(node_id)`.

Foreign keys are enabled (`PRAGMA foreign_keys = ON`). `fragments` deliberately
has no foreign key to `stripes`: a fragment may exist briefly before metadata
commits or after its stripe is superseded, and the orphan sweep removes it.

## Store interface

`storage.Store` exposes metadata, fragments, configuration, and observability.
There is no full-value surface; a committed key is metadata plus node-local
fragments.

- liveness: `Ping`
- metadata mutations: `ApplyPutMeta`, `ApplyMetaStatus`, `ApplyDeleteMeta`,
  `ApplySetConfig`
- metadata reads: `GetKey`, `ListKeys`, `LastAppliedIndex`
- fragments: `PutFragment`, `GetFragment`, `DeleteFragment`, `DeleteStripe`,
  `ListFragments`
- config: `PutConfig`, `GetConfig`
- observability: `Stats`
- `Close`

Every `Apply*` method updates the metadata tables and
`raft_state.last_applied_index` in one transaction. An entry at or below the
stored index is a replay and is a no-op. Version conflicts return
`storage.ErrConflict`; missing rows return `storage.ErrNotFound`; a closed store
returns `storage.ErrClosed`. All three support `errors.Is`.

`SQLiteStore` implements `storage.Store`. The in-memory test double lives in
`tests/support`; production packages must not import it.

## Snapshots

`SnapshotMetadata` / `RestoreMetadata` cover stripes, keys, placements, cluster
config, and the applied index. They never include fragment payloads. Restore
clears and reinserts metadata in dependency order inside a single transaction.
The storage-level envelope uses `schema_version`, `kind`, and `payload`; the
consensus FSM wraps the raw payload in its own version 3 envelope. Unknown
schema versions are rejected.

The transitional `store` table from earlier releases is inspected during
startup. An empty table is dropped and the migration recorded in
`schema_migrations`; a non-empty table fails startup with an export instruction.

## WAL and shutdown

The database uses WAL mode, `synchronous = FULL`, a bounded single-connection
pool, and a configurable busy timeout (`STORE_BUSY_TIMEOUT`, default `5s`). A
single connection serialises writes; that is acceptable for this stage and keeps
the FSM apply path simple. `Close` checkpoints the WAL and is idempotent.

`Stats` reports the combined size of `node.db`, `node.db-wal`, and `node.db-shm`
plus key, stripe, and fragment counts. Call `Checkpoint` first when a
measurement should exclude the WAL.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `STORE_PATH` | `${DATA_DIR}/node.db` | SQLite database file |
| `STORE_BUSY_TIMEOUT` | `5s` | SQLite lock wait |

## Tests

From the repository root, no host Go required:

```powershell
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race ./internal/storage ./tests/unit/storage
docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go test -race ./internal/storage -run TestSQLiteReopenPersistence -v
```

The storage tests cover schema idempotency, the guarded `store`-table
migration, metadata transactions, replay no-ops, version conflicts, fragment
checksums, cascade deletes, config singletons, stats, concurrent writers, and
metadata snapshot restore. Repository tests use temporary SQLite databases so
the in-memory test double cannot drift from the schema.

## Resetting data

Stop the node, remove `DATA_DIR/node.db`, `node.db-wal`, and `node.db-shm`, then
start it again. Removing the whole Docker volume also removes `raft.db` and
`snapshots/`, which resets Raft state as well.
