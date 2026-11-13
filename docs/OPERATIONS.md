# Operations Guide

This guide covers deployment, failure detection, degraded mode, repair,
rebalancing, and orphan cleanup for the fixed five-node cluster. For the design
rationale see [ARCHITECTURE.md](ARCHITECTURE.md) and
[DECISIONS.md](DECISIONS.md).

## Deployment

### Prerequisites

- Docker Engine or Docker Desktop with the Compose v2 plugin.
- No host Go installation. Every build and test runs in a container.
- The bash scripts use `jq` when available and fall back to `docker inspect`.

### One-command startup

```powershell
./scripts/up.ps1
```

```bash
./scripts/up.sh
```

`up` builds the image, runs `docker compose up --build -d`, waits for all five
health checks, waits for a leader and `degraded=false`, then prints
`docker compose ps` and `gateway status`. The default timeout is 120 seconds;
pass `-Timeout` / `--timeout` to change it. A timeout prints current container
status and the last 40 log lines before exiting non-zero.

Compose no longer makes node2-node5 wait for node1's health. node1 bootstraps
the five-voter configuration and Raft retries until peers appear, so startup
order does not matter. Temporary `connection refused` messages during the first
few seconds are expected and documented in the logs.

### Scripts

| Script | Purpose | Flags |
| --- | --- | --- |
| `up.ps1` / `up.sh` | Build, start, wait for health, print status | `-Timeout` / `--timeout` |
| `down.ps1` / `down.sh` | Stop containers; preserve volumes by default | `-Volumes` / `--volumes` |
| `reset.ps1` / `reset.sh` | Destructive reset of containers, network, and volumes | `-Build` / `--build` |
| `demo.ps1` / `demo.sh` | End-to-end RS/replication, failure, repair, delete | `-Mode`, `-SkipFailure`, `-Keep`, `-ValueSize` |
| `test.ps1` / `test.sh` | Unit, integration, chaos, bench suites | `-Integration`, `-Chaos`, `-Bench`, `-Keep` |
| `bench.ps1` / `bench.sh` | Load, storage, recovery, and CPU benchmarks | `-Runs`, `-Duration`, `-Mode`, `-Skip*` |

Every script supports `-Help` / `--help`. `down` is non-destructive; only
`reset` or `down -Volumes` / `down --volumes` deletes the five data volumes.

### Ports and volumes

| Node | gRPC | Raft | Volume |
| --- | --- | --- | --- |
| node1 | 50051 | 60051 | `distributed-systems_node1-data` |
| node2 | 50052 | 60052 | `distributed-systems_node2-data` |
| node3 | 50053 | 60053 | `distributed-systems_node3-data` |
| node4 | 50054 | 60054 | `distributed-systems_node4-data` |
| node5 | 50055 | 60055 | `distributed-systems_node5-data` |

The Compose project name is pinned to `distributed-systems`, so the network is
always `distributed-systems_cluster-net`. The integration tests rely on that
name. If an older checkout used a different project name, list `docker volume
ls` and rename the volumes or reset with `./scripts/reset.ps1`.

### CLI and auth

The gateway and bench tools are in the `tools` profile and are not started by
`up`:

```powershell
docker compose run --rm gateway status
docker compose run --rm gateway mode rs32
docker compose run --rm bench --help
```

When `AUTH_TOKEN` is set on the host, Compose passes it to all five nodes, the
gateway, the bench tool, and test containers. Scripts pass `-e AUTH_TOKEN` by
name and print only `auth: enabled` or `auth: disabled`; the token is never
echoed. `up` and `demo` wait on cluster status rather than auth-protected
fragments, so the same scripts work with auth on or off.

### Logs and inspection

```powershell
docker compose ps
docker compose logs -f --tail 100 node1
docker compose exec node1 sqlite3 /data/node.db "select count(*) from fragments;"
```

The runtime image includes the `sqlite` CLI specifically so operators can inspect
`/data/node.db` without installing tools on the host.

### Reset and volume migration

`docker compose down` and `./scripts/down.ps1` keep data. To start completely
fresh, use `./scripts/reset.ps1` (or `./scripts/reset.ps1 -Build` to rebuild the
image). The reset prints exactly what it will delete first.

### SQLite inspection

```powershell
docker compose exec node1 sqlite3 /data/node.db ".tables"
docker compose exec node1 sqlite3 /data/node.db "select count(*) from fragments;"
docker compose exec node1 sqlite3 /data/node.db "select key, mode, version, degraded from keys;"
docker compose exec node1 sqlite3 /data/node.db "select stripe_id, shard_index, node_id from placements order by stripe_id, shard_index;"
```

The tables are `keys`, `stripes`, `placements`, `fragments`, `cluster_config`,
and `raft_state`. The runtime image includes the `sqlite` CLI so operators do
not need host tools. Treat these queries as read-only: editing metadata by hand
can desynchronize it from Raft and from the fragments on the other nodes.

### Backup and restore

`node.db` is the authoritative local state for metadata and fragments on that
node. `raft.db` is rebuildable from peers and snapshots. `snapshots/` contains
Raft snapshots, which are **metadata only**.

#### Logical node backup

The safest per-node backup is a SQLite online backup while the node is running:

```powershell
docker compose exec node1 sqlite3 /data/node.db ".backup '/data/backup.db'"
docker cp node1:/data/backup.db ./node1-backup.db
docker compose exec node1 rm /data/backup.db
```

The `.backup` command produces a consistent single file without stopping the
node. It includes both metadata and that node's local fragments.

For a consistent set across all five nodes, stop the cluster first and copy each
volume. Do not copy `node.db` alone while writes are in flight; a plain file copy
can miss the WAL.

```powershell
./scripts/down.ps1
docker run --rm -v distributed-systems_node1-data:/data -v "${PWD}:/backup" `
  alpine tar czf /backup/node1-data.tgz -C /data .
```

#### Restore

Stop the node, replace `node.db` (and remove stale `node.db-wal`/`node.db-shm`),
then start it:

```powershell
docker compose stop node1
docker cp ./node1-backup.db node1:/data/node.db
docker compose exec node1 rm -f /data/node.db-wal /data/node.db-shm
docker compose start node1
```

If the restored metadata references fragments that are not in the local
database, the leader's repairer refills them from live peers. That is expected.

#### Metadata-only snapshot caveat

Raft snapshots deliberately exclude fragment payloads. A restore that uses only
`node.db` metadata, or only a Raft snapshot, gives the cluster the key map but
not the shard bytes. Fragments are repaired from peers when enough peers still
hold the stripe; if every copy of a shard is gone, the value is unrecoverable.

Therefore:

- Back up `node.db` for each node, not just one node's metadata.
- Never treat `snapshots/` as a complete data backup.
- After a single-node restore, wait for `degraded=false` before declaring the
  node healthy.
- A whole-cluster restore must restore all five node databases from the same
  point in time, or accept that repair can only recover stripes with at least
  three surviving shards (RS) or one surviving copy (replication).

### Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `up` times out with fewer than five healthy nodes | Run `docker compose ps` and `docker compose logs --tail 100 node1`; check port conflicts on 50051-50055 or 60051-60055 and give startup more time with `-Timeout` / `--timeout` |
| Script reports `docker compose (v2) is unavailable` | Install the Compose plugin; `docker compose version` must succeed |
| `node.db` or `raft.db` lock timeout | An old container still holds the volume; stop it and retry. `create bolt store: timeout` means a previous Raft process did not close `raft.db` |
| A node is unhealthy but the cluster still serves | The detector reports degraded; check `docker compose logs nodeN`, the Raft port, and the volume lock. Repair for that node starts automatically after it returns |
| No leader / election loops | Confirm exactly one node has `BOOTSTRAP=true`, `BOOTSTRAP_SERVERS` lists all five nodes identically, and all five Raft ports are reachable. Check `gateway status` and `docker compose logs` |
| Writes return "not the leader" | `PEERS` is missing or has the wrong gRPC addresses, so a follower cannot forward. Compare `PEERS` with the Compose node ports |
| Repair backlog keeps growing | Check `gateway status` repair queue, `gateway stats` failed count, and whether three RS shards or one replication copy survive. A sub-quorum repair cannot reconstruct; start more nodes |
| `degraded=true` never clears | Run `gateway repair <key> --force`, then inspect logs for repeated owner dial errors. Repair clears only when all five intended placements verify |
| Port 5005x or 6005x already in use | Stop the conflicting process or change the published port; inside Compose the container ports must still match `PORT` and `RAFT_PORT` |
| Integration tests cannot resolve `node1` | Run through `./scripts/test.ps1 -Integration` or use `--network distributed-systems_cluster-net` |
| Data appears missing after changing the directory name | The project name is now pinned; check `docker volume ls` and migrate or reset |
| `AUTH_TOKEN` mismatch | Set the same value for the host, every node, the gateway, and test containers; restart the cluster after changing it |
| Windows path or line-ending error | Use `./scripts/*.ps1` rather than hand-written Docker mounts, quote `${PWD}`, and keep the checkout on one drive. `gofmt` CRLF false positives are handled by `scripts/format-check.sh` |
| `cannot create /data/node.db: permission denied` on Windows | Docker Desktop file sharing is disabled for that drive, or the volume is owned by another container; reset the volume or re-enable sharing |

## What healthy looks like

- `ClusterHealth` reports `healthy_nodes = total_nodes = 5` and
  `degraded = false`.
- `degraded_stripes = 0`.
- `repairs_completed` grows during a repair but `repairs_failed` stays at zero.
- Every committed key has five local fragments distributed across five nodes.

`ClusterHealth` is leader-forwarded. It reads the leader's cached detector map;
nodes the detector has not probed yet are probed on demand.

## Degraded mode

A cluster is degraded when either condition holds:

1. Any Raft voter is unhealthy in the detector map, or
2. Any key's committed metadata has `degraded = 1`.

Degraded does **not** mean unavailable. Reads and writes continue while:

- at least three of five nodes are alive for Raft quorum, and
- a read can obtain three RS shards or one replication copy, and
- a write can store at least three shards or copies.

A healthy write stores five placements. A write that stores only three or four
commits with `degraded = true`; repair restores the remainder. Repair clears the
flag only after all five intended placements match the committed value.

## Detector settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `DETECT_INTERVAL` | `2s` | Probe period on the leader |
| `DETECT_TIMEOUT` | `1s` | One probe deadline |
| `DETECT_FAILURES` | `3` | Consecutive failures that mark a node down |
| `DETECT_SUCCESSES` | `1` | Consecutive successes that mark a node up |

A node with the default settings is declared down after roughly six seconds of
failed probes. A restarted node is marked up on its first successful probe.

## Repair settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `REPAIR_WORKERS` | `2` | Concurrent repair handlers |
| `REPAIR_MAX_ATTEMPTS` | `5` | Retries per task before it is marked failed |
| `REPAIR_SWEEP_INTERVAL` | `60s` | Metadata and orphan sweep period |
| `REPAIR_RATE_LIMIT` | `20` | Uploads per owner per second |
| `ORPHAN_GRACE_PERIOD` | `10m` | Minimum fragment age before orphan deletion |
| `ORPHAN_MAX_INDEX_LAG` | `1000` | Maximum applied-index lag for an orphan pass |

Retries back off at 1s, 2s, 4s, 8s, capped at 30s, with jitter. They do not block
client reads or writes.

## Failure runbook

### One or two nodes unavailable

Expected behaviour:

- Raft keeps quorum with three live nodes.
- Reads succeed from three RS shards or any live replication copy.
- Writes succeed if at least three placements are reachable; otherwise they fail
  with `success=false` and a message naming the stored-shard count.
- `ClusterHealth` reports `healthy_nodes = 3` or `4`, `degraded = true`, and the
  `repair_queue_depth` grows while tasks wait.

Actions:

```powershell
docker compose run --rm gateway status
docker compose stop node4 node5
# exercise reads/writes while stopped
docker compose start node4 node5
# wait for repair; status should return to 5/5 and degraded=false
docker compose run --rm gateway status
```

The `demo.ps1` / `demo.sh` runner automates this section: it stops node4 and
node5, reads and writes in RS mode, restarts them, and polls `gateway status`
until degraded clears. While the nodes are down it raises `GATEWAY_RETRIES` to 5
and the per-attempt timeout so every ring candidate, including the current
leader, is tried. Those are demo-only overrides; the normal gateway defaults
remain 3 retries and 15 seconds.

Do not edit `PEERS`, delete data volumes, or restart the whole cluster just
because two nodes are down. Repair is automatic once quorum exists.

### Three or more nodes unavailable

There is no Raft quorum. Reads and writes fail explicitly; the cluster does not
serve stale data. Restore enough nodes to reach three voters. Missing fragments
on returning nodes are refilled by the repairer.

### Restarted or returning node

The leader's detector emits a node-up event. The repairer enqueues every stripe
placed on that node and reconstructs missing fragments from live peers. A node
restored from a metadata-only snapshot has no local fragments until this pass
completes; do not treat it as fully healthy until `degraded` clears.

### Corrupt shard

A read detects a shard whose bytes no longer reconstruct the committed value
checksum and enqueues a read-repair task with `replace=true`. An operator can
request a forced repair:

```powershell
docker compose run --rm gateway repair <key> --force
```

The repairer reads a verified value first, re-encodes it, and replaces the shard
only if the key still points at the same stripe and version.

### Repair does not clear degraded

Check, in order:

1. `ClusterHealth` node list — are all five nodes healthy?
2. `Stats` on the leader — are `repairs_failed` increasing?
3. Logs for `repair: uploaded`, `repair: cleared degraded`, or repeated owner
   dial errors.
4. Whether three RS shards or one replication copy still exist. A repair cannot
   reconstruct from a sub-quorum.

If the value is unrecoverable, reads return `DataLoss` or `FailedPrecondition`
rather than unverified bytes. Restore the key from the client or a backup.

## Orphan sweep

Every node sweeps its own SQLite `fragments` table. A fragment is deleted only
when both conditions hold:

- it is older than `ORPHAN_GRACE_PERIOD`, and
- metadata refreshed immediately before deletion either has no stripe with that
  ID, or places that shard index on a different node.

The grace period must stay longer than `WRITE_TIMEOUT` so a write can finish
uploading fragments before its metadata commits. A node with no known leader or
an applied index more than `ORPHAN_MAX_INDEX_LAG` behind the leader's commit
index skips the pass. Orphan deletion touches fragments only; it never edits
metadata or reconstructs data.

## Fixed membership

The cluster is fixed at five nodes. Placement is deterministic from the sorted
node IDs, so changing the set changes the intended owner of every shard. Adding
or removing a node, and migrating existing stripes to a new node set, are future
work. Rebalancing here means refilling returning nodes and removing orphans.

## Turning detection off

Set the detector interval very high or do not start failure management. Reads
and writes still work; degraded metadata remains set and orphans remain until a
sweep runs. This is the rollback path for a repair problem.
