# Operations Guide

This guide covers failure detection, degraded mode, repair, rebalancing, and
orphan cleanup for the fixed five-node cluster. For the design rationale see
[ARCHITECTURE.md](ARCHITECTURE.md) and [DECISIONS.md](DECISIONS.md).

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
./gateway status
docker compose stop node4 node5
# exercise reads/writes while stopped
docker compose start node4 node5
# wait for repair; status should return to 5/5 and degraded=false
./gateway status
```

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
./gateway repair <key> --force
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
