# Benchmark Guide

The benchmark harness measures the claims this project makes about its data
path. It is part of the repository, driven by public RPCs, and writes raw CSVs
alongside a human-readable summary so a reader can check every number.

The harness never runs inside the server process. It dials the same gRPC
surface as the gateway, so measured latency includes real forwarding, Raft
commit, fragment upload, and read gathering.

## What is measured

| Claim | Measurement |
| --- | --- |
| Put and Get latency | p50, p95, p99, maximum, and mean per operation |
| Throughput | successful operations per second and payload bytes per second |
| Reliability | gRPC error rate, rejected writes, degraded writes, retries |
| RS(3,2) storage overhead | fragment payload bytes divided by logical client bytes |
| 5x replication overhead | the same accounting for five full copies |
| Disk overhead | SQLite database bytes divided by logical bytes |
| Recovery | time from failure injection to degraded, and restart to repaired |
| CPU | average per-node CPU percentage while a scenario runs |

Out of scope: comparing this system with another database, distributed load
generation, and flame-graph profiling. The harness measures this deployment on
one host.

## Prerequisites

- Docker Engine or Docker Desktop with the Compose v2 plugin.
- No host Go installation. The benchmark binary is built into the deployment
  image by the multi-stage `Dockerfile`.
- About 2 GB of free disk for three repetitions of each storage scenario. The
  storage scenarios reset the five data volumes.

## Quick start

Windows PowerShell:

```powershell
./scripts/bench.ps1 -Runs 1 -Duration 15s
```

Bash (WSL, Git Bash, Linux, macOS):

```bash
./scripts/bench.sh --runs 1 --duration 15s
```

The script ensures the cluster is healthy, runs the selected scenarios, samples
`docker stats` per node, and writes results under `bench-results/`. The default
is three repetitions of load, storage, and recovery. Use `-SkipStorage` or
`-SkipRecovery` for a faster non-destructive run.

Storage measurement is destructive by design: before each mode it runs
`docker compose down -v --remove-orphans`, starts a fresh cluster, and waits for
health. This prevents the previous mode's fragments from polluting the ratio.

## Commands

All commands accept `--nodes id=host:port,...`. When `--nodes` is omitted,
`GATEWAY_NODES` is used. If neither is set the command exits with a clear error.
`BENCH_TOKEN` overrides `AUTH_TOKEN` when a separate benchmark credential is
wanted; otherwise the same bearer token is attached through
`internal/rpc.DialOptions`. `MAX_MESSAGE_SIZE` and `GATEWAY_TIMEOUT` come from
the same environment as the gateway.

### Load

```powershell
docker compose run --rm bench run --mode rs32 `
  --concurrency 32 --duration 60s --warmup 10s `
  --value-size 4096 --keys 10000 --read-ratio 0.5 `
  --output /results --run-id demo-load
```

Rules:

- Keys are deterministic (`key-<n>`). Workers own disjoint ranges unless
  `--hot-keys` is set, so normal runs do not measure contention on one key.
- Values are random but seeded. The same seed produces the same payload bytes
  and checksums.
- Warmup operations still execute but their samples are discarded. The measured
  window starts after warmup.
- A read is only issued for a key the worker has already written, so NotFound
  does not inflate the error rate during the first pass.
- Percentiles use the nearest-rank method: rank `ceil(p/100 * n)` in a sorted
  sample slice. The method is documented in `internal/bench/stats.go`.
- Errors are classified by gRPC code. `PutResponse.Success=false` is classified
  by message category (`conflict`, `quorum`, `invalid`, `timeout`, `rejected`).
- The bench retries only `Unavailable`, which models a leader change. A retry is
  counted separately. `DeadlineExceeded`, validation errors, and application
  rejections are final.

### Storage

```powershell
docker compose run --rm bench storage --mode rs32 `
  --values 1000 --value-size 4096 `
  --output /results --run-id demo-storage-rs32
```

The sequence is fixed:

1. Write `N` keys of size `S` in the requested mode.
2. Verify that the cluster holds `N * 5` fragments, i.e. five shards or copies
   per stripe.
3. Call `ForceSnapshot` on every node so Raft state is compacted.
4. Call `CheckpointStore` on every node so each SQLite WAL is truncated.
5. Read `Stats` from every node.
6. Compute logical, fragment, and disk bytes and both ratios.

`ForceSnapshot` and `CheckpointStore` are additive admin RPCs. They are local to
the addressed node, so the harness calls all five. `ForceSnapshot` on a follower
snapshots that follower's own state and never blocks the leader.

Expected payload ratios:

| Mode | Payload stored | Ratio to logical bytes | Extra storage |
| --- | --- | --- | --- |
| `rs32` | three data shards plus two parity shards | about **1.67x** (`5/3`) | about **67%** |
| `replication` | five full copies | **5.00x** | **400%** |

Never describe RS(3,2) as "67% of the original". It stores about 67% **more**
than the logical payload. The disk ratio is reported separately and is normally
slightly higher than the payload ratio because SQLite stores pages, indexes, and
metadata. Both ratios are in `storage.csv`.

### Recovery

The script owns container stop/start because the bench container has no Docker
socket. The split is:

1. Seed keys and verify five fragments each: `bench recovery --seed-only`.
2. Stop one node, then two nodes, using `docker compose stop`.
3. Poll `gateway status --json` until `degraded=true`; record the timestamp.
4. Start the nodes and record the restart timestamp.
5. Run `bench recovery --measure-only` with the recorded timestamps. It polls
   `ClusterHealth` until `degraded=false` and writes `recovery.csv`.

```powershell
docker compose run --rm bench recovery --measure-only `
  --scenario one-node `
  --start-time 2026-01-02T15:04:05Z `
  --degraded-time 2026-01-02T15:04:11Z `
  --restart-time 2026-01-02T15:04:20Z `
  --timeout 300s --output /results --run-id demo-recovery
```

The CSV records `time_to_degraded_seconds`, `repair_seconds`, and
`total_seconds`, plus the final healthy node count, degraded stripe count, and
repair queue depth. Repair time is measured from restart when supplied, because
that is the interval an operator waits through after restarting a node.

A recovery is expected to finish within the repair sweep interval plus
reconstruction time. The default sweep is 60 seconds; the detector marks a node
down after about six seconds with default thresholds.

### Report

```powershell
docker compose run --rm bench report --input /results/demo-load
```

`report` reads `meta.json` and the CSVs and regenerates `summary.md`. It is what
the scripts run after adding `cpu.csv`.

## CPU sampling

`scripts/bench.ps1` and `bench.sh` sample `docker stats --no-stream` before,
during, and after each scenario. The average CPU percentage per node and the
number of samples are written to `cpu.csv`. Docker Desktop CPU percentages are
relative to the host, so compare runs on the same machine and with the same
resource limits.

## Output schema

Every scenario writes a directory under `bench-results/`:

```text
bench-results/20260102-150405-load-rs32-run1/
├── meta.json          # timestamp, git commit, image, OS, CPU count, flags
├── run.json           # full structured result
├── latency.csv        # operation, count, errors, p50/p95/p99/max/mean ms
├── throughput.csv     # operation, ops/s, bytes/s, error rate, retries, degraded
├── storage.csv        # mode, logical, fragment, disk bytes, payload/disk ratios
├── recovery.csv       # scenario, timestamps, degraded/repair/total seconds
├── cpu.csv            # node, average CPU percent, sample count
├── summary.md         # human-readable tables and expected ratios
└── console.txt        # captured bench output (scripts only)
```

`meta.json` records the git commit and whether the working tree was dirty. It
also records Go version, OS/architecture, CPU count, hostname, and every flag,
so a result can be reproduced or discarded as environment-specific.

## Repetition and noise

- The scripts default to `-Runs 3` / `--runs 3` and keep every run directory;
  compare the median run and the spread rather than cherry-picking one.
- Storage ratios should be stable within a few percent across clean runs on the
  same host. Larger variation usually means a volume was not reset or the WAL
  was not checkpointed.
- Error rate must be zero with five healthy nodes.
- Latency depends on Docker Desktop CPU and disk settings, the host CPU, and
  whether the machine is busy. Do not compare numbers across machines without
  stating the environment.
- Do not take a Raft snapshot during a load run. Snapshots are forced only
  before a storage measurement; a mid-run snapshot distorts latency.

## Honest reporting checklist

1. Report both payload and disk ratios. Payload is the claim; disk is the
   operator-visible cost.
2. Say "1.67x, about 67% extra", not "67% of the original".
3. Keep the raw CSVs and `meta.json` with any quoted number.
4. State the mode, value size, key count, concurrency, duration, and run count.
5. Report degraded writes and errors instead of hiding them in an aggregate.
6. For recovery, report the scenario (one or two nodes), stop/degraded/restart
   timestamps, and whether repair was still in progress.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `--nodes or GATEWAY_NODES is required` | Pass `--nodes` or use the Compose tools service, which sets `GATEWAY_NODES` |
| `Unauthenticated` | Pass the same `AUTH_TOKEN` to the bench container (`-e AUTH_TOKEN`) or set `BENCH_TOKEN` |
| `store does not support checkpointing` | The target is not the production SQLite store; run against the real cluster |
| Storage ratio is far above expected | Volumes were not reset, or the WAL was not checkpointed; rerun with a clean cluster |
| Recovery times out | Check `docker compose ps`, the repair settings, and whether quorum still exists |
| CPU file is missing | `docker stats` is unavailable or the scenario finished before the first sample |
