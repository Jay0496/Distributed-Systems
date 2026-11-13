# Demo Walkthrough

This is the repeatable demo for the finished system. It takes about three
minutes with the failure section, or about one minute with `-SkipFailure`.

The scripted runner performs every step and prints a PASS/FAIL summary:

<!-- runnable -->
```powershell
./scripts/demo.ps1
```

<!-- runnable -->
```bash
./scripts/demo.sh
```

The script starts the cluster only when fewer than five nodes are healthy, and
tears down only a cluster it started. Use `-Keep` / `--keep` to leave it
running, `-SkipFailure` / `--skip-failure` to omit the node-stop section, and
`-ValueSize` / `--value-size` to change the deterministic payload size
(default 4096 bytes).

The demo sets `GATEWAY_RETRIES=5` and `GATEWAY_TIMEOUT=45s` for its own run so
the gateway tries every ring candidate while two nodes are down. Those are
demo-only values; the gateway defaults remain 3 retries and 15 seconds. Both
variables are restored when the script exits.

## Prerequisites

- Docker Engine or Docker Desktop with Compose v2.
- No host Go installation.
- Ports 50051-50055 and 60051-60055 free.
- About two minutes of patience for image build and startup on a cold machine;
  later runs are faster.
- Optional: a second terminal for `docker compose logs -f node1`.

## Step 1 — Start five healthy nodes

```powershell
./scripts/up.ps1
```

Expected output categories:

```text
auth: disabled
Section: docker compose up
...
NAME      IMAGE                 COMMAND             SERVICE   STATUS                   PORTS
node1     distributed-systems…  /app/server         node1     Up (healthy)             0.0.0.0:50051->50051/tcp, …
node2     …                     node2               Up (healthy)             …
node3     …                     node3               Up (healthy)             …
node4     …                     node4               Up (healthy)             …
node5     …                     node5               Up (healthy)             …
Section: gateway status
leader: node1
mode: rs32
health: 5/5 nodes healthy, degraded=false, degraded_stripes=0, repair_queue=0
served by: node<n>

NODE    HEALTHY  LEADER  RAFT STATE  FRAGMENTS  STORAGE  ERROR
node1   yes      yes     Leader      …          …        -
node2   yes      no      Follower    …          …        -
…
cluster is ready
```

Talking point: every node runs the same binary; node1 is the bootstrap node but
leadership can move.

## Step 2 — Write and read in RS mode

```powershell
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway get demo-key
```

The demo uses a deterministic 4096-byte value so the storage numbers are easy to
compare. Expected categories:

```text
put committed on node1: key="demo-key" version=1 mode=rs32 shards=5/5 stripe=<id>

PASS: rs32 write - version=1 shards=5/5 stripe=<id>
PASS: rs32 read - verified 4096 bytes with 3 shards
```

`get` writes the value to stdout and the forwarding line to stderr. The value
must match byte for byte; the script fails otherwise.

## Step 3 — Show RS storage

The demo queries each node's SQLite `fragments` table for the stripe:

```text
Per-node fragments for the rs32 stripe:
Node    Fragments  Bytes
node1   1          1.3 KiB
node2   1          1.3 KiB
node3   1          1.3 KiB
node4   1          1.3 KiB
node5   1          1.3 KiB
PASS: rs32 placement - 5 shards, 6.7 KiB total
```

With a 4096-byte value, `shardSize = ceil(4096/3) = 1366`, so five shards store
6830 bytes — about **1.67x** the original, or about 67% extra. Three of the five
shards reconstruct the value.

## Step 4 — Compare with five-copy replication

```powershell
docker compose run --rm gateway mode replication
docker compose run --rm gateway put demo-key "hello" --mode replication
```

Expected categories:

```text
default mode set to replication on node1

Per-node fragments for the replication stripe:
Node    Fragments  Bytes
node1   1          4.0 KiB
…
PASS: replication placement - 5 copies, 20.0 KiB total

rs32:        6.7 KiB stored for 4.0 KiB original (1.67x, about 67% extra)
replication: 20.0 KiB stored for 4.0 KiB original (5x, about 400% extra)
replication / rs32 total ratio: 3x
```

Talking point: replication is deliberately five full copies because the cluster
has exactly five nodes. The project does not implement a 3x mode. RS trades CPU
for storage; replication trades storage for the simplest possible read.

## Step 5 — Stop two nodes

```powershell
docker compose stop node4 node5
docker compose run --rm gateway status
docker compose run --rm gateway get demo-key
```

The demo stops node4 and node5, waits for the detector to report degraded mode,
then reads a key written before the failure and writes a new degraded key.
Expected categories:

```text
PASS: failure baseline - wrote demo-key-failure with five shards
PASS: degraded status - healthy=3/5, degraded_stripes=1
PASS: read with two nodes down - reconstructed from 3 shards
PASS: write with two nodes down - committed 3/5 shards, degraded=true
```

`status` shows `healthy=3/5`, `degraded=true`, and the stopped nodes as
unhealthy. Reads still reconstruct from three shards. A new write stores only
three placements and commits as degraded.

Talking point: this is the important qualification. A **healthy** write, or a
stripe repaired back to five shards, tolerates two failures. A degraded RS
write that stored only three shards is readable now but would not survive two
further failures until repair finishes.

## Step 6 — Restart and repair

```powershell
docker compose start node4 node5
docker compose run --rm gateway health
docker compose run --rm gateway status
```

The demo restarts the nodes and polls `health` instead of grepping logs.
Expected categories:

```text
waiting for automatic repair (not a log-line grep)
PASS: repair - degraded mode cleared automatically
Per-node fragments after repair:
Node    Fragments  Bytes
node1   1          …
…
PASS: fragment refill - 5 shards present after restart
```

Timing: detection defaults to a 2-second interval with three failures (roughly
six seconds), and the repair sweep defaults to 60 seconds. On a local Docker
Desktop machine the whole restart-to-repaired step is normally well under the
demo's 120-second repair timeout.

Talking point: the leader's detector emits node-up, the repairer enqueues every
stripe placed on the returning node, reconstructs from verified peers, and
clears the degraded flag only when all five placements match.

## Step 7 — Delete and garbage collection

```powershell
docker compose run --rm gateway delete demo-key
docker compose run --rm gateway get demo-key
```

Expected categories:

```text
delete on node1: key "demo-key" removed (old stripe <id>)
PASS: delete - key is gone after delete
```

Metadata commits first, then the old stripe's fragments are deleted on their
owners. A subsequent `get` returns `NotFound`. The demo also deletes its
failure and degraded keys.

## Step 8 — Optional benchmark summary

```powershell
./scripts/bench.ps1 -Runs 1 -Duration 15s -SkipStorage -SkipRecovery
```

Use the full command without the skip flags when you need the storage and
recovery numbers. The benchmark docs and the expected 1.67x / 5.00x ratios are in
[BENCHMARKS.md](BENCHMARKS.md). Quote the medians and the raw CSVs, not a single
best run.

## Speaker checklist

Before the demo:

- [ ] `./scripts/check.ps1` (or `check.sh`) passes.
- [ ] `./scripts/check-docs.ps1` (or `check-docs.sh`) passes.
- [ ] `docker compose ps` shows five healthy nodes, or the cluster is stopped
      and `up` will start it.
- [ ] `./scripts/demo.ps1` passes once from a clean cluster.
- [ ] The second terminal is ready with `docker compose logs -f node1`.
- [ ] The expected 1.67x versus 5.00x numbers are on hand; do not say "67% of
      the original".

During the demo:

- [ ] Say "entry node" for the gateway and "leader" for shard fan-out.
- [ ] Say "five full copies" for replication, not "3x replication".
- [ ] Qualify the two-failure guarantee for degraded RS writes.
- [ ] Say "refill returning nodes and sweep orphans" for rebalancing; do not
      claim key migration or dynamic membership.
- [ ] Show `status` before and after the failure section.

## Recovery if a step fails

1. Capture state:

   ```powershell
   docker compose ps
   docker compose logs --tail 100 node1
   docker compose run --rm gateway status
   ```

2. If the cluster is unhealthy or stuck, reset it destructively and restart:

   ```powershell
   ./scripts/reset.ps1 -Build
   ./scripts/up.ps1
   ```

3. If only the demo data is inconsistent, delete the demo keys and rerun:

   ```powershell
   docker compose run --rm gateway delete demo-key
   docker compose run --rm gateway delete demo-key-failure
   docker compose run --rm gateway delete demo-key-degraded
   ./scripts/demo.ps1
   ```

4. If repair does not clear, follow the repair runbook in
   [OPERATIONS.md](OPERATIONS.md#repair-does-not-clear-degraded).

The demo is safe to rerun: keys are overwritten with a new version and the
script tolerates an existing cluster.
