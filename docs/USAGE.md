# CLI Usage Reference

The gateway is a one-shot client, not a service. It reads `GATEWAY_NODES`,
selects an entry node with a consistent-hash ring, calls the gRPC API, retries
transient failures against ring successors, and exits.

Every command accepts `--json`. Text output is for humans; JSON is the stable
interface used by scripts, the demo, and the benchmark harness.

## Invocation

Inside the Compose deployment, run the `gateway` service:

```powershell
docker compose run --rm gateway <command> [arguments] [flags]
```

The `gateway` image is built from the same binary as the nodes. If you have a
built binary, the equivalent is:

```powershell
./gateway <command> [arguments] [flags]
```

`docker compose run` passes the container exit status through. Run
`gateway help` or `gateway <command> --help` for the same text as this
document. Help does not require `GATEWAY_NODES` to be set.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `GATEWAY_NODES` | required | `id=host:port,...` mapping Raft node IDs to gRPC addresses. There is no localhost fallback. |
| `GATEWAY_VNODES` | `100` | Virtual nodes per host on the consistent-hash ring. |
| `GATEWAY_TIMEOUT` | `15s` | Deadline for one RPC attempt. |
| `GATEWAY_RETRIES` | `3` | Retries after the first attempt; `0` disables retries. |
| `AUTH_TOKEN` | empty | Optional shared bearer token. Empty disables auth and the server logs a startup warning. |
| `MAX_MESSAGE_SIZE` | `32 MiB` | Client and server gRPC message limit. |
| `BENCH_TOKEN` | empty | Benchmark-only override for `AUTH_TOKEN`; not used by the gateway. |

The Compose `gateway` service sets a complete `GATEWAY_NODES` for the five-node
network. A documented explicit override is allowed:

```powershell
docker compose run --rm -e GATEWAY_NODES=node1=localhost:50051,node2=localhost:50052 gateway status
```

The CLI never assumes that mapping. If `GATEWAY_NODES` is unset or malformed,
startup fails with an error naming the variable and showing the expected format.

Server-side tuning variables (`MAX_VALUE_SIZE`, `READ_TIMEOUT`,
`READ_PARALLELISM`, `SHARD_UPLOAD_TIMEOUT`, `DETECT_*`, `REPAIR_*`,
`ORPHAN_*`) are documented in [OPERATIONS.md](OPERATIONS.md).

## Common behavior

- Flags may appear before or after positional arguments (`put --mode rs32 k v`
  and `put k v --mode rs32` are equivalent). `--` ends flag parsing.
- `-h` is an alias for `--help`.
- `--json` is available on every command. It writes exactly one JSON object to
  stdout.
- `get` writes the raw value and a trailing newline to stdout. Forwarding
  metadata (`served by ... mode=... version=... shards=... repaired=...`) goes
  to stderr so the value can be piped or redirected.
- Values may contain arbitrary bytes when read with `--value-file`. A
  positional value is passed through the shell, so binary data should use a
  file.
- `put` accepts either a positional value or `--value-file`, never both. Keys
  must be non-empty. An empty value is valid and is encoded as an empty stripe;
  the server rejects values larger than `MAX_VALUE_SIZE` (default 8 MiB).
- A write is last-write-wins unless `--version N` supplies an expected version.
  `delete` is idempotent; `--version N` makes it compare-and-delete.
- Retries use 100 ms, 250 ms, and 500 ms steps with ±20% jitter. `Unavailable`
  and `DeadlineExceeded` try the next ring candidate. `Aborted` without
  `--version` re-resolves the leader and retries. Validation, auth, not-found,
  and conflict responses return immediately. The final error lists every
  attempted node and its error.
- Tokens are never printed.

## Commands

### `put`

```text
gateway put <key> <value> [flags]
gateway put <key> --value-file PATH [flags]
```

| Flag | Meaning |
| --- | --- |
| `--mode rs32\|replication` | Override the cluster default for this write. |
| `--version N` | Write only when the stored version is `N`; a mismatch is a conflict. |
| `--value-file PATH` | Read the value bytes from a file. |
| `--json` | Emit JSON. |

```powershell
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway put demo-key "hello" --mode replication
docker compose run --rm gateway put demo-key --value-file /tmp/value.bin
docker compose run --rm gateway put demo-key "next" --version 2
```

Text output:

```text
put committed on node1: key="demo-key" version=1 mode=rs32 shards=5/5 stripe=<id>
```

A degraded write prints `degraded=true` and `shards=3/5` or `shards=4/5`.
`--json` output:

```json
{
  "node": "node1",
  "key": "demo-key",
  "success": true,
  "message": "",
  "stripe_id": "…",
  "mode": "rs32",
  "version": 1,
  "degraded": false,
  "shards_stored": 5,
  "checksum": "…"
}
```

### `get`

```text
gateway get <key> [--json]
```

Reads a linearizable value through the leader, reconstructs from any three RS
shards or one replication copy, and verifies the committed size and SHA-256
before returning. Missing keys are `NotFound`; unverifiable data is
`FailedPrecondition` or `DataLoss`, never partial bytes.

```powershell
docker compose run --rm gateway get demo-key
docker compose run --rm gateway get demo-key --json
docker compose run --rm gateway get demo-key > value.txt
```

`--json` output:

```json
{
  "node": "node1",
  "key": "demo-key",
  "success": true,
  "value": "hello",
  "mode": "rs32",
  "stripe_id": "…",
  "version": 1,
  "shards_used": 3,
  "repaired": false,
  "checksum": "…"
}
```

`repaired` is true when a successful read enqueued at least one asynchronous
read-repair task.

### `delete`

```text
gateway delete <key> [--version N] [--json]
```

Deletes metadata through Raft first, then garbage-collects the stripe's
fragments on their owners. A missing key is successful and idempotent.

```powershell
docker compose run --rm gateway delete demo-key
docker compose run --rm gateway delete demo-key --version 3
```

`--json` output includes `success`, `found`, `old_stripe_id`, and `message`.
`found=false` means the key was already absent.

### `mode`

```text
gateway mode [rs32|replication] [--json]
```

With no argument, reads the cluster-wide default. With an argument, validates
it and commits a new default through Raft. Only `rs32` and `replication` are
accepted; anything else is rejected by the CLI before an RPC is sent.

```powershell
docker compose run --rm gateway mode
docker compose run --rm gateway mode rs32
docker compose run --rm gateway mode replication
```

Changing the default does not rewrite existing stripes; it affects later writes
that do not supply `--mode`.

### `status`

```text
gateway status [--json]
```

Shows the leader, global mode, healthy nodes, degraded state, degraded stripe
count, repair queue depth, and one row per Raft voter.

```powershell
docker compose run --rm gateway status
docker compose run --rm gateway status --json
```

`--json` output:

```json
{
  "node": "node1",
  "success": true,
  "leader_id": "node1",
  "mode": "rs32",
  "degraded": false,
  "healthy_nodes": 5,
  "total_nodes": 5,
  "degraded_stripes": 0,
  "repair_queue_depth": 0,
  "message": "",
  "nodes": [
    {
      "node_id": "node1",
      "grpc_addr": "node1:50051",
      "raft_state": "Leader",
      "healthy": true,
      "leader": true,
      "last_seen_unix": 1767225600,
      "fragments": 10,
      "storage_bytes": 81920
    }
  ]
}
```

### `health`

```text
gateway health [--json]
```

Prints one line and exits non-zero unless every voter is healthy, a leader is
known, and no committed stripe is degraded. This is the script-friendly check
used by `up`, `demo`, and `bench`.

```powershell
docker compose run --rm gateway health
docker compose run --rm gateway health --json
```

Text output:

```text
healthy=true leader=node1 mode=rs32 nodes=5/5 degraded=false degraded_stripes=0 repair_queue=0 message=""
```

### `stats`

```text
gateway stats [--json]
```

Reports local storage and repair counters from the leader (or the first
reachable node when no leader is known). Counters are node-local; use `status`
for the cluster view.

```powershell
docker compose run --rm gateway stats
docker compose run --rm gateway stats --json
```

`--json` output includes `node_id`, `mode`, `leader`, `keys`, `stripes`,
`fragments`, `storage_bytes`, `repair_queue_depth`, `repairs_completed`,
`repairs_failed`, and `uptime_seconds`.

### `repair`

```text
gateway repair <key> [--stripe ID] [--force] [--json]
```

Asks the leader to enqueue repair for a key and returns immediately; it does
not wait for the repair to finish. `--stripe ID` must match the key's current
stripe. `--force` re-encodes and replaces present shards after a verified read.

```powershell
docker compose run --rm gateway repair demo-key
docker compose run --rm gateway repair demo-key --force
docker compose run --rm gateway repair demo-key --stripe <stripe-id>
```

Check progress with `status` or `health`; degraded clears only when all five
intended placements verify.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | The command succeeded, help was printed, or `health` found a healthy cluster. |
| `1` | A usage error, configuration error, transport failure, rejected application result, or an unhealthy `health` check. |
| `2` | Reserved for `bench` command-line misuse; the gateway uses `1`. |

`docker compose run --rm` returns the container exit status, so scripts can use
the gateway command directly in an `if` or with `$LASTEXITCODE` / `$?`.

## Authentication

When `AUTH_TOKEN` is set, the gateway attaches
`authorization: Bearer <token>` to every call through the shared
`internal/rpc.DialOptions` helper. The server compares tokens in constant time
and returns `Unauthenticated` for a missing or wrong token.

Only `/grpc.health.v1.Health/Check` and `/grpc.health.v1.Health/Watch` bypass
authentication. The gateway's `health` command calls `ClusterHealth`, which
requires the token. Set the same `AUTH_TOKEN` for the cluster, the gateway
container, and any test container:

```powershell
$env:AUTH_TOKEN = "change-me"
./scripts/up.ps1
docker compose run --rm -e AUTH_TOKEN gateway status
```

See [SECURITY.md](SECURITY.md) for the threat model and limitations.

## Modes and config precedence

For each write, the effective mode is:

1. the request's non-empty `--mode`, otherwise
2. the committed cluster default from `mode`, otherwise
3. `rs32`.

`mode` changes only tier 2. A per-request override never changes the cluster
default, and a rejected config write never changes the effective mode.

## Common errors

| Error | Cause / fix |
| --- | --- |
| `GATEWAY_NODES is required` | Set the variable or use the Compose `gateway` service. |
| `Unauthenticated` | The token differs from the cluster's; restart tools and nodes with the same value. |
| `no leader` / `DeadlineExceeded` | Fewer than three voters are reachable, or the cluster is still electing. Run `status` and check logs. |
| `FailedPrecondition` on `get` | Fewer than three RS shards or no replication copy survive. Start enough nodes and run `repair`. |
| `DataLoss` on `get` | Surviving shards no longer match the committed checksum. The value is not returned; restore it from the client. |
| `Aborted` with `--version` | The stored version differs. Re-read the key and retry with the current version. |
| `permission denied` reading `--value-file` | The file must be readable inside the container; mount it with `-v`. |
