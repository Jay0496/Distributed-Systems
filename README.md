# Distributed NoSQL Key-Value Store

A five-node distributed key-value store built for the course project. Every
value is written with either Reed-Solomon RS(3,2) erasure coding or five full
replicas. Consensus carries metadata only; shard payloads move out of band
directly between nodes. A gateway CLI hides leader changes, a leader-owned
detector handles failure reporting, and a repairer refills missing shards
automatically.

The repository is Docker-first: **no host Go installation is required** for any
build, test, benchmark, or check.

## What the project demonstrates

- **Metadata-only Raft.** The replicated log stores keys, stripe IDs, checksums,
  versions, and placements — never payload bytes.
- **Two redundancy modes on five nodes.**
  - `rs32`: three data shards plus two parity shards, one shard per node,
    about **1.67x** the logical payload (about 67% extra).
  - `replication`: five full copies, one per node, **5.00x** the logical
    payload (400% extra).
- **Out-of-band fragment transfer.** The leader streams each shard to its owner
  over `UploadFragment`; Raft records only the metadata after enough shards are
  durable.
- **Failure tolerance after a healthy write or completed repair.** Three live
  nodes hold both a Raft quorum and enough shards or copies, so two node
  failures are tolerated. A degraded RS write that stored only three shards is
  readable immediately but needs repair before it survives two further failures.
- **Automatic repair.** Read repair, degraded-stripe repair, returning-node
  refill, a periodic sweep, and a node-local orphan cleanup run without operator
  action.
- **Durable local state.** SQLite holds metadata and local fragments on every
  node; BoltDB holds the Raft log and stable state; Raft snapshots capture
  metadata only and fragments are repaired from peers.

## Architecture at a glance

```text
                       consistent-hash ring picks an entry node
client / CLI ──gRPC──► entry node ──forward──► leader
                                                 │
                          ┌──────────────────────┼──────────────────────┐
                          │                      │                      │
                    Raft metadata          encode RS(3,2)         five-copy
                    (no payload)           or 5 copies           placement
                          │                      │                      │
                          ▼                      ▼                      ▼
                    SQLite on every        UploadFragment        one shard or
                    node; snapshots        to each owner         copy per node
                    are metadata-only      (out of band)         (5 nodes)
```

A write uploads its shards or copies first, then commits a metadata-only entry
through Raft. A read commits a metadata lookup, gathers enough verified
fragments from their owners, checks the value checksum, and returns only bytes
that match the committed size and SHA-256.

The full design, request sequences, and consistency model are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Rationale is recorded in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Quick start

Prerequisites: Docker Engine or Docker Desktop with the Compose v2 plugin.

<!-- runnable -->
```powershell
# Windows PowerShell
./scripts/up.ps1
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway get demo-key
docker compose run --rm gateway status
./scripts/demo.ps1
./scripts/down.ps1
```

<!-- runnable -->
```bash
# Bash (WSL, Git Bash, macOS, Linux)
./scripts/up.sh
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway get demo-key
docker compose run --rm gateway status
./scripts/demo.sh
./scripts/down.sh
```

`up` builds the deployment image, starts node1 through node5, waits for all five
health checks, waits for a leader and `degraded=false`, then prints
`docker compose ps` and `gateway status`. It exits non-zero if the cluster does
not become healthy.

The raw Compose commands also work:

<!-- runnable -->
```powershell
docker compose up --build -d
docker compose ps
docker compose down        # keeps data volumes
docker compose down -v     # deletes data volumes
```

Prefer the scripts: `down` preserves volumes by default and `reset` explicitly
deletes them. See [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Redundancy modes

| Mode | Fragments per stripe | Placement | Reconstruct from | Payload ratio |
| --- | --- | --- | --- | --- |
| `rs32` | 5 (indices 0-2 data, 3-4 parity) | one shard per node | any 3 distinct shards | about 1.67x (67% extra) |
| `replication` | 5 full copies (indices 0-4 data) | one copy per node | any 1 valid copy | 5.00x (400% extra) |

The cluster default is `rs32` until an operator changes it. A per-request
`--mode` override always wins:

<!-- runnable -->
```powershell
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway put demo-key "hello" --mode replication
docker compose run --rm gateway mode rs32          # set the cluster default
docker compose run --rm gateway mode               # read the current default
```

Placement is deterministic: the five sorted node IDs are rotated by a hash of
the stripe ID, so every shard has a stable intended owner while membership stays
fixed. The gateway's consistent-hash ring chooses an **entry node** only; the
leader coordinates shard fan-out. Consistent hashing does not change data
placement.

## CLI overview

The gateway is a one-shot Compose tool. Every command supports `--json` for
machine-readable output, and `get` writes the value to stdout while forwarding
metadata goes to stderr so values can be piped.

| Command | Purpose |
| --- | --- |
| `put <key> <value>` | Store a value; flags `--mode`, `--version N`, `--value-file PATH` |
| `get <key>` | Read a verified value |
| `delete <key>` | Idempotent delete; `--version N` makes it compare-and-delete |
| `mode [rs32\|replication]` | Read or set the cluster-wide default |
| `status` | Leader, mode, per-node health, degraded stripes, repair queue |
| `health` | One health line; exits non-zero unless every voter is healthy and nothing is degraded |
| `stats` | Local storage, fragment, and repair counters from the leader |
| `repair <key>` | Enqueue repair; `--stripe ID`, `--force` |

```powershell
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway get demo-key
docker compose run --rm gateway status
docker compose run --rm gateway health
docker compose run --rm gateway stats
docker compose run --rm gateway repair demo-key --force
```

`GATEWAY_NODES` is required by the CLI and has no localhost fallback; the
Compose `gateway` service sets it. The full reference — flags, environment
variables, exit codes, JSON schemas, and auth behavior — is in
[docs/USAGE.md](docs/USAGE.md).

## Failure behavior

| Situation | Behaviour |
| --- | --- |
| One or two nodes down after a healthy write | Raft keeps quorum; reads reconstruct from three RS shards or a surviving copy; writes commit if at least three placements are reachable |
| A write stores three or four placements | Commits as `degraded=true`; repair restores the missing shards |
| A write stores fewer than three | Rejected; the metadata is not committed |
| A node returns | The leader's detector emits node-up and the repairer refills every stripe placed on that node |
| Fewer than three of five nodes alive | No quorum: reads and writes fail explicitly rather than serving stale data |
| A shard is missing or corrupt | Reads try other shards, verify the value checksum, and enqueue read repair |
| Repair cannot reconstruct | Reads return `FailedPrecondition` or `DataLoss`, never unverified bytes |

A degraded RS write is the one qualification: it stored only three shards, so
it does not survive two further failures until repair completes. Replication
degraded writes remain readable through any surviving copy.

A scripted walkthrough with expected output is in [docs/DEMO.md](docs/DEMO.md).
Operator runbooks are in [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Verification

The canonical local verification commands are the Docker-based check scripts.
There is no GitHub-hosted workflow and no paid CI dependency.

<!-- runnable -->
```powershell
# Windows PowerShell: full local check
./scripts/check.ps1

# Documentation link and stale-string checks
./scripts/check-docs.ps1
```

<!-- runnable -->
```bash
# Bash: full local check
./scripts/check.sh

# Documentation link and stale-string checks
./scripts/check-docs.sh
```

`check` runs `go build`, all three vet tag sets, the fast race suite, the chaos
suite, and a CRLF-safe `gofmt` check entirely in `golang:1.22` Docker.

| Goal | Command |
| --- | --- |
| Unit and in-process integration | `./scripts/test.ps1` / `./scripts/test.sh` |
| Compose-backed integration | `./scripts/test.ps1 -Integration` / `./scripts/test.sh --integration` |
| Chaos suite | `./scripts/test.ps1 -Chaos` / `./scripts/test.sh --chaos` |
| Benchmark harness checks | `./scripts/test.ps1 -Bench` / `./scripts/test.sh --bench` |
| Full local check | `./scripts/check.ps1` / `./scripts/check.sh` |
| Documentation check | `./scripts/check-docs.ps1` / `./scripts/check-docs.sh` |

Benchmarks measure put/get latency and throughput, RS-versus-replication
storage overhead, recovery time, and per-node CPU:

<!-- runnable -->
```powershell
./scripts/bench.ps1 -Runs 1 -Duration 15s
```

```bash
./scripts/bench.sh --runs 1 --duration 15s
```

Results are written under `bench-results/<run-id>/` as CSVs, `meta.json`,
`run.json`, and `summary.md`. Storage measurement is destructive because it
resets the five data volumes before each mode; use `-SkipStorage` /
`--skip-storage` for a non-destructive run. Methodology, output schema, and the
expected ratios are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

The test layout, build tags, coverage matrix, and white-box exceptions are in
[docs/TESTING.md](docs/TESTING.md) and [tests/README.md](tests/README.md).

## Repository layout

```text
cmd/
  server/       process wiring for a cluster node
  gateway/      CLI entrypoint
  healthcheck/  Compose health probe
  bench/        benchmark harness
internal/
  config/       environment parsing and validation
  server/       gRPC handlers, forwarding, auth, fragment transport
  consensus/    Raft node, metadata FSM, snapshots
  storage/      Store interface, SQLite metadata and fragment repositories
  redundancy/   RS(3,2), five-copy replication, checksums, placement
  cluster/      failure detector, repair queue and repairer, orphan sweep
  gateway/      ring, retrying client, CLI commands, rendering
  bench/        load, storage, recovery, and report logic
  rpc/          shared dial options, auth metadata, message limits
proto/          storage.proto and committed generated code
scripts/        PowerShell and bash wrappers for every workflow
tests/          support, unit, integration, chaos, and bench suites
docs/           usage, demo, operations, security, architecture, decisions,
                codebase, testing, benchmarks, requirements mapping
```

Dependency rule: `cmd/*` contains wiring only; `internal/server` may import
domain packages; domain packages never import `internal/server`,
`internal/gateway`, or `cmd/*`; production packages never import `tests/`.
See [docs/CODEBASE.md](docs/CODEBASE.md) for the request lifecycle and the
add-an-RPC guide.

## Configuration

All node and CLI settings are environment variables. The common ones:

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_ID` | `node1` | Raft server ID and Compose hostname |
| `PORT` / `RAFT_PORT` | `50051` / `60051` | gRPC and Raft listen ports |
| `DATA_DIR` | `/data` | `node.db`, `raft.db`, and `snapshots/` |
| `STORE_PATH` | `${DATA_DIR}/node.db` | SQLite database file |
| `BOOTSTRAP` | `false` | `true` on node1 only for a fresh cluster |
| `BOOTSTRAP_SERVERS` | empty | `id=raft-addr,...` for the full five-node cluster |
| `PEERS` | empty | `id=grpc-addr,...` used for leader forwarding |
| `GATEWAY_NODES` | required | `id=host:port,...` for the CLI; no localhost fallback |
| `AUTH_TOKEN` | empty | Shared bearer token; empty disables auth with a warning |
| `TLS_ENABLED` | `false` | Optional TLS on the gRPC server and forwarding clients |

Detector, repair, fragment, and read-path tuning variables are documented with
their defaults in [docs/OPERATIONS.md](docs/OPERATIONS.md) and
[docs/USAGE.md](docs/USAGE.md).

## Documentation index

| Document | Contents |
| --- | --- |
| [docs/USAGE.md](docs/USAGE.md) | Full CLI reference, flags, env vars, exit codes, JSON output, auth |
| [docs/DEMO.md](docs/DEMO.md) | Scripted demo, expected output, timings, speaker checklist |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Start/stop/reset, ports, volumes, logs, degraded mode, repair, backup, troubleshooting |
| [docs/SECURITY.md](docs/SECURITY.md) | Threat model, bearer token, health exemption, TLS, secrets, out-of-scope items |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Final design, metadata protocol, write/read paths, repair, snapshots, consistency |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Architecture decision records |
| [docs/CODEBASE.md](docs/CODEBASE.md) | Package tree, request lifecycle, adding an RPC, test seams |
| [docs/TESTING.md](docs/TESTING.md) | Test layers, tags, commands, coverage, local checks |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | Benchmark methodology, commands, output schema, expected ratios |
| [docs/REQUIREMENTS_MAPPING.md](docs/REQUIREMENTS_MAPPING.md) | Course principles and proposal components mapped to code and commands |
| [tests/README.md](tests/README.md) | Test layout, shared harness, white-box exceptions |

## Current limitations and future work

- Reads are leader-coordinated. There is no leader lease, read-index protocol,
  or follower-local read yet; every read costs one metadata Raft round trip plus
  fragment fetches.
- Membership is fixed at five nodes. Adding or removing a node would change
  deterministic placement; dynamic membership and automatic data migration are
  out of scope.
- "Rebalancing" here means refilling returning nodes and sweeping orphans, not
  migrating keys between a changed node set.
- Repair state is derived from metadata and the in-process queue; there is no
  persistent repair journal. A leader restart rebuilds work from metadata and
  the periodic sweep.
- Orphan fragments from an ambiguous metadata commit or a process death during
  garbage collection are removed by the local sweep after a grace period.
- TLS is optional and not provisioned automatically; the Compose cluster runs in
  plaintext on a private Docker network.
- Authentication is a single shared bearer token, not per-user authorization.
  There is no audit log. See [docs/SECURITY.md](docs/SECURITY.md).
- A 3x replication mode is deliberately not implemented; the course scenario is
  five nodes with five full copies for the replication comparison.
