# Security

The project has one authentication mechanism: a shared bearer token. It is
deliberately small and is not a replacement for TLS client certificates, OAuth,
JWTs, or per-user authorization. This document states what is protected, what is
not, and which claims the implementation does not make.

## Threat model

### Assets

- Key values stored as RS shards or full copies.
- Key metadata: names, stripe IDs, modes, sizes, checksums, versions, placements.
- Cluster configuration and availability.
- The shared `AUTH_TOKEN`.

### Trust boundaries

- The five node containers and the Compose network
  `distributed-systems_cluster-net` are the cluster boundary.
- The host has full access to the Docker socket, volumes, and container
  environment. A host operator is trusted.
- Any process that can reach a node's gRPC port is outside the cluster boundary
  and must present the token when auth is enabled.
- The Raft port is a separate plaintext channel on the same Docker network.

### Actors

| Actor | Capability |
| --- | --- |
| Host operator | Full control: can read volumes, inspect containers, stop nodes, and read the token from the process environment |
| Authorized client | Anyone holding the token and network access: full API access, including `put`, `get`, `delete`, `mode`, `repair`, and `stats` |
| Unauthenticated client | Only `/grpc.health.v1.Health/Check` and `/grpc.health.v1.Health/Watch` when auth is enabled |
| Network observer | Can read plaintext gRPC and Raft traffic unless TLS is enabled |

### Threats and mitigations

| Threat | Mitigation | Residual risk |
| --- | --- | --- |
| Unauthorized API use | Shared bearer token on every RPC except the two health methods; constant-time comparison | Anyone with the token has full access |
| Token theft from network traffic | Optional `TLS_ENABLED` on the gRPC server and forwarding clients | TLS is off by default and not provisioned; Raft traffic stays plaintext |
| Token leakage in logs | Tokens are never printed; errors name the node and operation, not the credential | The token is visible in `docker inspect` and the process environment |
| Tampered shard or value | SHA-256 per-shard checksums and a committed value checksum; reads never return unverified bytes | Checksums detect corruption, not forgery by someone with write access |
| Denial of service | `MAX_VALUE_SIZE` (8 MiB), `MAX_MESSAGE_SIZE` (32 MiB), bounded fragment staging (64 per node), bounded repair queue | No rate limiting or per-client quotas |
| Unauthorized metadata edits | All metadata changes go through Raft with validation | A host operator or volume write can bypass the API |
| Data remanence | None beyond deleting volumes | No encryption at rest |
| Audit or attribution | None | No audit log; the token identifies the cluster, not a person |

## Bearer token

- Set `AUTH_TOKEN` to the same non-empty value on every server, the gateway, and
  any tool that calls the gRPC API directly.
- Clients send `authorization: Bearer <token>`. The gateway and every
  server-internal client (Raft forwarding, fragment upload/download/delete,
  admin probes, the failure detector, and the repairer) build their dial options
  through `internal/rpc.DialOptions`, so `AUTH_TOKEN` and `MAX_MESSAGE_SIZE` are
  applied consistently.
- The server compares tokens with `crypto/subtle.ConstantTimeCompare` over
  fixed-size SHA-256 digests, avoiding an early exit that would leak length.
- Missing or invalid credentials return `Unauthenticated`.
- The token is never printed. Error messages name the node and operation, not
  the credential, and the client helper never includes the token in dial errors.

`AUTH_TOKEN` is **not** per-user authorization. There are no users, roles,
tenants, or permission scopes. Anyone with the token and network access can read
every key, overwrite or delete data, change the cluster default mode, trigger
repairs, and read operational counters. There is no audit log, so the system
cannot say who performed an action.

## Startup behavior

- `AUTH_TOKEN` set: `cmd/server` installs the unary and stream interceptors and
  calls `Server.EnableAuth`. `Server.CheckAuth` refuses to start if the token is
  set but the interceptors were not installed.
- `AUTH_TOKEN` empty: authentication is disabled and `cmd/server` logs a warning
  at startup. Use this only on a trusted local network.

## Health-check exemption

Only these methods bypass authentication:

- `/grpc.health.v1.Health/Check`
- `/grpc.health.v1.Health/Watch`

The exemption keeps Docker Compose health checks and the failure detector
working when auth is enabled. Every other RPC, including `NodeStatus`, `Stats`,
`ClusterHealth`, and `Repair`, requires the token. Health responses are a
liveness signal only; they do not verify authorization.

## Transport and TLS

Bearer tokens are sent as plaintext gRPC metadata unless the existing optional
TLS mode is enabled. Enable `TLS_ENABLED` and configure the certificate files
when the deployment crosses an untrusted network:

| Variable | Meaning |
| --- | --- |
| `TLS_ENABLED` | `true` enables TLS on the gRPC server and forwarding clients |
| `TLS_CERT_FILE` | Server certificate |
| `TLS_KEY_FILE` | Server private key |
| `TLS_CA_FILE` | CA used by forwarding clients to verify peers |

The gateway client currently uses plaintext transport. Add a gateway TLS client
configuration before exposing it outside the Compose network. Certificates are
not issued or rotated automatically, and the Compose health check dials with
insecure credentials, so enabling TLS in Compose also requires updating the
health-check command. The Raft link is plain TCP with no TLS and no
authentication; it is acceptable only on the private Docker network.

## Secret handling in Compose

- Compose interpolates `AUTH_TOKEN` from the host environment:
  `AUTH_TOKEN: ${AUTH_TOKEN:-}`. The value is passed by name, not echoed by the
  scripts, and `up`/`demo` print only `auth: enabled` or `auth: disabled`.
- The token is still present in each container's environment and is visible to
  `docker inspect` and to anyone with Docker socket access. Do not treat it as a
  secret from the host operator.
- Do not hard-code a token in `docker-compose.yml`, a script, or a committed
  `.env` file. Supply it through the host environment or an external secret
  store.
- Rotate by restarting the whole cluster, the gateway, and any tools with the new
  value. There is no online rotation path. If auth causes a lockout, remove
  `AUTH_TOKEN` from the server environment and restart; authentication is
  disabled again and the startup warning returns.

## What is explicitly out of scope

- Encryption at rest. SQLite, BoltDB, snapshots, and volumes are plaintext.
- Per-user authentication, authorization, roles, tenants, or API keys.
- Audit logging, request attribution, and tamper-evident logs.
- Certificate provisioning, rotation, or mutual TLS.
- TLS or authentication on the Raft transport.
- Network policy beyond the Compose bridge network.
- Rate limiting, per-client quotas, and abuse detection.
- Hardening the host, Docker daemon, or volume permissions.
- Secret managers or external key management.

## Reporting a security issue

This is a course project, not a production service. If you find a security
problem, open an issue in the repository with the affected component and a
minimal reproduction. Do not include real credentials or data in the report.
