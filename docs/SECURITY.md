# Security

The project has one authentication mechanism: a shared bearer token. It is
deliberately small and is not a replacement for TLS client certificates, OAuth,
JWT, or per-user authorization.

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

## Startup behavior

- `AUTH_TOKEN` set: `cmd/server` installs the unary and stream interceptors and
  calls `Server.EnableAuth`. `Server.CheckAuth` refuses to start if the token is
  set but the interceptors were not installed.
- `AUTH_TOKEN` empty: authentication is disabled and `cmd/server` logs a warning
  at startup.

## Health-check exemption

Only these methods bypass authentication:

- `/grpc.health.v1.Health/Check`
- `/grpc.health.v1.Health/Watch`

The exemption keeps Docker Compose health checks and the failure detector
working when auth is enabled. Every other RPC, including `NodeStatus`, `Stats`,
`ClusterHealth`, and `Repair`, requires the token.

## Transport caveat

Bearer tokens are sent as plaintext gRPC metadata unless the existing optional
TLS mode is enabled. Enable `TLS_ENABLED` and configure the certificate files
when the deployment crosses an untrusted network. The gateway's dialer currently
uses plaintext transport; add a gateway TLS client configuration before exposing
it outside the Compose network.

## Operational notes

- Rotate the token by restarting the cluster with the new value. There is no
  online rotation path.
- If auth causes a lockout, remove `AUTH_TOKEN` from the server environment and
  restart; authentication is disabled again and the startup warning returns.
- Health probes remain unauthenticated by design, so they do not verify
  authorization. Treat a healthy response as a liveness signal only.
