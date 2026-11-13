# Gateway and CLI

The gateway code moved to `cmd/gateway` (process wiring and flag parsing) and
`internal/gateway` (consistent-hash entry ring, retrying client, CLI commands,
and rendering). There is no standalone single-file Go entrypoint: build the
binary in Docker, or use the Compose `gateway` service.

The maintained documentation is:

- [docs/USAGE.md](../../docs/USAGE.md) — full CLI reference, flags, environment
  variables, exit codes, JSON output, and authentication.
- [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#gateway-and-retries) —
  routing, retry, and auth design.
- [docs/SECURITY.md](../../docs/SECURITY.md) — token handling and limitations.
- [docs/DEMO.md](../../docs/DEMO.md) — scripted end-to-end walkthrough.

Quick examples:

```powershell
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway get demo-key
docker compose run --rm gateway status
docker compose run --rm gateway health
```
