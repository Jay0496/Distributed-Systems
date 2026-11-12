# Gateway & CLI

The gateway moved to `cmd/gateway` (process wiring) and `internal/gateway`
(consistent-hash entry ring, retrying client, CLI commands, and rendering).

Usage, environment variables, retry behavior, and authentication are documented
in the root [README.md](../README.md#gateway-cli) and
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md#gateway-and-retries). Security
notes live in [docs/SECURITY.md](../docs/SECURITY.md).

```powershell
docker compose run --rm gateway put demo-key "hello" --mode rs32
docker compose run --rm gateway get demo-key
docker compose run --rm gateway status
```
