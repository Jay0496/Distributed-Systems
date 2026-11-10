# Gateway & CLI

The CLI client (`put <key> <val>`, `get <key>`, and redundancy mode toggles) and
the consistent-hash routing gateway. The command lives in `cmd/gateway`; the hash
ring lives in `internal/gateway`.

### How to run your code without installing Go

From the repository root:

```powershell
docker run --rm -it -v "${PWD}:/app" -w /app golang:1.22 go run ./cmd/gateway put test-key test-val
```
