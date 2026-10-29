# Gateway & CLI
The goal is to build the CLI client interface (`put <key> <val>`, `get <key>`, and redundancy mode toggles) and the consistent hash routing gateway.

### How to run your code without installing Go:
docker run --rm -it -v $(pwd)/..:/app -w /app/gateway golang:1.22 go run main.go put test-key test-val