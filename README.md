# Distributed NoSQL Key-Value Store

## Zero-Env Development Guide
You do **not** need to install Go locally. All code runs inside a temporary Go Docker container.

### How to test your code by task:

* **Task 1: CLI Client & Routing Gateway** 
  (Builds the command-line interface and consistent hash router)[cite: 3, 5]
  ```bash
  docker run --rm -it -v $(pwd):/app -w /app/gateway golang:1.22 go run main.go put test-key test-val

* **Task 2: SQLite Local Persistence**
  (Builds the embedded SQLite3 storage daemons and disk-write interfaces)\
  ```bash
  docker run --rm -v $(pwd):/app -w /app/storage golang:1.22 go test -v ./...
