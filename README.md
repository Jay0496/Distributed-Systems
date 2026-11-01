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

## Phase 1: Docker Network & gRPC Server Skeleton
- **Container Topology**: Multi-node cluster (`node1` through `node5`) running on a custom bridge network (`cluster-net`) via Docker Compose.
- **gRPC Communication**: Strongly typed RPC contracts compiled via Protocol Buffers (`proto/storage.proto`) supporting `Put`, `Get`, and `Heartbeat`.
- **Persistent Storage**: Each node automatically initializes its own isolated embedded SQLite database daemon on startup.

### How to Run the Cluster:
```bash
docker compose up --build
```

### How to Test the Cluster
```bash
docker run --rm -it --network distributed-systems_cluster-net -v $(pwd):/app -w /app golang:1.22-alpine sh -c "
  go test -v ./tests/...
"
```

### Tests cover:
- Connection Handshake: Establishes an insecure gRPC client connection to node1 over the Docker bridge network (cluster-net).
- Heartbeat Validation: Dispatches a HeartbeatRequest RPC to ensure the server skeleton is successfully handling requests, reading from the underlying network layer, and returning valid status responses.