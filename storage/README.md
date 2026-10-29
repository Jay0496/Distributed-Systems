# SQLite Persistence
The Your goal is to set up the embedded SQLite3 local storage daemons and disk-write interfaces for each node container.

### How to run your tests without installing Go:
docker run --rm -v $(pwd)/..:/app -w /app/storage golang:1.22 go test -v