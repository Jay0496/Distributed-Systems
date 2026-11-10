// Command bench is the placeholder entrypoint for the benchmark harness. It
// currently prints usage and exits successfully; the measurement scenarios land
// with the benchmark task.
package main

import (
	"fmt"
	"os"
)

func main() {
	usage := `Usage: bench [flags]

The benchmark harness is not implemented yet. It will measure write/read
throughput and total storage amplification for rs32 versus replication.

Run it with:
  docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go run ./cmd/bench
`
	fmt.Fprint(os.Stdout, usage)
}
