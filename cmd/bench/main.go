// Command bench is the placeholder entrypoint for the benchmark harness. The
// measurement scenarios and real load flags land with the benchmark task. Until
// then it prints usage, accepts --help, and exits successfully so deployment
// scripts and Compose can exercise the one-shot tools profile.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage())
	}
	help := flag.Bool("help", false, "print benchmark help")
	flag.Parse()

	if *help || flag.NArg() > 0 {
		fmt.Fprint(os.Stdout, usage())
		if !*help && flag.NArg() > 0 {
			os.Exit(2)
		}
		return
	}

	fmt.Fprint(os.Stdout, usage())
}

func usage() string {
	return `Usage: bench [flags]

The benchmark harness is not implemented yet. It will measure write/read
throughput and total storage amplification for rs32 versus replication.

Run it inside the deployment image:
  docker compose run --rm bench --help

Or directly from source with Docker:
  docker run --rm -v "${PWD}:/app" -w /app golang:1.22 go run ./cmd/bench
`
}
