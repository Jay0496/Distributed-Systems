// Command bench is the benchmark harness. It measures put/get latency and
// throughput, storage overhead for rs32 versus replication, recovery time, and
// writes CSV/JSON/report artifacts.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stdout, usage())
		return 0
	}
	switch args[0] {
	case "run":
		return runLoad(args[1:], stdout, stderr)
	case "storage":
		return runStorage(args[1:], stdout, stderr)
	case "recovery":
		return runRecovery(args[1:], stdout, stderr)
	case "help", "--help", "-h":
		fmt.Fprint(stdout, usage())
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage())
		return 2
	}
}

func usage() string {
	return `Usage: bench <command> [flags]

Commands:
  run       mixed put/get load with latency percentiles and throughput
  storage   write a dataset, force snapshots and WAL checkpoints, measure ratios
  recovery  measure degraded and repair time through ClusterHealth

Run "bench <command> --help" for command flags.

Connection settings:
  --nodes defaults to GATEWAY_NODES. If neither is set, the command exits with
  a clear error. BENCH_TOKEN overrides AUTH_TOKEN for this tool. MAX_MESSAGE_SIZE
  and GATEWAY_TIMEOUT are read from the same environment as the gateway.

Examples:
  bench run --mode rs32 --duration 60s --concurrency 32 --output bench-results
  bench storage --mode replication --values 1000 --value-size 4096
  bench recovery --measure-only --start-time 2026-01-02T15:04:05Z
`
}
