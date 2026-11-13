package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/signal"
	"syscall"
	"time"

	"distributed-kv-store/internal/bench"
)

func runLoad(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodesFlag := fs.String("nodes", "", "id=host:port,... (default GATEWAY_NODES)")
	mode := fs.String("mode", "rs32", "redundancy mode: rs32 or replication")
	concurrency := fs.Int("concurrency", 32, "parallel workers")
	duration := fs.Duration("duration", 60*time.Second, "measured duration")
	warmup := fs.Duration("warmup", 10*time.Second, "warmup discarded before measuring")
	valueSize := fs.Int("value-size", 4096, "value size in bytes")
	keys := fs.Int("keys", 10000, "key-space size")
	readRatio := fs.Float64("read-ratio", 0.5, "fraction of reads in the mix")
	hotKeys := fs.Bool("hot-keys", false, "share the key range across workers")
	seed := fs.Int64("seed", 1, "deterministic seed")
	timeout := fs.Duration("timeout", 10*time.Second, "per-RPC timeout")
	output := fs.String("output", "bench-results", "result directory")
	runID := fs.String("run-id", "", "run directory name (default timestamp)")
	fs.Usage = func() { fmt.Fprint(stderr, "Usage: bench run [flags]\n\n"); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return flagExit(err)
	}

	opts := bench.LoadOptions{
		Mode:        *mode,
		Concurrency: *concurrency,
		Duration:    *duration,
		Warmup:      *warmup,
		ValueSize:   *valueSize,
		Keys:        *keys,
		ReadRatio:   *readRatio,
		HotKeys:     *hotKeys,
		Seed:        *seed,
		Timeout:     *timeout,
	}
	if _, err := opts.Normalize(); err != nil {
		fmt.Fprintf(stderr, "bench run: %v\n", err)
		return 2
	}

	nodes, closeNodes, err := dial(*nodesFlag, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "bench run: %v\n", err)
		return 1
	}
	defer closeNodes()
	_ = output
	_ = runID

	runner, err := bench.NewLoadRunner(nodes, opts)
	if err != nil {
		fmt.Fprintf(stderr, "bench run: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result, err := runner.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(stderr, "bench run: %v\n", err)
		return 1
	}

	printLoadSummary(stdout, result)
	return 0
}

// printLoadSummary renders the console table before the CSV/JSON writers are
// used. It keeps the load generator runnable without a result directory.
func printLoadSummary(w io.Writer, result *bench.LoadResult) {
	fmt.Fprintf(w, "mode=%s concurrency=%d duration=%s warmup=%s value_size=%d keys=%d read_ratio=%.2f nodes=%d\n",
		result.Mode, result.Concurrency, result.Measured, result.Warmup, result.ValueSize, result.Keys, result.ReadRatio, result.Nodes)
	fmt.Fprintln(w, "operation  count  errors  p50_ms  p95_ms  p99_ms  max_ms  ops_per_sec  bytes_per_sec")
	for _, op := range []bench.OpStats{result.Put, result.Get} {
		fmt.Fprintf(w, "%-10s %6d %7d %7s %7s %7s %7s %12.2f %14.2f\n",
			op.Operation, op.Count, op.Errors,
			bench.FormatDuration(op.P50), bench.FormatDuration(op.P95), bench.FormatDuration(op.P99), bench.FormatDuration(op.Max),
			op.OpsPerSecond, op.BytesPerSecond)
	}
	fmt.Fprintf(w, "error_rate=%.3f%% degraded_writes=%d retries=%d\n", result.ErrorRate*100, result.Degraded, result.Retries)
}
