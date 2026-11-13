package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os/signal"
	"syscall"

	"distributed-kv-store/internal/bench"
)

func runStorage(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("storage", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodesFlag := fs.String("nodes", "", "id=host:port,... (default GATEWAY_NODES)")
	mode := fs.String("mode", "rs32", "redundancy mode: rs32 or replication")
	values := fs.Int("values", 1000, "number of keys to write")
	valueSize := fs.Int("value-size", 4096, "value size in bytes")
	prefix := fs.String("key-prefix", "storage", "key prefix for the dataset")
	seed := fs.Int64("seed", 1, "deterministic seed")
	output := fs.String("output", "bench-results", "result directory")
	runID := fs.String("run-id", "", "run directory name (default timestamp)")
	fs.Usage = func() { fmt.Fprint(stderr, "Usage: bench storage [flags]\n\n"); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return flagExit(err)
	}

	opts := bench.DatasetOptions{Mode: *mode, Values: *values, ValueSize: *valueSize, Prefix: *prefix, Seed: *seed}
	if _, err := opts.Normalize(); err != nil {
		fmt.Fprintf(stderr, "bench storage: %v\n", err)
		return 2
	}

	nodes, closeNodes, err := dial(*nodesFlag, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "bench storage: %v\n", err)
		return 1
	}
	defer closeNodes()
	_ = output
	_ = runID

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result, err := bench.MeasureStorage(ctx, nodes, opts)
	if err != nil {
		fmt.Fprintf(stderr, "bench storage: %v\n", err)
		return 1
	}

	printStorageSummary(stdout, *result)
	return 0
}

// printStorageSummary renders payload and disk ratios separately so RS is never
// described as a percentage of the original.
func printStorageSummary(w io.Writer, result bench.StorageResult) {
	fmt.Fprintf(w, "mode=%s keys=%d value_size=%d logical_bytes=%d fragment_bytes=%d disk_bytes=%d payload_ratio=%.3fx disk_ratio=%.3fx\n",
		result.Mode, result.Keys, result.ValueSize, result.LogicalBytes, result.FragmentBytes, result.DiskBytes, result.PayloadRatio, result.DiskRatio)
	for _, node := range result.Nodes {
		fmt.Fprintf(w, "  %s keys=%d fragments=%d fragment_bytes=%d disk_bytes=%d\n",
			node.NodeID, node.Keys, node.Fragments, node.FragmentBytes, node.DiskBytes)
	}
}
