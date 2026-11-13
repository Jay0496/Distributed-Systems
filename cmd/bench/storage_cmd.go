package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os/signal"
	"strings"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result, err := bench.MeasureStorage(ctx, nodes, opts)
	if err != nil {
		fmt.Fprintf(stderr, "bench storage: %v\n", err)
		return 1
	}

	artifacts := bench.RunArtifacts{
		Meta: bench.NewMeta("", map[string]string{
			"command":    "storage",
			"mode":       opts.Mode,
			"values":     fmt.Sprint(opts.Values),
			"value_size": fmt.Sprint(opts.ValueSize),
			"key_prefix": opts.Prefix,
			"seed":       fmt.Sprint(opts.Seed),
			"nodes":      strings.Join(nodes.OrderedNodes(), ","),
		}),
		Storage: []bench.StorageResult{*result},
	}
	dir := runDirectory(*output, *runID, artifacts.Meta.Timestamp)
	if err := bench.WriteRun(dir, artifacts); err != nil {
		fmt.Fprintf(stderr, "bench storage: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, bench.RenderSummary(artifacts))
	fmt.Fprintf(stdout, "\nresults: %s\n", dir)
	return 0
}
