package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/signal"
	"strings"
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

	artifacts := bench.RunArtifacts{
		Meta: bench.NewMeta("", map[string]string{
			"command":     "run",
			"mode":        opts.Mode,
			"concurrency": fmt.Sprint(opts.Concurrency),
			"duration":    opts.Duration.String(),
			"warmup":      opts.Warmup.String(),
			"value_size":  fmt.Sprint(opts.ValueSize),
			"keys":        fmt.Sprint(opts.Keys),
			"read_ratio":  fmt.Sprint(opts.ReadRatio),
			"hot_keys":    fmt.Sprint(opts.HotKeys),
			"seed":        fmt.Sprint(opts.Seed),
			"nodes":       strings.Join(nodes.OrderedNodes(), ","),
		}),
		Load: result,
	}
	dir := runDirectory(*output, *runID, artifacts.Meta.Timestamp)
	if err := bench.WriteRun(dir, artifacts); err != nil {
		fmt.Fprintf(stderr, "bench run: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, bench.RenderSummary(artifacts))
	fmt.Fprintf(stdout, "\nresults: %s\n", dir)
	return 0
}
