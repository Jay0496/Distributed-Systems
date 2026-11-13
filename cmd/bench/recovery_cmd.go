package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distributed-kv-store/internal/bench"
)

func runRecovery(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("recovery", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodesFlag := fs.String("nodes", "", "id=host:port,... (default GATEWAY_NODES)")
	scenario := fs.String("scenario", "one-node", "scenario label")
	measureOnly := fs.Bool("measure-only", false, "poll health only; the script stops and starts containers")
	seedOnly := fs.Bool("seed-only", false, "write and verify the recovery dataset, then exit")
	startTime := fs.String("start-time", "", "RFC3339 time the failure was injected")
	degradedTime := fs.String("degraded-time", "", "RFC3339 time degradation was observed")
	restartTime := fs.String("restart-time", "", "RFC3339 time nodes were restarted")
	timeout := fs.Duration("timeout", 5*time.Minute, "repair wait timeout")
	pollInterval := fs.Duration("poll-interval", time.Second, "health poll interval")
	seedKeys := fs.Int("seed-keys", 100, "keys written before a stop/start scenario")
	valueSize := fs.Int("value-size", 4096, "value size in bytes")
	prefix := fs.String("key-prefix", "recovery", "key prefix for seeded keys")
	mode := fs.String("mode", "rs32", "mode for seeded keys")
	seed := fs.Int64("seed", 1, "deterministic seed")
	output := fs.String("output", "bench-results", "result directory")
	runID := fs.String("run-id", "", "run directory name (default timestamp)")
	fs.Usage = func() { fmt.Fprint(stderr, "Usage: bench recovery [flags]\n\n"); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		return flagExit(err)
	}

	opts := bench.RecoveryOptions{
		Scenario:     *scenario,
		StartTime:    mustTime(*startTime),
		DegradedTime: mustTime(*degradedTime),
		RestartTime:  mustTime(*restartTime),
		Timeout:      *timeout,
		PollInterval: *pollInterval,
		SeedKeys:     *seedKeys,
		SeedPrefix:   *prefix,
		ValueSize:    *valueSize,
		Seed:         *seed,
		SeedOnly:     *seedOnly,
		MeasureOnly:  *measureOnly,
		Mode:         *mode,
	}
	if *startTime != "" && opts.StartTime.IsZero() {
		fmt.Fprintf(stderr, "bench recovery: invalid --start-time %q (want RFC3339)\n", *startTime)
		return 2
	}
	if *degradedTime != "" && opts.DegradedTime.IsZero() {
		fmt.Fprintf(stderr, "bench recovery: invalid --degraded-time %q (want RFC3339)\n", *degradedTime)
		return 2
	}
	if *restartTime != "" && opts.RestartTime.IsZero() {
		fmt.Fprintf(stderr, "bench recovery: invalid --restart-time %q (want RFC3339)\n", *restartTime)
		return 2
	}

	nodes, closeNodes, err := dial(*nodesFlag, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "bench recovery: %v\n", err)
		return 1
	}
	defer closeNodes()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *seedOnly {
		dataset, err := bench.SeedRecoveryDataset(ctx, nodes, opts)
		if err != nil {
			fmt.Fprintf(stderr, "bench recovery: seed: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "seeded %d keys (%d logical bytes) with five fragments each\n",
			len(dataset.Keys), dataset.LogicalBytes)
		return 0
	}

	if !*measureOnly {
		if _, err := bench.SeedRecoveryDataset(ctx, nodes, opts); err != nil {
			fmt.Fprintf(stderr, "bench recovery: seed: %v\n", err)
			return 1
		}
	}

	result, err := bench.PollRecovery(ctx, bench.NodesHealthSource(nodes), opts)
	if err != nil {
		if result != nil {
			artifacts := recoveryArtifacts(result, nodes)
			dir := runDirectory(*output, *runID, artifacts.Meta.Timestamp)
			if writeErr := bench.WriteRun(dir, artifacts); writeErr != nil {
				fmt.Fprintf(stderr, "bench recovery: write results: %v\n", writeErr)
			}
		}
		fmt.Fprintf(stderr, "bench recovery: %v\n", err)
		return 1
	}

	artifacts := recoveryArtifacts(result, nodes)
	dir := runDirectory(*output, *runID, artifacts.Meta.Timestamp)
	if err := bench.WriteRun(dir, artifacts); err != nil {
		fmt.Fprintf(stderr, "bench recovery: %v\n", err)
		return 1
	}
	fmt.Fprint(stdout, bench.RenderSummary(artifacts))
	fmt.Fprintf(stdout, "\nresults: %s\n", dir)
	return 0
}

func recoveryArtifacts(result *bench.RecoveryResult, nodes bench.Nodes) bench.RunArtifacts {
	return bench.RunArtifacts{
		Meta: bench.NewMeta("", map[string]string{
			"command":  "recovery",
			"scenario": result.Scenario,
			"nodes":    strings.Join(nodes.OrderedNodes(), ","),
		}),
		Recovery: []bench.RecoveryResult{*result},
	}
}
