package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"distributed-kv-store/internal/bench"
	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/rpc"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// dial resolves node addresses and opens one lazy gRPC connection per node.
// Settings come from the same environment as the gateway, with BENCH_TOKEN
// overriding AUTH_TOKEN for this tool only.
func dial(nodesFlag string, stderr io.Writer) (bench.Nodes, func(), error) {
	cfg, err := connectionConfig(nodesFlag)
	if err != nil {
		return nil, nil, err
	}
	opts, err := rpc.DialOptions(cfg.Token, cfg.MaxMessageBytes)
	if err != nil {
		return nil, nil, err
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	nodes := make(bench.Nodes, 0, len(cfg.Order))
	closers := make([]*grpc.ClientConn, 0, len(cfg.Order))
	for _, id := range cfg.Order {
		address := cfg.Nodes[id]
		conn, err := grpc.DialContext(context.Background(), address, opts...)
		if err != nil {
			for _, existing := range closers {
				_ = existing.Close()
			}
			return nil, nil, fmt.Errorf("dial %s (%s): %w", id, address, err)
		}
		closers = append(closers, conn)
		nodes = append(nodes, bench.Node{ID: id, API: pb.NewStorageServiceClient(conn)})
	}
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes configured")
	}
	closeAll := func() {
		for _, conn := range closers {
			_ = conn.Close()
		}
	}
	return nodes, closeAll, nil
}

// connectionConfig builds the benchmark's connection settings without requiring
// GATEWAY_NODES to be present when --nodes was supplied explicitly.
func connectionConfig(nodesFlag string) (config.GatewayConfig, error) {
	raw := strings.TrimSpace(nodesFlag)
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("GATEWAY_NODES"))
	}
	if raw == "" {
		return config.GatewayConfig{}, fmt.Errorf("--nodes or GATEWAY_NODES is required (example: --nodes node1=localhost:50051,node2=localhost:50052)")
	}
	nodes := config.ParsePeerMap(raw)
	if len(nodes) == 0 {
		return config.GatewayConfig{}, fmt.Errorf("--nodes %q contains no valid id=host:port entries", raw)
	}
	order := make([]string, 0, len(nodes))
	for id := range nodes {
		order = append(order, id)
	}
	sort.Strings(order)

	token := strings.TrimSpace(os.Getenv("BENCH_TOKEN"))
	if token == "" {
		token = strings.TrimSpace(os.Getenv("AUTH_TOKEN"))
	}
	return config.GatewayConfig{
		Nodes:           nodes,
		Order:           order,
		Timeout:         envDuration("GATEWAY_TIMEOUT", config.DefaultGatewayTimeout),
		Token:           token,
		MaxMessageBytes: envInt("MAX_MESSAGE_SIZE", config.DefaultMaxMessageSize),
	}, nil
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func runDirectory(output, runID string, timestamp time.Time) string {
	if strings.TrimSpace(output) == "" {
		output = "bench-results"
	}
	if strings.TrimSpace(runID) == "" {
		runID = bench.RunID(timestamp)
	}
	return filepath.Join(output, runID)
}

func mustTime(raw string) time.Time {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}
	}
	value, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}
	}
	return value
}

func flagExit(err error) int {
	if err == flag.ErrHelp {
		return 0
	}
	return 2
}
