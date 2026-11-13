// Command gateway is the client CLI. It reads GATEWAY_NODES and the other
// gateway environment variables, selects an entry node with the consistent
// hash ring, and retries transient failures against ring successors.
package main

import (
	"context"
	"fmt"
	"os"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/gateway"
	"distributed-kv-store/internal/logging"
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/version"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		logger := logging.New(os.Stderr, os.Getenv("LOG_FORMAT"), "gateway",
			"version", version.Value())
		logger.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// Help must not require a configured cluster: it is the first thing a new
	// operator runs, including inside `docker run --entrypoint /app/gateway`.
	if len(args) > 0 {
		switch args[0] {
		case "help", "--help", "-h":
			fmt.Fprint(os.Stdout, gateway.Usage())
			return nil
		case "version", "--version":
			fmt.Fprintln(os.Stdout, version.Value())
			return nil
		}
	}

	cfg, err := config.LoadGatewayConfig()
	if err != nil {
		return err
	}
	client, err := gateway.NewClient(cfg)
	if err != nil {
		return err
	}

	// One ID per CLI invocation correlates the gateway attempt with the entry
	// node, the leader, and every fragment call.
	ctx := rpc.WithRequestID(context.Background(), rpc.NewRequestID())
	return gateway.NewCLI(client, os.Stdout, os.Stderr).Run(ctx, args)
}
