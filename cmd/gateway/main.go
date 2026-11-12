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
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.LoadGatewayConfig()
	if err != nil {
		return err
	}
	client, err := gateway.NewClient(cfg)
	if err != nil {
		return err
	}
	return gateway.NewCLI(client, os.Stdout, os.Stderr).Run(context.Background(), args)
}
