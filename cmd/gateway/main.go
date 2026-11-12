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
	// Help must not require a configured cluster: it is the first thing a new
	// operator runs, including inside `docker run --entrypoint /app/gateway`.
	if len(args) > 0 {
		switch args[0] {
		case "help", "--help", "-h":
			fmt.Fprint(os.Stdout, gateway.Usage())
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
	return gateway.NewCLI(client, os.Stdout, os.Stderr).Run(context.Background(), args)
}
