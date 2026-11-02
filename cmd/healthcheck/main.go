// Command healthcheck probes a node's gRPC health endpoint. It is used as the
// Docker Compose healthcheck so dependent nodes start only when node1's
// process is accepting RPCs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC address to probe")
	timeout := flag.Duration("timeout", 3*time.Second, "probe timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		fmt.Fprintf(os.Stderr, "health check %s: %v\n", *addr, err)
		os.Exit(1)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		fmt.Fprintf(os.Stderr, "node %s status: %s\n", *addr, resp.GetStatus())
		os.Exit(1)
	}
}
