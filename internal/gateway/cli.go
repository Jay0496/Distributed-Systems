package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"

	pb "distributed-kv-store/proto"
)

// Service is the client surface the CLI needs. It lets command parsing and
// rendering be tested against a fake without a gRPC transport.
type Service interface {
	Put(ctx context.Context, req *pb.PutRequest) (string, *pb.PutResponse, error)
	Get(ctx context.Context, key string) (string, *pb.GetResponse, error)
}

// CLI parses commands, calls the gateway client, and renders the result.
type CLI struct {
	svc    Service
	out    io.Writer
	errOut io.Writer
}

// NewCLI builds a CLI bound to a gateway service and its output streams.
func NewCLI(svc Service, out, errOut io.Writer) *CLI {
	return &CLI{svc: svc, out: out, errOut: errOut}
}

// Run executes one command. A nil error means exit status zero; help returns
// nil as well. Any returned error should be printed by the caller and mapped to
// a non-zero exit status.
func (c *CLI) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(c.out, Usage())
		return errors.New("a command is required")
	}

	switch args[0] {
	case "help", "--help", "-h":
		fmt.Fprint(c.out, Usage())
		return nil
	case "put":
		if len(args) != 3 {
			return errors.New("put requires a key and value")
		}
		node, resp, err := c.svc.Put(ctx, &pb.PutRequest{Key: args[1], Value: []byte(args[2])})
		if err != nil {
			return fmt.Errorf("put failed: %w", err)
		}
		if !resp.GetSuccess() {
			return fmt.Errorf("put rejected by %s: %s", node, resp.GetMessage())
		}
		fmt.Fprintf(c.out, "put committed on %s: key=%q version=%d mode=%s shards=%d/5\n",
			node, args[1], resp.GetVersion(), resp.GetMode(), resp.GetShardsStored())
		return nil
	case "get":
		if len(args) != 2 {
			return errors.New("get requires a key")
		}
		node, resp, err := c.svc.Get(ctx, args[1])
		if err != nil {
			return fmt.Errorf("get failed: %w", err)
		}
		fmt.Fprintf(c.out, "%s\n", resp.GetValue())
		fmt.Fprintf(c.errOut, "served by %s (mode=%s version=%d)\n", node, resp.GetMode(), resp.GetVersion())
		return nil
	default:
		fmt.Fprint(c.out, Usage())
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// Usage returns the command help text.
func Usage() string {
	return `Usage:
  gateway put <key> <value>
  gateway get <key>
  gateway help

Environment:
  GATEWAY_NODES    required, id=host:port,... (for example node1=localhost:50051,node2=localhost:50052)
  GATEWAY_VNODES   virtual nodes per host (default 100)
  GATEWAY_TIMEOUT  per-attempt RPC timeout (default 15s)
  GATEWAY_RETRIES  retries after the first attempt (default 3)
  AUTH_TOKEN       optional bearer token
  MAX_MESSAGE_SIZE maximum gRPC message size (default 32 MiB)
`
}
