package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/rpc"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DialFunc opens a gRPC connection to one gateway node. It is a seam so tests
// can replace the transport with an in-memory listener.
type DialFunc func(ctx context.Context, target string) (*grpc.ClientConn, error)

type callFunc func(ctx context.Context, client pb.StorageServiceClient) error

// Client is the gateway's gRPC client. It selects an entry node with the hash
// ring, retries transient failures across ring successors, and re-resolves the
// leader after an Aborted response.
type Client struct {
	cfg    config.GatewayConfig
	router *Router
	dial   DialFunc
	sleep  func(time.Duration)
}

// NewClient builds a client from gateway configuration. It never falls back to
// a hardcoded address: an empty node map is a configuration error.
func NewClient(cfg config.GatewayConfig) (*Client, error) {
	cfg = normalizeClientConfig(cfg)
	if len(cfg.Nodes) == 0 {
		return nil, errors.New("no gateway nodes configured; set GATEWAY_NODES")
	}

	dial, err := newDialer(cfg)
	if err != nil {
		return nil, err
	}
	return newClientWithDialer(cfg, dial), nil
}

// newClientWithDialer is the test seam for NewClient.
func newClientWithDialer(cfg config.GatewayConfig, dial DialFunc) *Client {
	cfg = normalizeClientConfig(cfg)
	return &Client{
		cfg:    cfg,
		router: NewRouter(cfg.NodeAddrs(), cfg.VNodes),
		dial:   dial,
		sleep:  time.Sleep,
	}
}

// normalizeClientConfig fills defaults for callers that build GatewayConfig
// directly instead of going through config.LoadGatewayConfig.
func normalizeClientConfig(cfg config.GatewayConfig) config.GatewayConfig {
	if cfg.VNodes <= 0 {
		cfg.VNodes = config.DefaultGatewayVNodes
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = config.DefaultGatewayTimeout
	}
	if cfg.Retries < 0 {
		cfg.Retries = config.DefaultGatewayRetries
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = config.DefaultMaxMessageSize
	}
	if len(cfg.Order) == 0 && len(cfg.Nodes) > 0 {
		cfg.Order = make([]string, 0, len(cfg.Nodes))
		for id := range cfg.Nodes {
			cfg.Order = append(cfg.Order, id)
		}
		sort.Strings(cfg.Order)
	}
	return cfg
}

// newDialer returns the production dialer. The shared rpc helper attaches
// AUTH_TOKEN to unary and streaming calls and raises the gRPC message limit
// above the 4 MiB default, which large Get responses would otherwise exceed.
func newDialer(cfg config.GatewayConfig) (DialFunc, error) {
	opts, err := rpc.DialOptions(cfg.Token, cfg.MaxMessageBytes)
	if err != nil {
		return nil, fmt.Errorf("build gateway dial options: %w", err)
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, target, opts...)
	}, nil
}

// Put sends a write through the retry loop and returns the node that served it.
func (c *Client) Put(ctx context.Context, req *pb.PutRequest) (string, *pb.PutResponse, error) {
	var resp *pb.PutResponse
	node, err := c.do(ctx, req.GetKey(), true, req.GetExpectedVersion() != 0, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Put(ctx, req)
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// Get sends a read through the retry loop and returns the node that served it.
func (c *Client) Get(ctx context.Context, key string) (string, *pb.GetResponse, error) {
	var resp *pb.GetResponse
	node, err := c.do(ctx, key, false, false, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// Delete sends a delete through the retry loop. Deletion is idempotent.
func (c *Client) Delete(ctx context.Context, req *pb.DeleteRequest) (string, *pb.DeleteResponse, error) {
	var resp *pb.DeleteResponse
	node, err := c.do(ctx, req.GetKey(), true, req.GetExpectedVersion() != 0, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Delete(ctx, req)
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// GetConfig reads the cluster-wide default mode.
func (c *Client) GetConfig(ctx context.Context) (string, *pb.ConfigResponse, error) {
	var resp *pb.ConfigResponse
	node, err := c.do(ctx, "", false, false, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.GetConfig(ctx, &pb.GetConfigRequest{})
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// SetConfig commits a new cluster-wide default mode.
func (c *Client) SetConfig(ctx context.Context, mode string) (string, *pb.ConfigResponse, error) {
	var resp *pb.ConfigResponse
	node, err := c.do(ctx, "", true, false, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.SetConfig(ctx, &pb.SetConfigRequest{DefaultMode: mode})
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// ClusterHealth asks the leader for the cluster-wide health view.
func (c *Client) ClusterHealth(ctx context.Context) (string, *pb.ClusterHealthResponse, error) {
	var resp *pb.ClusterHealthResponse
	node, err := c.do(ctx, "", false, false, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// Stats reads counters from the leader when it is known, otherwise from the
// first reachable candidate. Stats is local-only, so the leader preference is
// a demo convenience rather than a correctness requirement.
func (c *Client) Stats(ctx context.Context) (string, *pb.StatsResponse, error) {
	candidates := c.router.Candidates("")
	if _, health, err := c.ClusterHealth(ctx); err == nil && health.GetLeaderId() != "" {
		if addr := c.cfg.Nodes[health.GetLeaderId()]; addr != "" {
			candidates = moveFirst(addr, candidates)
		}
	}

	var resp *pb.StatsResponse
	node, err := c.doOrdered(ctx, candidates, false, false, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Stats(ctx, &pb.StatsRequest{})
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// Repair enqueues a repair on the leader and returns immediately.
func (c *Client) Repair(ctx context.Context, req *pb.RepairRequest) (string, *pb.RepairResponse, error) {
	var resp *pb.RepairResponse
	node, err := c.do(ctx, req.GetKey(), false, false, func(ctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Repair(ctx, req)
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	return node, resp, err
}

// do orders candidates for key and runs the retry loop.
func (c *Client) do(ctx context.Context, key string, isWrite, hasExpectedVersion bool, call callFunc) (string, error) {
	return c.doOrdered(ctx, c.router.Candidates(key), isWrite, hasExpectedVersion, call)
}

// doOrdered runs at most 1+Retries RPC attempts across the candidate order.
// Transport failures and Aborted responses are retried; validation, auth, and
// not-found failures stop immediately because retrying them cannot help.
func (c *Client) doOrdered(ctx context.Context, candidates []string, isWrite, hasExpectedVersion bool, call callFunc) (string, error) {
	candidates = uniqueStrings(candidates)
	if len(candidates) == 0 {
		return "", errors.New("gateway has no configured nodes")
	}

	attempts := 1 + c.cfg.Retries
	if attempts < 1 {
		attempts = 1
	}

	index := 0
	failures := make([]string, 0, attempts)
	for attempt := 0; attempt < attempts; attempt++ {
		node := candidates[index%len(candidates)]
		index++

		err := c.invoke(ctx, node, call)
		if err == nil {
			return node, nil
		}

		switch status.Code(err) {
		case codes.Aborted:
			if isWrite && hasExpectedVersion {
				return node, fmt.Errorf("version conflict on %s: %w", node, err)
			}
			failures = append(failures, fmt.Sprintf("%s: %v", node, err))
			if leader := c.discoverLeader(ctx, candidates); leader != "" {
				candidates = moveFirst(leader, candidates)
				index = 0
			}
		case codes.Unavailable, codes.DeadlineExceeded:
			failures = append(failures, fmt.Sprintf("%s: %v", node, err))
		case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
			codes.Unauthenticated, codes.AlreadyExists, codes.FailedPrecondition,
			codes.ResourceExhausted:
			return node, fmt.Errorf("%s: %w", node, err)
		default:
			failures = append(failures, fmt.Sprintf("%s: %v", node, err))
		}

		if attempt+1 < attempts {
			c.sleep(backoffDelay(attempt))
		}
	}

	return "", fmt.Errorf("all %d attempts failed: %s", attempts, strings.Join(failures, "; "))
}

// invoke makes one RPC against one node with a per-attempt deadline. There is
// no cached connection: the CLI is a one-shot process and a fresh dial avoids
// retrying over a connection to a node that just changed state.
func (c *Client) invoke(ctx context.Context, node string, call callFunc) error {
	attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	conn, err := c.dial(attemptCtx, node)
	if err != nil {
		if ctxErr := attemptCtx.Err(); ctxErr != nil {
			return status.FromContextError(ctxErr).Err()
		}
		return status.Errorf(codes.Unavailable, "dial %s: %v", node, err)
	}
	defer func() { _ = conn.Close() }()

	return call(attemptCtx, pb.NewStorageServiceClient(conn))
}

// discoverLeader asks candidates for NodeStatus until one reports a leader
// whose address is configured. It returns "" when no leader can be resolved,
// in which case the retry loop keeps its existing candidate order.
func (c *Client) discoverLeader(ctx context.Context, candidates []string) string {
	for _, node := range candidates {
		var leaderID string
		actx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		err := c.invoke(actx, node, func(ctx context.Context, client pb.StorageServiceClient) error {
			resp, err := client.NodeStatus(ctx, &pb.NodeStatusRequest{})
			if err == nil {
				leaderID = resp.GetLeaderId()
			}
			return err
		})
		cancel()
		if err != nil || leaderID == "" {
			continue
		}
		if addr := c.cfg.Nodes[leaderID]; addr != "" {
			return addr
		}
	}
	return ""
}

// backoffDelay returns 100ms, 250ms, or 500ms with +/-20% jitter, capped at the
// 500ms step for later attempts.
func backoffDelay(attempt int) time.Duration {
	steps := [...]time.Duration{100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond}
	base := steps[len(steps)-1]
	if attempt >= 0 && attempt < len(steps) {
		base = steps[attempt]
	}
	jitter := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(base) * jitter)
}

// uniqueStrings preserves order while removing empty and duplicate entries.
func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// moveFirst moves value to the front of values, prepending it when absent.
func moveFirst(value string, values []string) []string {
	if value == "" {
		return values
	}
	out := make([]string, 0, len(values)+1)
	out = append(out, value)
	for _, candidate := range values {
		if candidate == value || candidate == "" {
			continue
		}
		out = append(out, candidate)
	}
	return out
}
