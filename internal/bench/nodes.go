package bench

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
)

// API is the subset of StorageService used by the benchmark harness. The
// generated StorageServiceClient satisfies it, and tests can supply a small
// fake without implementing the whole service.
type API interface {
	Put(ctx context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error)
	Get(ctx context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error)
	Stats(ctx context.Context, in *pb.StatsRequest, opts ...grpc.CallOption) (*pb.StatsResponse, error)
	ForceSnapshot(ctx context.Context, in *pb.ForceSnapshotRequest, opts ...grpc.CallOption) (*pb.ForceSnapshotResponse, error)
	CheckpointStore(ctx context.Context, in *pb.CheckpointRequest, opts ...grpc.CallOption) (*pb.CheckpointResponse, error)
	ClusterHealth(ctx context.Context, in *pb.ClusterHealthRequest, opts ...grpc.CallOption) (*pb.ClusterHealthResponse, error)
}

// Node is one benchmark target: a stable node ID and its StorageService client.
type Node struct {
	ID  string
	API API
}

// Nodes is an ordered, non-empty set of benchmark targets. The order is
// deterministic so retry and load-spreading decisions are reproducible.
type Nodes []Node

// Validate reports whether the node set is usable.
func (n Nodes) Validate() error {
	if len(n) == 0 {
		return fmt.Errorf("at least one node is required")
	}
	for i, node := range n {
		if node.ID == "" {
			return fmt.Errorf("node %d has an empty ID", i)
		}
		if node.API == nil {
			return fmt.Errorf("node %q has no client", node.ID)
		}
	}
	return nil
}

// OrderedNodes returns the node IDs in their configured order.
func (n Nodes) OrderedNodes() []string {
	ids := make([]string, len(n))
	for i, node := range n {
		ids[i] = node.ID
	}
	return ids
}

// SortedNodes sorts a copy of nodes by ID.
func SortedNodes(nodes []Node) []Node {
	out := append([]Node(nil), nodes...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// NormalizeMode validates a benchmark mode. An empty mode is rejected because a
// benchmark must state which redundancy scheme it measured.
func NormalizeMode(mode string) (string, error) {
	mode = strings.TrimSpace(strings.ToLower(mode))
	switch mode {
	case "rs32", "replication":
		return mode, nil
	case "":
		return "", fmt.Errorf("mode is required (rs32 or replication)")
	default:
		return "", fmt.Errorf("unsupported mode %q (want rs32 or replication)", mode)
	}
}

// WithTimeout returns a child context bounded by timeout when timeout is
// positive. The caller must always invoke the returned cancel function.
func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
