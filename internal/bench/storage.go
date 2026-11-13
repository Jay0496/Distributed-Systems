package bench

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DatasetOptions configures a deterministic write-only dataset.
type DatasetOptions struct {
	Mode      string
	Values    int
	ValueSize int
	Prefix    string
	Seed      int64
	Timeout   time.Duration
}

// Dataset is the logical view of a written key set.
type Dataset struct {
	Mode         string
	Keys         []string
	ValueSize    int
	LogicalBytes int64
}

// Normalize validates dataset options.
func (o DatasetOptions) Normalize() (DatasetOptions, error) {
	mode, err := NormalizeMode(o.Mode)
	if err != nil {
		return DatasetOptions{}, err
	}
	o.Mode = mode
	if o.Values <= 0 {
		return DatasetOptions{}, fmt.Errorf("values must be positive, got %d", o.Values)
	}
	if o.ValueSize <= 0 {
		return DatasetOptions{}, fmt.Errorf("value size must be positive, got %d", o.ValueSize)
	}
	if strings.TrimSpace(o.Prefix) == "" {
		o.Prefix = "bench"
	}
	if o.Timeout <= 0 {
		o.Timeout = 20 * time.Second
	}
	return o, nil
}

// NodeStorage is one node's storage contribution.
type NodeStorage struct {
	NodeID        string `json:"node_id"`
	Keys          uint64 `json:"keys"`
	Stripes       uint64 `json:"stripes"`
	Fragments     uint64 `json:"fragments"`
	FragmentBytes uint64 `json:"fragment_bytes"`
	DiskBytes     uint64 `json:"disk_bytes"`
}

// StorageResult is the measured storage overhead for one mode.
type StorageResult struct {
	Mode          string        `json:"mode"`
	Keys          int           `json:"keys"`
	ValueSize     int           `json:"value_size"`
	LogicalBytes  int64         `json:"logical_bytes"`
	FragmentBytes int64         `json:"fragment_bytes"`
	DiskBytes     int64         `json:"disk_bytes"`
	PayloadRatio  float64       `json:"payload_ratio"`
	DiskRatio     float64       `json:"disk_ratio"`
	Nodes         []NodeStorage `json:"nodes"`
	MeasuredAt    time.Time     `json:"measured_at"`
}

// WriteDataset writes a deterministic key set through the supplied nodes. Each
// key is written once, so logical bytes are exactly len(keys)*value_size.
func WriteDataset(ctx context.Context, nodes Nodes, opts DatasetOptions) (*Dataset, error) {
	if err := nodes.Validate(); err != nil {
		return nil, err
	}
	normalized, err := opts.Normalize()
	if err != nil {
		return nil, err
	}

	dataset := &Dataset{
		Mode:         normalized.Mode,
		Keys:         make([]string, normalized.Values),
		ValueSize:    normalized.ValueSize,
		LogicalBytes: int64(normalized.Values) * int64(normalized.ValueSize),
	}
	rng := rand.New(rand.NewSource(normalized.Seed))
	for i := 0; i < normalized.Values; i++ {
		key := fmt.Sprintf("%s-%06d", normalized.Prefix, i)
		dataset.Keys[i] = key
		value := make([]byte, normalized.ValueSize)
		for j := range value {
			value[j] = byte(rng.Intn(256))
		}
		if err := putWithRetry(ctx, nodes, i, &pb.PutRequest{Key: key, Value: value, Mode: normalized.Mode}, normalized.Timeout); err != nil {
			return nil, fmt.Errorf("write %s: %w", key, err)
		}
	}
	return dataset, nil
}

// VerifyFragments waits until every written stripe has all five fragments
// somewhere in the cluster. A healthy RS or replication write stores five
// shards or copies.
func VerifyFragments(ctx context.Context, nodes Nodes, dataset *Dataset, timeout time.Duration) error {
	if dataset == nil {
		return fmt.Errorf("dataset is required")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	want := uint64(len(dataset.Keys)) * 5
	deadline := time.Now().Add(timeout)
	var last string
	for {
		var total uint64
		for _, node := range nodes {
			callCtx, cancel := WithTimeout(ctx, 5*time.Second)
			stats, err := node.API.Stats(callCtx, &pb.StatsRequest{})
			cancel()
			if err != nil {
				last = fmt.Sprintf("%s: %v", node.ID, err)
				total = 0
				break
			}
			total += stats.GetFragments()
		}
		if total == want {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			if last == "" {
				last = fmt.Sprintf("found %d fragments, want %d", total, want)
			}
			return fmt.Errorf("timed out waiting for %d fragments: %s", want, last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// MeasureStorage writes a dataset, forces a Raft snapshot and a SQLite WAL
// checkpoint on every node, then reads first-class storage counters. Ratios use
// logical payload bytes as the denominator so RS is not misreported as a
// percentage of the original.
func MeasureStorage(ctx context.Context, nodes Nodes, opts DatasetOptions) (*StorageResult, error) {
	dataset, err := WriteDataset(ctx, nodes, opts)
	if err != nil {
		return nil, err
	}
	if err := VerifyFragments(ctx, nodes, dataset, 30*time.Second); err != nil {
		return nil, err
	}

	for _, node := range nodes {
		if err := forceSnapshot(ctx, node); err != nil {
			return nil, err
		}
		if err := checkpointStore(ctx, node); err != nil {
			return nil, err
		}
	}

	result := &StorageResult{
		Mode:         dataset.Mode,
		Keys:         len(dataset.Keys),
		ValueSize:    dataset.ValueSize,
		LogicalBytes: dataset.LogicalBytes,
		MeasuredAt:   time.Now(),
		Nodes:        make([]NodeStorage, 0, len(nodes)),
	}
	for _, node := range nodes {
		callCtx, cancel := WithTimeout(ctx, 10*time.Second)
		stats, err := node.API.Stats(callCtx, &pb.StatsRequest{})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("stats on %s: %w", node.ID, err)
		}
		result.Nodes = append(result.Nodes, NodeStorage{
			NodeID:        node.ID,
			Keys:          stats.GetKeys(),
			Stripes:       stats.GetStripes(),
			Fragments:     stats.GetFragments(),
			FragmentBytes: stats.GetFragmentBytes(),
			DiskBytes:     stats.GetStorageBytes(),
		})
		result.FragmentBytes += int64(stats.GetFragmentBytes())
		result.DiskBytes += int64(stats.GetStorageBytes())
	}
	if result.LogicalBytes > 0 {
		result.PayloadRatio = float64(result.FragmentBytes) / float64(result.LogicalBytes)
		result.DiskRatio = float64(result.DiskBytes) / float64(result.LogicalBytes)
	}
	return result, nil
}

// forceSnapshot compacts one node's Raft state before measurement.
func forceSnapshot(ctx context.Context, node Node) error {
	callCtx, cancel := WithTimeout(ctx, 60*time.Second)
	defer cancel()
	resp, err := node.API.ForceSnapshot(callCtx, &pb.ForceSnapshotRequest{})
	if err != nil {
		return fmt.Errorf("force snapshot on %s: %w", node.ID, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("force snapshot on %s rejected: %s", node.ID, resp.GetMessage())
	}
	return nil
}

// checkpointStore truncates one node's WAL before measurement.
func checkpointStore(ctx context.Context, node Node) error {
	callCtx, cancel := WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := node.API.CheckpointStore(callCtx, &pb.CheckpointRequest{})
	if err != nil {
		return fmt.Errorf("checkpoint on %s: %w", node.ID, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("checkpoint on %s rejected: %s", node.ID, resp.GetMessage())
	}
	return nil
}

// putWithRetry writes one key, retrying only leader-change Unavailable errors.
func putWithRetry(ctx context.Context, nodes Nodes, index int, req *pb.PutRequest, timeout time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < len(nodes); attempt++ {
		node := nodes[(index+attempt)%len(nodes)]
		callCtx, cancel := WithTimeout(ctx, timeout)
		resp, err := node.API.Put(callCtx, req)
		cancel()
		if err == nil {
			if resp.GetSuccess() {
				return nil
			}
			return fmt.Errorf("put rejected: %s", resp.GetMessage())
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable {
			return err
		}
	}
	return lastErr
}
