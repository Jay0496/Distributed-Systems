package bench

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "distributed-kv-store/proto"
)

// RecoveryOptions configures one degraded/repair measurement.
type RecoveryOptions struct {
	Scenario     string
	StartTime    time.Time
	DegradedTime time.Time
	RestartTime  time.Time
	Timeout      time.Duration
	PollInterval time.Duration
	SeedKeys     int
	SeedPrefix   string
	ValueSize    int
	Seed         int64
	SeedOnly     bool
	MeasureOnly  bool
	Mode         string
}

// HealthSample is one observed cluster-health point.
type HealthSample struct {
	At               time.Time `json:"at"`
	Degraded         bool      `json:"degraded"`
	HealthyNodes     uint32    `json:"healthy_nodes"`
	TotalNodes       uint32    `json:"total_nodes"`
	DegradedStripes  uint64    `json:"degraded_stripes"`
	RepairQueueDepth uint64    `json:"repair_queue_depth"`
	LeaderID         string    `json:"leader_id"`
}

// RecoveryResult is one recovery scenario, with timestamps rather than only
// aggregate durations so a reader can check the clock arithmetic.
type RecoveryResult struct {
	Scenario            string        `json:"scenario"`
	StartTime           time.Time     `json:"start_time"`
	DegradedAt          time.Time     `json:"degraded_at,omitempty"`
	RestartTime         time.Time     `json:"restart_time,omitempty"`
	RepairedAt          time.Time     `json:"repaired_at,omitempty"`
	TimeToDegraded      time.Duration `json:"-"`
	RepairDuration      time.Duration `json:"-"`
	TotalDuration       time.Duration `json:"-"`
	HealthyNodes        uint32        `json:"healthy_nodes"`
	TotalNodes          uint32        `json:"total_nodes"`
	DegradedStripes     uint64        `json:"degraded_stripes"`
	RepairQueueDepth    uint64        `json:"repair_queue_depth"`
	Observed            []HealthSample `json:"observed"`
	Error               string        `json:"error,omitempty"`
}

// Normalize fills recovery defaults.
func (o RecoveryOptions) Normalize() RecoveryOptions {
	if strings.TrimSpace(o.Scenario) == "" {
		o.Scenario = "one-node"
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Minute
	}
	if o.PollInterval <= 0 {
		o.PollInterval = time.Second
	}
	if o.SeedKeys <= 0 {
		o.SeedKeys = 100
	}
	if o.ValueSize <= 0 {
		o.ValueSize = 4096
	}
	if strings.TrimSpace(o.SeedPrefix) == "" {
		o.SeedPrefix = "recovery"
	}
	if o.Mode == "" {
		o.Mode = "rs32"
	}
	return o
}

// SeedRecoveryDataset writes a known key set and verifies five fragments per
// stripe. It is the pre-step for a script-driven stop/start scenario.
func SeedRecoveryDataset(ctx context.Context, nodes Nodes, opts RecoveryOptions) (*Dataset, error) {
	opts = opts.Normalize()
	dataset, err := WriteDataset(ctx, nodes, DatasetOptions{
		Mode:      opts.Mode,
		Values:    opts.SeedKeys,
		ValueSize: opts.ValueSize,
		Prefix:    opts.SeedPrefix,
		Seed:      opts.Seed,
	})
	if err != nil {
		return nil, err
	}
	if err := VerifyFragments(ctx, nodes, dataset, 30*time.Second); err != nil {
		return nil, err
	}
	return dataset, nil
}

// HealthSource is the leader-forwarded ClusterHealth surface used for recovery
// polling. Nodes each satisfy it, and tests can supply a scripted sequence.
type HealthSource interface {
	ClusterHealth(ctx context.Context, in *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error)
}

// HealthSourceFunc adapts a function to HealthSource.
type HealthSourceFunc func(ctx context.Context, in *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error)

// ClusterHealth implements HealthSource.
func (f HealthSourceFunc) ClusterHealth(ctx context.Context, in *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error) {
	return f(ctx, in)
}

// NodesHealthSource tries each node in order until one answers. A stopped node
// therefore does not stall the measurement.
func NodesHealthSource(nodes Nodes) HealthSource {
	return HealthSourceFunc(func(ctx context.Context, in *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error) {
		var lastErr error
		for _, node := range nodes {
			callCtx, cancel := WithTimeout(ctx, 5*time.Second)
			resp, err := node.API.ClusterHealth(callCtx, in)
			cancel()
			if err == nil {
				return resp, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("no nodes configured")
		}
		return nil, lastErr
	})
}

// PollRecovery measures degradation and repair. If DegradedTime was observed by
// the caller (the scripts do this), the poller waits only for the repair to
// clear. Otherwise it records the first degraded observation itself.
func PollRecovery(ctx context.Context, source HealthSource, opts RecoveryOptions) (*RecoveryResult, error) {
	opts = opts.Normalize()
	if source == nil {
		return nil, fmt.Errorf("health source is required")
	}
	start := opts.StartTime
	if start.IsZero() {
		start = time.Now()
	}
	result := &RecoveryResult{
		Scenario:     opts.Scenario,
		StartTime:    start,
		DegradedAt:   opts.DegradedTime,
		RestartTime:  opts.RestartTime,
		Observed:     make([]HealthSample, 0, 16),
	}

	deadline := time.Now().Add(opts.Timeout)
	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	for {
		resp, err := source.ClusterHealth(ctx, &pb.ClusterHealthRequest{})
		now := time.Now()
		if err == nil && resp != nil {
			sample := HealthSample{
				At:               now,
				Degraded:         resp.GetDegraded(),
				HealthyNodes:     resp.GetHealthyNodes(),
				TotalNodes:       resp.GetTotalNodes(),
				DegradedStripes:  resp.GetDegradedStripes(),
				RepairQueueDepth: resp.GetRepairQueueDepth(),
				LeaderID:         resp.GetLeaderId(),
			}
			result.Observed = append(result.Observed, sample)
			result.HealthyNodes = sample.HealthyNodes
			result.TotalNodes = sample.TotalNodes
			result.DegradedStripes = sample.DegradedStripes
			result.RepairQueueDepth = sample.RepairQueueDepth

			if sample.Degraded {
				if result.DegradedAt.IsZero() {
					result.DegradedAt = sample.At
				}
			} else if !result.DegradedAt.IsZero() {
				result.RepairedAt = sample.At
				result.TimeToDegraded = result.DegradedAt.Sub(result.StartTime)
				repairFrom := result.RestartTime
				if repairFrom.IsZero() {
					repairFrom = result.DegradedAt
				}
				result.RepairDuration = result.RepairedAt.Sub(repairFrom)
				result.TotalDuration = result.RepairedAt.Sub(result.StartTime)
				return result, nil
			}
		} else if err != nil {
			result.Error = err.Error()
		}

		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if time.Now().After(deadline) {
			if result.Error == "" {
				if result.DegradedAt.IsZero() {
					result.Error = "cluster was not observed degraded; pass --degraded-time from the script"
				} else {
					result.Error = fmt.Sprintf("degraded state did not clear within %s", opts.Timeout)
				}
			}
			return result, fmt.Errorf("recovery measurement timed out: %s", result.Error)
		}

		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-ticker.C:
		}
	}
}
