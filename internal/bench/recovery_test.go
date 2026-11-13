package bench

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "distributed-kv-store/proto"
)

func TestPollRecoveryTransitions(t *testing.T) {
	start := time.Now()
	var calls int64
	var mu sync.Mutex
	fake := HealthSourceFunc(func(context.Context, *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 2 {
			return &pb.ClusterHealthResponse{
				Success: true, Degraded: true, HealthyNodes: 4, TotalNodes: 5,
				DegradedStripes: 3, RepairQueueDepth: 7,
			}, nil
		}
		return &pb.ClusterHealthResponse{
			Success: true, Degraded: false, HealthyNodes: 5, TotalNodes: 5,
		}, nil
	})

	result, err := PollRecovery(context.Background(), fake, RecoveryOptions{
		Scenario:     "one-node",
		StartTime:    start,
		Timeout:      2 * time.Second,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("PollRecovery: %v", err)
	}
	if result.DegradedAt.IsZero() || result.RepairedAt.IsZero() {
		t.Fatalf("missing timestamps: %+v", result)
	}
	if result.TotalDuration <= 0 {
		t.Fatalf("total duration = %s", result.TotalDuration)
	}
	if result.HealthyNodes != 5 || result.TotalNodes != 5 {
		t.Fatalf("final health = %d/%d", result.HealthyNodes, result.TotalNodes)
	}
}

func TestPollRecoveryUsesObservedDegradedTime(t *testing.T) {
	degraded := time.Now().Add(-3 * time.Second)
	start := degraded.Add(-time.Second)
	fake := HealthSourceFunc(func(context.Context, *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error) {
		return &pb.ClusterHealthResponse{Success: true, Degraded: false, HealthyNodes: 5, TotalNodes: 5}, nil
	})
	result, err := PollRecovery(context.Background(), fake, RecoveryOptions{
		Scenario:     "two-node",
		StartTime:    start,
		DegradedTime: degraded,
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("PollRecovery: %v", err)
	}
	if result.TimeToDegraded != time.Second {
		t.Fatalf("time to degraded = %s, want 1s", result.TimeToDegraded)
	}
	if result.RepairedAt.IsZero() {
		t.Fatal("repair completion was not recorded")
	}
}
