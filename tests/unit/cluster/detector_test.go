package cluster_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/tests/support"
)

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func startTestDetector(t *testing.T, opts cluster.DetectorOptions) *cluster.Detector {
	t.Helper()
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Millisecond
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 50 * time.Millisecond
	}
	d := cluster.NewDetector(opts)
	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)
	t.Cleanup(func() {
		cancel()
		d.Stop()
	})
	return d
}

func waitEvent(t *testing.T, d *cluster.Detector, kind cluster.EventKind, nodeID string) cluster.Event {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-d.Events():
			if event.Kind == kind && (nodeID == "" || event.NodeID == nodeID) {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s event for %q", kind, nodeID)
		}
	}
}

func TestDetectorMarksHealthyNodeUp(t *testing.T) {
	var probes atomic.Int32
	d := startTestDetector(t, cluster.DetectorOptions{
		Nodes:    map[string]string{"node1": "local", "node2": "remote"},
		SelfNode: "node1",
		Failures: 3,
		Probe: func(context.Context, string, string) (cluster.NodeState, error) {
			probes.Add(1)
			return cluster.NodeState{Healthy: true}, nil
		},
		IsLeader: func() bool { return true },
	})

	waitFor(t, time.Second, func() bool {
		state := d.Snapshot()["node2"]
		return state.Healthy && !state.LastSeen.IsZero() && probes.Load() > 0
	})
	if !d.Active() {
		t.Fatal("detector is not active on a leader")
	}
}

func TestDetectorHysteresisMarksDownAndUp(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(false)
	d := startTestDetector(t, cluster.DetectorOptions{
		Nodes:     map[string]string{"node1": "local", "node2": "remote"},
		SelfNode:  "node1",
		Failures:  3,
		Successes: 1,
		Probe: func(context.Context, string, string) (cluster.NodeState, error) {
			if healthy.Load() {
				return cluster.NodeState{}, nil
			}
			return cluster.NodeState{}, errors.New("probe failed")
		},
		IsLeader: func() bool { return true },
	})

	waitEvent(t, d, cluster.EventNodeDown, "node2")
	state := d.Snapshot()["node2"]
	if state.Healthy || state.Failures < 3 {
		t.Fatalf("state after down = %+v", state)
	}

	healthy.Store(true)
	waitEvent(t, d, cluster.EventNodeUp, "node2")
	state = d.Snapshot()["node2"]
	if !state.Healthy || state.LastSeen.IsZero() {
		t.Fatalf("state after up = %+v", state)
	}
}

// TestDetectorHysteresisWithFakeClock uses the shared fake clock so transition
// timestamps and LastSeen are deterministic. State convergence still comes from
// WaitFor, never a fixed sleep.
func TestDetectorHysteresisWithFakeClock(t *testing.T) {
	clock := support.NewFakeClock(time.Time{})
	prober := support.NewFakeProber()
	prober.SetHealthy("node2", false)

	d := startTestDetector(t, cluster.DetectorOptions{
		Nodes:     map[string]string{"node1": "local", "node2": "remote"},
		SelfNode:  "node1",
		Failures:  2,
		Successes: 2,
		Now:       clock.Now,
		Probe:     prober.Probe,
		IsLeader:  func() bool { return true },
	})

	down := waitEvent(t, d, cluster.EventNodeDown, "node2")
	if !down.Time.Equal(clock.Now()) {
		t.Fatalf("down event time = %s, want fake clock %s", down.Time, clock.Now())
	}

	clock.Advance(3 * time.Second)
	prober.SetHealthy("node2", true)

	up := waitEvent(t, d, cluster.EventNodeUp, "node2")
	if !up.Time.Equal(clock.Now()) {
		t.Fatalf("up event time = %s, want fake clock %s", up.Time, clock.Now())
	}
	if up.Time.Sub(down.Time) != 3*time.Second {
		t.Fatalf("event gap = %s, want 3s", up.Time.Sub(down.Time))
	}
	state := d.Snapshot()["node2"]
	if !state.Healthy || !state.LastSeen.Equal(clock.Now()) {
		t.Fatalf("state after fake-clock recovery = %+v", state)
	}
	if prober.Calls("node2") < 4 {
		t.Fatalf("probes = %d, want at least 4", prober.Calls("node2"))
	}
}

func TestDetectorDoesNotProbeOnFollower(t *testing.T) {
	var probes atomic.Int32
	d := startTestDetector(t, cluster.DetectorOptions{
		Nodes:    map[string]string{"node1": "local", "node2": "remote"},
		SelfNode: "node1",
		Failures: 1,
		Probe: func(context.Context, string, string) (cluster.NodeState, error) {
			probes.Add(1)
			return cluster.NodeState{}, nil
		},
		IsLeader: func() bool { return false },
	})

	time.Sleep(50 * time.Millisecond)
	if probes.Load() != 0 {
		t.Fatalf("follower ran %d probes", probes.Load())
	}
	if d.Active() {
		t.Fatal("follower detector reported active")
	}
}

func TestDetectorStopsOnLeadershipLoss(t *testing.T) {
	leaderCh := make(chan bool, 2)
	var probes atomic.Int32
	var leader atomic.Bool
	leader.Store(true)
	d := startTestDetector(t, cluster.DetectorOptions{
		Nodes:    map[string]string{"node1": "local", "node2": "remote"},
		SelfNode: "node1",
		Failures: 1,
		Probe: func(context.Context, string, string) (cluster.NodeState, error) {
			probes.Add(1)
			return cluster.NodeState{}, nil
		},
		IsLeader: func() bool { return leader.Load() },
		LeaderCh: leaderCh,
	})

	waitFor(t, time.Second, func() bool { return probes.Load() > 0 })
	leader.Store(false)
	leaderCh <- false
	waitEvent(t, d, cluster.EventLeaderLost, "node1")
	observed := probes.Load()
	time.Sleep(50 * time.Millisecond)
	if probes.Load() != observed {
		t.Fatalf("probes continued after leadership loss: %d -> %d", observed, probes.Load())
	}
	if d.Active() {
		t.Fatal("detector still active after leadership loss")
	}

	leader.Store(true)
	leaderCh <- true
	waitEvent(t, d, cluster.EventLeaderGained, "node1")
	waitFor(t, time.Second, func() bool { return probes.Load() > observed })
}

func TestDetectorSnapshotIsACopy(t *testing.T) {
	d := cluster.NewDetector(cluster.DetectorOptions{
		Nodes:    map[string]string{"node1": "local"},
		SelfNode: "node1",
		Probe:    func(context.Context, string, string) (cluster.NodeState, error) { return cluster.NodeState{}, nil },
		IsLeader: func() bool { return true },
	})

	snapshot := d.Snapshot()
	snapshot["node1"] = cluster.NodeState{NodeID: "mutated"}
	if got := d.Snapshot()["node1"]; got.NodeID != "node1" {
		t.Fatalf("Snapshot exposed internal state: %+v", got)
	}
}
