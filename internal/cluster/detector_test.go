package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func startTestDetector(t *testing.T, opts DetectorOptions) *Detector {
	t.Helper()
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Millisecond
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 50 * time.Millisecond
	}
	d := NewDetector(opts)
	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)
	t.Cleanup(func() {
		cancel()
		d.Stop()
	})
	return d
}

func waitEvent(t *testing.T, d *Detector, kind EventKind, nodeID string) Event {
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
	d := startTestDetector(t, DetectorOptions{
		Nodes:    map[string]string{"node1": "local", "node2": "remote"},
		SelfNode: "node1",
		Failures: 3,
		Probe: func(context.Context, string, string) (NodeState, error) {
			probes.Add(1)
			return NodeState{Healthy: true}, nil
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
	d := startTestDetector(t, DetectorOptions{
		Nodes:     map[string]string{"node1": "local", "node2": "remote"},
		SelfNode:  "node1",
		Failures:  3,
		Successes: 1,
		Probe: func(context.Context, string, string) (NodeState, error) {
			if healthy.Load() {
				return NodeState{}, nil
			}
			return NodeState{}, errors.New("probe failed")
		},
		IsLeader: func() bool { return true },
	})

	waitEvent(t, d, EventNodeDown, "node2")
	state := d.Snapshot()["node2"]
	if state.Healthy || state.Failures < 3 {
		t.Fatalf("state after down = %+v", state)
	}

	healthy.Store(true)
	waitEvent(t, d, EventNodeUp, "node2")
	state = d.Snapshot()["node2"]
	if !state.Healthy || state.LastSeen.IsZero() {
		t.Fatalf("state after up = %+v", state)
	}
}

func TestDetectorDoesNotProbeOnFollower(t *testing.T) {
	var probes atomic.Int32
	d := startTestDetector(t, DetectorOptions{
		Nodes:    map[string]string{"node1": "local", "node2": "remote"},
		SelfNode: "node1",
		Failures: 1,
		Probe: func(context.Context, string, string) (NodeState, error) {
			probes.Add(1)
			return NodeState{}, nil
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
	d := startTestDetector(t, DetectorOptions{
		Nodes:    map[string]string{"node1": "local", "node2": "remote"},
		SelfNode: "node1",
		Failures: 1,
		Probe: func(context.Context, string, string) (NodeState, error) {
			probes.Add(1)
			return NodeState{}, nil
		},
		IsLeader: func() bool { return leader.Load() },
		LeaderCh: leaderCh,
	})

	waitFor(t, time.Second, func() bool { return probes.Load() > 0 })
	leader.Store(false)
	leaderCh <- false
	waitEvent(t, d, EventLeaderLost, "node1")
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
	waitEvent(t, d, EventLeaderGained, "node1")
	waitFor(t, time.Second, func() bool { return probes.Load() > observed })
}

func TestDetectorSnapshotIsACopy(t *testing.T) {
	d := NewDetector(DetectorOptions{
		Nodes:    map[string]string{"node1": "local"},
		SelfNode: "node1",
		Probe:    func(context.Context, string, string) (NodeState, error) { return NodeState{}, nil },
		IsLeader: func() bool { return true },
	})

	snapshot := d.Snapshot()
	snapshot["node1"] = NodeState{NodeID: "mutated"}
	if got := d.Snapshot()["node1"]; got.NodeID != "node1" {
		t.Fatalf("Snapshot exposed internal state: %+v", got)
	}
}
