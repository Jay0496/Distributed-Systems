package cluster

import (
	"context"
	"sort"
	"sync"
	"time"
)

// EventKind identifies a detector transition. Node events tell the repairer
// which stripes need attention; leadership events pause and resume repair work.
type EventKind int

const (
	// EventNodeUp is emitted when a node crosses the success threshold.
	EventNodeUp EventKind = iota
	// EventNodeDown is emitted when a node crosses the failure threshold.
	EventNodeDown
	// EventLeaderGained is emitted when this node starts acting as leader.
	EventLeaderGained
	// EventLeaderLost is emitted when this node stops acting as leader.
	EventLeaderLost
)

// String renders an event kind for logs.
func (k EventKind) String() string {
	switch k {
	case EventNodeUp:
		return "node_up"
	case EventNodeDown:
		return "node_down"
	case EventLeaderGained:
		return "leader_gained"
	case EventLeaderLost:
		return "leader_lost"
	default:
		return "unknown"
	}
}

// NodeState is the detector's cached view of one node.
type NodeState struct {
	NodeID    string
	GRPCAddr  string
	Healthy   bool
	Leader    bool
	LastSeen  time.Time
	LastError string
	Failures  int
	Successes int
}

// Event is one detector transition. State is the post-transition snapshot.
type Event struct {
	Kind   EventKind
	NodeID string
	State  NodeState
	Time   time.Time
}

// ProbeFunc performs one liveness probe of a peer. A successful probe returns
// the peer's node state; an error counts as one failure. The detector owns the
// hysteresis, not the probe.
type ProbeFunc func(ctx context.Context, nodeID, grpcAddr string) (NodeState, error)

// DetectorOptions configures the leader-gated failure detector.
type DetectorOptions struct {
	// Interval is the probe period (DETECT_INTERVAL).
	Interval time.Duration
	// Timeout bounds one probe (DETECT_TIMEOUT).
	Timeout time.Duration
	// Failures is the number of consecutive failures that mark a node down
	// (DETECT_FAILURES).
	Failures int
	// Successes is the number of consecutive successes that mark a node up
	// (DETECT_SUCCESSES).
	Successes int
	// Nodes maps node ID to gRPC address for every peer, including the local
	// node. The local node is never probed over the network.
	Nodes map[string]string
	// SelfNode is the ID of the node running this detector.
	SelfNode string
	// Probe performs one peer probe.
	Probe ProbeFunc
	// IsLeader reports whether this node currently leads the Raft group. It is
	// polled every Interval as a fallback when LeaderCh misses an event.
	IsLeader func() bool
	// LeaderCh optionally delivers immediate Raft leadership changes.
	LeaderCh <-chan bool
	// EventsBuffer bounds the event channel. A slow consumer drops events after
	// the buffer fills rather than blocking probes.
	EventsBuffer int
	// Now is injectable for tests. Defaults to time.Now.
	Now func() time.Time
}

// Detector probes peers on the leader and maintains a health map with
// hysteresis. Followers do not probe; only the leader has a complete,
// authoritative view for repair scheduling.
type Detector struct {
	opts   DetectorOptions
	events chan Event

	mu      sync.RWMutex
	states  map[string]NodeState
	active  bool
	started bool
	cancel  context.CancelFunc

	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewDetector builds a detector. Missing options fall back to the documented
// defaults so tests can focus on the fields they exercise.
func NewDetector(opts DetectorOptions) *Detector {
	if opts.Interval <= 0 {
		opts.Interval = 2 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = time.Second
	}
	if opts.Failures <= 0 {
		opts.Failures = 3
	}
	if opts.Successes <= 0 {
		opts.Successes = 1
	}
	if opts.EventsBuffer <= 0 {
		opts.EventsBuffer = 256
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Nodes == nil {
		opts.Nodes = map[string]string{}
	}

	states := make(map[string]NodeState, len(opts.Nodes))
	for id, addr := range opts.Nodes {
		// Start optimistic: a node is healthy until enough probes fail. This
		// also lets the first failure threshold emit a real down transition.
		states[id] = NodeState{
			NodeID:   id,
			GRPCAddr: addr,
			Healthy:  true,
			Leader:   id == opts.SelfNode,
		}
	}
	if opts.SelfNode != "" {
		if _, ok := states[opts.SelfNode]; !ok {
			states[opts.SelfNode] = NodeState{
				NodeID:   opts.SelfNode,
				Healthy:  true,
				Leader:   true,
				GRPCAddr: opts.Nodes[opts.SelfNode],
			}
		}
	}

	return &Detector{
		opts:   opts,
		events: make(chan Event, opts.EventsBuffer),
		states: states,
		done:   make(chan struct{}),
	}
}

// Events exposes detector transitions. Consumers must keep draining the
// channel; dropped events are recoverable because the periodic repair sweep
// rebuilds work from metadata.
func (d *Detector) Events() <-chan Event {
	return d.events
}

// Start launches the supervised probe loop. It is legal to call Start more
// than once; subsequent calls are no-ops.
func (d *Detector) Start(ctx context.Context) {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.mu.Unlock()

	d.wg.Add(1)
	go d.run(runCtx)
}

// Stop shuts the detector down and waits for the probe loop. It is idempotent.
func (d *Detector) Stop() {
	d.stopOnce.Do(func() {
		d.mu.Lock()
		cancel := d.cancel
		started := d.started
		d.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		close(d.done)
		if started {
			d.wg.Wait()
		}
	})
}

// Active reports whether the detector is currently probing as leader.
func (d *Detector) Active() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.active
}

// Snapshot returns a copy of the current health map.
func (d *Detector) Snapshot() map[string]NodeState {
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make(map[string]NodeState, len(d.states))
	for id, state := range d.states {
		out[id] = state
	}
	return out
}

func (d *Detector) run(ctx context.Context) {
	defer d.wg.Done()

	ticker := time.NewTicker(d.opts.Interval)
	defer ticker.Stop()

	leader := d.isLeader()
	d.setActive(leader)
	if leader {
		d.probeAll(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			d.setActive(false)
			return
		case isLeader, ok := <-d.opts.LeaderCh:
			if ok && isLeader != leader {
				leader = isLeader
				d.setActive(leader)
				if leader {
					d.probeAll(ctx)
				}
			}
		case <-ticker.C:
			// Polling is the fallback that prevents a missed LeaderCh event
			// from leaving the detector running on a follower.
			current := d.isLeader()
			if current != leader {
				leader = current
				d.setActive(current)
			}
			if leader {
				d.probeAll(ctx)
			}
		}
	}
}

func (d *Detector) isLeader() bool {
	return d.opts.IsLeader != nil && d.opts.IsLeader()
}

// setActive starts or stops the leader-only work and emits one transition.
func (d *Detector) setActive(active bool) {
	d.mu.Lock()
	if d.active == active {
		d.mu.Unlock()
		return
	}
	d.active = active
	now := d.opts.Now()
	if d.opts.SelfNode != "" {
		state := d.states[d.opts.SelfNode]
		state.NodeID = d.opts.SelfNode
		state.Healthy = true
		state.Leader = active
		state.LastSeen = now
		state.LastError = ""
		d.states[d.opts.SelfNode] = state
	}
	d.mu.Unlock()

	kind := EventLeaderLost
	if active {
		kind = EventLeaderGained
	}
	d.emit(Event{Kind: kind, NodeID: d.opts.SelfNode, Time: now})
}

// probeAll probes every peer concurrently. The local node is never probed.
func (d *Detector) probeAll(ctx context.Context) {
	if d.opts.Probe == nil {
		return
	}

	type target struct {
		id   string
		addr string
	}
	targets := make([]target, 0, len(d.opts.Nodes))
	for id, addr := range d.opts.Nodes {
		if id == "" || id == d.opts.SelfNode {
			continue
		}
		targets = append(targets, target{id: id, addr: addr})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].id < targets[j].id })

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, d.opts.Timeout)
			defer cancel()
			state, err := d.opts.Probe(probeCtx, t.id, t.addr)
			d.applyProbe(t.id, state, err)
		}(t)
	}
	wg.Wait()
}

func (d *Detector) applyProbe(nodeID string, probed NodeState, err error) {
	now := d.opts.Now()

	var event *Event
	d.mu.Lock()
	state, ok := d.states[nodeID]
	if !ok {
		state.NodeID = nodeID
		state.Healthy = true
	}
	state.NodeID = nodeID
	if probed.GRPCAddr != "" {
		state.GRPCAddr = probed.GRPCAddr
	}

	if err != nil {
		state.Failures++
		state.Successes = 0
		state.LastError = err.Error()
		if state.Healthy && state.Failures >= d.opts.Failures {
			state.Healthy = false
			event = &Event{Kind: EventNodeDown, NodeID: nodeID, State: state, Time: now}
		}
	} else {
		state.Successes++
		state.Failures = 0
		state.LastError = ""
		state.LastSeen = now
		state.Leader = probed.Leader
		if probed.GRPCAddr != "" {
			state.GRPCAddr = probed.GRPCAddr
		}
		if !state.Healthy && state.Successes >= d.opts.Successes {
			state.Healthy = true
			event = &Event{Kind: EventNodeUp, NodeID: nodeID, State: state, Time: now}
		}
	}
	d.states[nodeID] = state
	d.mu.Unlock()

	if event != nil {
		d.emit(*event)
	}
}

func (d *Detector) emit(event Event) {
	select {
	case d.events <- event:
	default:
		// Never block a probe on a slow consumer. The repair sweep will
		// rediscover the state from metadata and the snapshot.
	}
}
