package support

import (
	"context"
	"fmt"
	"sync"

	"distributed-kv-store/internal/cluster"
)

// FakeProber is a controllable cluster.ProbeFunc. By default every node is
// healthy. SetHealthy(false) makes the next probe return an error, while
// SetError installs a specific failure for assertions on error propagation.
type FakeProber struct {
	mu      sync.Mutex
	healthy map[string]bool
	errs    map[string]error
	calls   map[string]int
}

// NewFakeProber returns a prober that reports every unconfigured node healthy.
func NewFakeProber() *FakeProber {
	return &FakeProber{
		healthy: make(map[string]bool),
		errs:    make(map[string]error),
		calls:   make(map[string]int),
	}
}

var _ cluster.ProbeFunc = (*FakeProber)(nil).Probe

// SetHealthy controls whether the next probe of nodeID succeeds.
func (p *FakeProber) SetHealthy(nodeID string, healthy bool) {
	p.mu.Lock()
	p.healthy[nodeID] = healthy
	if healthy {
		delete(p.errs, nodeID)
	}
	p.mu.Unlock()
}

// SetError installs a probe error for nodeID. A nil error clears the override.
func (p *FakeProber) SetError(nodeID string, err error) {
	p.mu.Lock()
	if err == nil {
		delete(p.errs, nodeID)
	} else {
		p.errs[nodeID] = err
	}
	p.mu.Unlock()
}

// Probe implements cluster.ProbeFunc.
func (p *FakeProber) Probe(_ context.Context, nodeID, grpcAddr string) (cluster.NodeState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls[nodeID]++

	if err := p.errs[nodeID]; err != nil {
		return cluster.NodeState{}, err
	}
	if healthy, ok := p.healthy[nodeID]; ok && !healthy {
		return cluster.NodeState{}, fmt.Errorf("support: node %s is unhealthy", nodeID)
	}
	return cluster.NodeState{NodeID: nodeID, GRPCAddr: grpcAddr, Healthy: true}, nil
}

// Calls reports how many probes nodeID has received.
func (p *FakeProber) Calls(nodeID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[nodeID]
}
