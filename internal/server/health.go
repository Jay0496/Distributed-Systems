package server

import (
	"context"
	"sync"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthServiceName is the gRPC health service reported as readiness. The empty
// service name is reserved for liveness so a load balancer can tell "running"
// from "ready to serve user traffic".
const HealthServiceName = "storage.StorageService"

// readinessPollInterval is how often the background monitor refreshes the
// gRPC health statuses. It is short enough for startup and shutdown
// transitions to be visible to Compose and the detector.
const readinessPollInterval = 500 * time.Millisecond

// healthState owns the process lifecycle flags and the optional gRPC health
// server. It is deliberately separate from Raft so liveness remains answerable
// while the node has no leader.
type healthState struct {
	mu           sync.RWMutex
	health       *health.Server
	shuttingDown bool
}

// Liveness reports whether the process is running and its SQLite store answers
// a ping. A node without a leader is still live.
func (s *Server) Liveness() bool {
	if s == nil || s.store == nil {
		return false
	}
	s.health.mu.RLock()
	shuttingDown := s.health.shuttingDown
	s.health.mu.RUnlock()
	if shuttingDown {
		return false
	}
	return s.store.Ping() == nil
}

// Ready reports whether the node can serve user traffic: it is live, its
// repairer is initialized, and Raft knows a leader. Followers with a leader are
// ready because they forward client requests.
func (s *Server) Ready() bool {
	if !s.Liveness() {
		return false
	}
	if s.repairer == nil || s.node == nil || s.node.Raft == nil {
		return false
	}
	_, leaderID := s.node.Raft.LeaderWithID()
	return leaderID != ""
}

// SetHealthServer attaches the standard gRPC health service. The process wiring
// calls it before Serve so startup transitions are visible immediately.
func (s *Server) SetHealthServer(hs *health.Server) {
	s.health.mu.Lock()
	s.health.health = hs
	s.health.mu.Unlock()
	s.RefreshHealth()
}

// RefreshHealth publishes the current liveness and readiness statuses.
func (s *Server) RefreshHealth() {
	s.health.mu.RLock()
	hs := s.health.health
	s.health.mu.RUnlock()
	if hs == nil {
		return
	}

	live := healthpb.HealthCheckResponse_NOT_SERVING
	if s.Liveness() {
		live = healthpb.HealthCheckResponse_SERVING
	}
	ready := healthpb.HealthCheckResponse_NOT_SERVING
	if s.Ready() {
		ready = healthpb.HealthCheckResponse_SERVING
	}

	hs.SetServingStatus("", live)
	hs.SetServingStatus(HealthServiceName, ready)
}

// StartHealthMonitor refreshes the health statuses until ctx is canceled. It is
// idempotent per process because the caller owns the context.
func (s *Server) StartHealthMonitor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(readinessPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.MarkShuttingDown()
				return
			case <-ticker.C:
				s.RefreshHealth()
			}
		}
	}()
}

// MarkShuttingDown flips readiness and liveness to NOT_SERVING. It is
// idempotent and is the first step of graceful shutdown.
func (s *Server) MarkShuttingDown() {
	s.health.mu.Lock()
	s.health.shuttingDown = true
	s.health.mu.Unlock()
	s.RefreshHealth()
}

// ShuttingDown reports whether shutdown has begun.
func (s *Server) ShuttingDown() bool {
	s.health.mu.RLock()
	defer s.health.mu.RUnlock()
	return s.health.shuttingDown
}

// Heartbeat stays wire-compatible with the original liveness RPC but reports
// readiness: true means the node is live, initialized, and has a known leader.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &pb.HeartbeatResponse{Alive: s.Ready()}, nil
}
