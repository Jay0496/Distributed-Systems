package server

import (
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/rpc"

	pb "distributed-kv-store/proto"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// TestReadinessFollowsLeaderAndShutdown proves that readiness is distinct from
// liveness and that the heartbeat RPC reports readiness.
func TestReadinessFollowsLeaderAndShutdown(t *testing.T) {
	c := newTestCluster(t, 1)
	leader := c.waitLeader(t)

	leader.srv.RefreshHealth()

	if !leader.srv.Liveness() {
		t.Fatal("live node reported not live")
	}
	if !leader.srv.Ready() {
		t.Fatal("leader with a repairer reported not ready")
	}
	if status := healthStatus(t, leader, HealthServiceName); status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("readiness status = %s", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := leader.client.Heartbeat(ctx, &pb.HeartbeatRequest{})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !resp.GetAlive() {
		t.Fatal("heartbeat reported not alive for a ready leader")
	}

	leader.srv.MarkShuttingDown()
	if leader.srv.Ready() {
		t.Fatal("shutting-down node reported ready")
	}
	if status := healthStatus(t, leader, HealthServiceName); status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness after shutdown = %s", status)
	}
	resp, err = leader.client.Heartbeat(ctx, &pb.HeartbeatRequest{})
	if err != nil {
		t.Fatalf("Heartbeat after shutdown: %v", err)
	}
	if resp.GetAlive() {
		t.Fatal("heartbeat reported ready after shutdown")
	}
}

// TestForwardingPropagatesRequestID proves that an inbound correlation header
// survives a follower-to-leader forward.
func TestForwardingPropagatesRequestID(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	var follower *testNode
	for _, node := range c.nodes {
		if node != leader {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower in the test cluster")
	}

	const requestID = "forward-request-id"
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		rpc.RequestIDMetadataKey, requestID,
	))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := follower.client.Put(ctx, &pb.PutRequest{
		Key:   "forwarded",
		Value: []byte("value"),
		Mode:  "rs32",
	})
	if err != nil {
		t.Fatalf("Put through follower: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	if got := leader.lastRequestID(); got != requestID {
		t.Fatalf("leader request ID = %q, want %q", got, requestID)
	}
}

func healthStatus(t *testing.T, node *testNode, service string) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := node.health.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		t.Fatalf("health check %q: %v", service, err)
	}
	return resp.GetStatus()
}
