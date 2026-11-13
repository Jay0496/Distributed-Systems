package integration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/gateway"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const authClusterToken = "integration-secret"

func startAuthCluster(t *testing.T) *support.Cluster {
	t.Helper()
	return support.StartClusterWithOptions(t, support.ClusterOptions{
		Count:     5,
		AuthToken: authClusterToken,
	})
}

func TestAuthEndToEnd(t *testing.T) {
	c := startAuthCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	raw, err := c.DialRaw(ctx, c.Nodes[0].ID, "")
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer func() { _ = raw.Close() }()
	rawClient := pb.NewStorageServiceClient(raw)

	if _, err := rawClient.Get(ctx, &pb.GetRequest{Key: "missing"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token Get = %v, want Unauthenticated", err)
	}
	if _, err := rawClient.ForceSnapshot(ctx, &pb.ForceSnapshotRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token ForceSnapshot = %v, want Unauthenticated", err)
	}
	if _, err := rawClient.CheckpointStore(ctx, &pb.CheckpointRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token CheckpointStore = %v, want Unauthenticated", err)
	}

	wrong, err := c.DialRaw(ctx, c.Nodes[0].ID, "wrong-token")
	if err != nil {
		t.Fatalf("wrong-token dial: %v", err)
	}
	defer func() { _ = wrong.Close() }()
	if _, err := pb.NewStorageServiceClient(wrong).Get(ctx, &pb.GetRequest{Key: "missing"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token Get = %v, want Unauthenticated", err)
	}

	// Benchmark admin RPCs are not exempt: the harness's authenticated client
	// must succeed on both, and they stay local to the addressed node.
	if _, err := c.Nodes[0].Client.ForceSnapshot(ctx, &pb.ForceSnapshotRequest{}); err != nil {
		t.Fatalf("authenticated ForceSnapshot: %v", err)
	}
	if _, err := c.Nodes[0].Client.CheckpointStore(ctx, &pb.CheckpointRequest{}); err != nil {
		t.Fatalf("authenticated CheckpointStore: %v", err)
	}

	// The harness's ordinary clients include the bearer token. A write sent to
	// a follower must forward internally with the same credential.
	value := []byte("authenticated")
	follower := c.NonLeaderIDs(t)[0]
	resp := c.PutVia(t, follower, &pb.PutRequest{Key: "auth-key", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("authenticated forwarded Put rejected: %s", resp.GetMessage())
	}
	got, err := c.GetVia(t, follower, "auth-key")
	if err != nil {
		t.Fatalf("authenticated forwarded Get: %v", err)
	}
	if !bytes.Equal(got.GetValue(), value) {
		t.Fatalf("Get returned %d bytes", len(got.GetValue()))
	}

	// Health checks are deliberately exempt so Compose healthchecks work.
	health, err := healthpb.NewHealthClient(raw).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check without token: %v", err)
	}
	if health.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status = %s", health.GetStatus())
	}
}

func TestAuthPropagatesToFragmentCalls(t *testing.T) {
	c := startAuthCluster(t)
	node := c.Nodes[0]
	data := []byte("authenticated-fragment")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	raw, err := c.DialRaw(ctx, node.ID, "")
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer func() { _ = raw.Close() }()
	stream, err := pb.NewStorageServiceClient(raw).UploadFragment(ctx)
	if err != nil {
		t.Fatalf("raw UploadFragment: %v", err)
	}
	if err := stream.Send(&pb.FragmentChunk{
		StripeId: "auth-fragment", ShardIndex: 1, Checksum: "00",
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated fragment upload = %v, want Unauthenticated", err)
	}

	// The node's own client carries the token, as do server-internal pushes.
	resp := uploadFragment(t, node.Client, "auth-fragment", 1, data, len(data), false)
	if !resp.GetSuccess() {
		t.Fatalf("authenticated upload rejected: %s", resp.GetMessage())
	}
}

func TestGatewayAuth(t *testing.T) {
	c := startAuthCluster(t)
	cfg := config.GatewayConfig{
		Nodes:           map[string]string{},
		Order:           nil,
		VNodes:          50,
		Timeout:         20 * time.Second,
		Retries:         1,
		Token:           authClusterToken,
		MaxMessageBytes: 8 << 20,
	}
	for _, node := range c.Nodes {
		cfg.Nodes[node.ID] = node.ID
		cfg.Order = append(cfg.Order, node.ID)
	}

	client := gateway.NewClientWithDialer(cfg, c.DialerWithToken(authClusterToken))
	_, resp, err := client.Put(context.Background(), &pb.PutRequest{Key: "gateway-auth", Value: []byte("v")})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("authenticated gateway Put = %+v, %v", resp, err)
	}

	bad := gateway.NewClientWithDialer(cfg, c.DialerWithToken("wrong-token"))
	if _, _, err := bad.Get(context.Background(), "gateway-auth"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("gateway with wrong token = %v, want Unauthenticated", err)
	}
}
