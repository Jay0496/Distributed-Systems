package gateway

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/rpc"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 8 << 20

// fakeStorage is a controllable StorageService for client retry tests.
type fakeStorage struct {
	pb.UnimplementedStorageServiceServer

	mu       sync.Mutex
	putCalls int
	getCalls int
	put      func(*pb.PutRequest) (*pb.PutResponse, error)
	getFn    func(*pb.GetRequest) (*pb.GetResponse, error)
	status   func() *pb.NodeStatusResponse
	lastAuth string
}

func (f *fakeStorage) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	f.mu.Lock()
	f.putCalls++
	f.lastAuth = authHeader(ctx)
	fn := f.put
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &pb.PutResponse{Success: true, Version: 1, Mode: "rs32", ShardsStored: 5}, nil
}

func (f *fakeStorage) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	f.mu.Lock()
	f.getCalls++
	fn := f.getFn
	f.mu.Unlock()
	if fn != nil {
		return fn(req)
	}
	return &pb.GetResponse{Success: true, Value: []byte("value"), Mode: "rs32", Version: 1}, nil
}

func (f *fakeStorage) NodeStatus(context.Context, *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	if f.status != nil {
		return f.status(), nil
	}
	return &pb.NodeStatusResponse{}, nil
}

func (f *fakeStorage) calls() (put, get int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putCalls, f.getCalls
}

func authHeader(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(rpc.AuthMetadataKey)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// startFake starts one bufconn gRPC server and returns its listener.
func startFake(t *testing.T, impl pb.StorageServiceServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(testBufSize)
	srv := grpc.NewServer()
	pb.RegisterStorageServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis
}

// dialMap builds a DialFunc that connects to the supplied bufconn listeners and
// fails every other target, modelling unreachable nodes.
func dialMap(t *testing.T, listeners map[string]*bufconn.Listener) DialFunc {
	t.Helper()
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		lis, ok := listeners[target]
		if !ok || lis == nil {
			return nil, status.Errorf(codes.Unavailable, "dial %s: connection refused", target)
		}
		return grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
	}
}

func testGatewayConfig(nodes ...string) config.GatewayConfig {
	peers := make(map[string]string, len(nodes))
	for _, node := range nodes {
		peers[node] = node
	}
	return config.GatewayConfig{
		Nodes:           peers,
		VNodes:          10,
		Timeout:         2 * time.Second,
		Retries:         2,
		MaxMessageBytes: 1 << 20,
	}
}

func newTestClient(t *testing.T, cfg config.GatewayConfig, listeners map[string]*bufconn.Listener) *Client {
	t.Helper()
	client := newClientWithDialer(cfg, dialMap(t, listeners))
	client.sleep = func(time.Duration) {}
	return client
}

// keyForEntry finds a key whose candidate order starts with want.
func keyForEntry(t *testing.T, client *Client, want string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := "key-" + string(rune('a'+i%26)) + strings.Repeat("x", i/26)
		candidates := client.router.Candidates(key)
		if len(candidates) > 0 && candidates[0] == want {
			return key
		}
	}
	t.Fatalf("could not find a key routed to %s", want)
	return ""
}

func TestNewClientRejectsEmptyNodes(t *testing.T) {
	if _, err := NewClient(config.GatewayConfig{}); err == nil {
		t.Fatal("NewClient accepted empty node configuration")
	}
}

func TestClientPutSucceeds(t *testing.T) {
	fake := &fakeStorage{}
	client := newTestClient(t, testGatewayConfig("node1"), map[string]*bufconn.Listener{
		"node1": startFake(t, fake),
	})

	node, resp, err := client.Put(context.Background(), &pb.PutRequest{Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if node != "node1" {
		t.Fatalf("served by %q, want node1", node)
	}
	if !resp.GetSuccess() || resp.GetShardsStored() != 5 {
		t.Fatalf("response = %+v", resp)
	}
	if put, _ := fake.calls(); put != 1 {
		t.Fatalf("put calls = %d, want 1", put)
	}
}

func TestClientFallsThroughUnavailableEntry(t *testing.T) {
	fake := &fakeStorage{}
	cfg := testGatewayConfig("node1", "node2", "node3")
	cfg.Retries = 4
	client := newTestClient(t, cfg, map[string]*bufconn.Listener{
		"node2": startFake(t, fake),
		"node3": startFake(t, &fakeStorage{}),
	})

	key := keyForEntry(t, client, "node1")
	node, resp, err := client.Put(context.Background(), &pb.PutRequest{Key: key, Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if node == "node1" {
		t.Fatal("Put succeeded on the unavailable entry node")
	}
	if !resp.GetSuccess() {
		t.Fatalf("response = %+v", resp)
	}
}

func TestClientAllNodesFailWithDiagnostic(t *testing.T) {
	cfg := testGatewayConfig("node1", "node2", "node3")
	cfg.Retries = 1
	client := newTestClient(t, cfg, nil)

	_, _, err := client.Get(context.Background(), "missing")
	if err == nil {
		t.Fatal("Get succeeded with no reachable nodes")
	}
	message := err.Error()
	if !strings.Contains(message, "all 2 attempts failed") {
		t.Fatalf("error %q does not report the attempt count", message)
	}
	for _, node := range client.router.Candidates("missing")[:2] {
		if !strings.Contains(message, node) {
			t.Fatalf("error %q does not name attempted node %q", message, node)
		}
	}
}

func TestClientStopsOnNonRetryableError(t *testing.T) {
	fake := &fakeStorage{
		getFn: func(*pb.GetRequest) (*pb.GetResponse, error) {
			return nil, status.Error(codes.InvalidArgument, "bad key")
		},
	}
	cfg := testGatewayConfig("node1", "node2")
	cfg.Retries = 5
	client := newTestClient(t, cfg, map[string]*bufconn.Listener{
		"node1": startFake(t, fake),
		"node2": startFake(t, &fakeStorage{}),
	})
	key := keyForEntry(t, client, "node1")

	if _, _, err := client.Get(context.Background(), key); err == nil {
		t.Fatal("Get returned no error")
	}
	if _, get := fake.calls(); get != 1 {
		t.Fatalf("get calls = %d, want 1 for a non-retryable error", get)
	}
}

func TestClientRetriesAbortedAgainstNewLeader(t *testing.T) {
	first := &fakeStorage{
		put: func(*pb.PutRequest) (*pb.PutResponse, error) {
			return nil, status.Error(codes.Aborted, "not the leader")
		},
	}
	first.status = func() *pb.NodeStatusResponse {
		return &pb.NodeStatusResponse{State: "follower", LeaderId: "node2"}
	}
	second := &fakeStorage{}

	cfg := testGatewayConfig("node1", "node2")
	cfg.Retries = 2
	client := newTestClient(t, cfg, map[string]*bufconn.Listener{
		"node1": startFake(t, first),
		"node2": startFake(t, second),
	})
	key := keyForEntry(t, client, "node1")

	node, resp, err := client.Put(context.Background(), &pb.PutRequest{Key: key, Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if node != "node2" {
		t.Fatalf("served by %q, want the new leader node2", node)
	}
	if !resp.GetSuccess() {
		t.Fatalf("response = %+v", resp)
	}
	if put, _ := first.calls(); put != 1 {
		t.Fatalf("first node put calls = %d, want 1", put)
	}
	if put, _ := second.calls(); put != 1 {
		t.Fatalf("second node put calls = %d, want 1", put)
	}
}

func TestClientVersionConflictStopsImmediately(t *testing.T) {
	fake := &fakeStorage{
		put: func(*pb.PutRequest) (*pb.PutResponse, error) {
			return nil, status.Error(codes.Aborted, "version mismatch")
		},
	}
	cfg := testGatewayConfig("node1", "node2")
	cfg.Retries = 5
	client := newTestClient(t, cfg, map[string]*bufconn.Listener{
		"node1": startFake(t, fake),
		"node2": startFake(t, &fakeStorage{}),
	})
	key := keyForEntry(t, client, "node1")

	_, _, err := client.Put(context.Background(), &pb.PutRequest{
		Key: key, Value: []byte("v"), ExpectedVersion: 3,
	})
	if err == nil {
		t.Fatal("Put returned no conflict error")
	}
	if !strings.Contains(err.Error(), "version conflict") {
		t.Fatalf("error = %v, want a version conflict", err)
	}
	if put, _ := fake.calls(); put != 1 {
		t.Fatalf("put calls = %d, want 1", put)
	}
}

func TestClientAttachesBearerToken(t *testing.T) {
	fake := &fakeStorage{}
	lis := startFake(t, fake)

	cfg := testGatewayConfig("node1")
	cfg.Token = "secret-token"
	opts, err := rpc.DialOptions(cfg.Token, cfg.MaxMessageBytes)
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	opts = append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	client := newClientWithDialer(cfg, func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, "bufnet", opts...)
	})
	client.sleep = func(time.Duration) {}

	if _, _, err := client.Put(context.Background(), &pb.PutRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	fake.mu.Lock()
	got := fake.lastAuth
	fake.mu.Unlock()
	if got != "Bearer secret-token" {
		t.Fatalf("authorization header = %q, want Bearer secret-token", got)
	}
}

func TestBackoffDelayIsBounded(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		delay := backoffDelay(attempt)
		if delay < 80*time.Millisecond || delay > 600*time.Millisecond {
			t.Fatalf("backoffDelay(%d) = %v, outside the jitter bounds", attempt, delay)
		}
	}
}

func TestMoveFirstDeduplicates(t *testing.T) {
	got := moveFirst("b", []string{"a", "b", "c"})
	want := []string{"b", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("moveFirst = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("moveFirst = %v, want %v", got, want)
		}
	}
	if got := moveFirst("d", []string{"a"}); len(got) != 2 || got[0] != "d" {
		t.Fatalf("moveFirst prepend = %v", got)
	}
}
