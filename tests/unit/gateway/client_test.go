package gateway_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/gateway"
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type fakeStorage struct {
	pb.UnimplementedStorageServiceServer

	mu       sync.Mutex
	lastAuth string
	putCalls int
}

func (f *fakeStorage) Put(ctx context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	f.mu.Lock()
	f.putCalls++
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(rpc.AuthMetadataKey); len(values) > 0 {
			f.lastAuth = values[0]
		}
	}
	f.mu.Unlock()
	return &pb.PutResponse{Success: true, Version: 1, Mode: "rs32", ShardsStored: 5}, nil
}

func (f *fakeStorage) Get(context.Context, *pb.GetRequest) (*pb.GetResponse, error) {
	return &pb.GetResponse{Success: true, Value: []byte("value"), Mode: "rs32", Version: 1}, nil
}

func (f *fakeStorage) auth() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastAuth
}

func (f *fakeStorage) puts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putCalls
}

func startFakeStorage(t *testing.T, impl pb.StorageServiceServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterStorageServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis
}

func testConfig(nodes ...string) config.GatewayConfig {
	peers := make(map[string]string, len(nodes))
	for _, node := range nodes {
		peers[node] = node
	}
	return config.GatewayConfig{
		Nodes:           peers,
		Order:           nodes,
		VNodes:          50,
		Timeout:         2 * time.Second,
		Retries:         0,
		MaxMessageBytes: 1 << 20,
	}
}

func TestNewClientRejectsEmptyNodes(t *testing.T) {
	if _, err := gateway.NewClient(config.GatewayConfig{}); err == nil {
		t.Fatal("NewClient accepted an empty node map")
	}
}

func TestClientPutAndGet(t *testing.T) {
	fake := &fakeStorage{}
	lis := startFakeStorage(t, fake)
	dialer := support.NewDialer()
	dialer.Register("node1", lis)

	client := gateway.NewClientWithDialer(testConfig("node1"), dialer.Dial)
	resp := mustPut(t, client, "k")
	if !resp.GetSuccess() || fake.puts() != 1 {
		t.Fatalf("Put response = %+v, calls = %d", resp, fake.puts())
	}
	_, got, err := client.Get(context.Background(), "k")
	if err != nil || string(got.GetValue()) != "value" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
}

func TestClientRetriesPastAStoppedEntryNode(t *testing.T) {
	fake := &fakeStorage{}
	lis := startFakeStorage(t, fake)
	dialer := support.NewDialer()
	dialer.Register("node2", lis)
	// node1 is intentionally not registered, so its dial fails Unavailable.

	cfg := testConfig("node1", "node2")
	cfg.Retries = 1
	client := gateway.NewClientWithDialer(cfg, dialer.Dial)

	router := gateway.NewRouter([]string{"node1", "node2"}, cfg.VNodes)
	key := ""
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("retry-%d", i)
		if node, _ := router.GetNode(candidate); node == "node1" {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("could not find a key routed to node1")
	}

	resp := mustPut(t, client, key)
	if !resp.GetSuccess() || fake.puts() != 1 {
		t.Fatalf("retry Put response = %+v, calls = %d", resp, fake.puts())
	}
}

func TestClientAttachesBearerToken(t *testing.T) {
	fake := &fakeStorage{}
	lis := startFakeStorage(t, fake)
	dialer := support.NewDialer()
	dialer.Register("node1", lis)

	cfg := testConfig("node1")
	cfg.Token = "secret-token"
	client := gateway.NewClientWithDialer(cfg, func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		return dialer.DialWithToken(ctx, target, cfg.Token)
	})

	mustPut(t, client, "k")
	if got := fake.auth(); got != "Bearer secret-token" {
		t.Fatalf("authorization = %q, want Bearer secret-token", got)
	}
}

func mustPut(t *testing.T, client *gateway.Client, key string) *pb.PutResponse {
	t.Helper()
	_, resp, err := client.Put(context.Background(), &pb.PutRequest{Key: key, Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return resp
}
