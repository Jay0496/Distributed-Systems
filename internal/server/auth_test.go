package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const authTestBufSize = 8 << 20

type authTestService struct {
	pb.UnimplementedStorageServiceServer

	mu       sync.Mutex
	lastAuth string
	data     []byte
	checksum string
}

func (s *authTestService) record(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAuth = incomingAuth(ctx)
}

func incomingAuth(ctx context.Context) string {
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

func (s *authTestService) Put(ctx context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	s.record(ctx)
	return &pb.PutResponse{Success: true, Message: "write committed"}, nil
}

func (s *authTestService) NodeStatus(ctx context.Context, _ *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	s.record(ctx)
	return &pb.NodeStatusResponse{NodeId: "node2", State: "Follower", LeaderId: "node1"}, nil
}

func (s *authTestService) Stats(ctx context.Context, _ *pb.StatsRequest) (*pb.StatsResponse, error) {
	s.record(ctx)
	return &pb.StatsResponse{NodeId: "node2", Leader: false, Mode: "rs32"}, nil
}

func (s *authTestService) UploadFragment(stream pb.StorageService_UploadFragmentServer) error {
	s.record(stream.Context())
	return stream.SendAndClose(&pb.FragmentResponse{Success: true, Message: "stored"})
}

func (s *authTestService) DownloadFragment(req *pb.FragmentRequest, stream pb.StorageService_DownloadFragmentServer) error {
	s.record(stream.Context())
	return stream.Send(&pb.FragmentChunk{
		StripeId:   req.GetStripeId(),
		ShardIndex: req.GetShardIndex(),
		Checksum:   s.checksum,
		Offset:     0,
		TotalSize:  int64(len(s.data)),
		Data:       s.data,
		Last:       true,
	})
}

func (s *authTestService) DeleteFragment(ctx context.Context, _ *pb.DeleteFragmentRequest) (*pb.FragmentOpResponse, error) {
	s.record(ctx)
	return &pb.FragmentOpResponse{Success: true, Message: "deleted"}, nil
}

func (s *authTestService) auth() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuth
}

// startAuthServer starts a gRPC server with the bearer interceptors and health
// service, returning its bufconn listener.
func startAuthServer(t *testing.T, token string, impl pb.StorageServiceServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(authTestBufSize)
	srv := grpc.NewServer(AuthServerOptions(token)...)
	pb.RegisterStorageServiceServer(srv, impl)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("storage.StorageService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis
}

// dialTest opens a connection to lis. When withHelper is true it uses the
// shared internal/rpc options; otherwise it uses a raw unauthenticated dial.
func dialTest(t *testing.T, lis *bufconn.Listener, token string, withHelper bool) *grpc.ClientConn {
	t.Helper()

	var opts []grpc.DialOption
	if withHelper {
		shared, err := rpc.DialOptions(token, config.DefaultMaxMessageSize)
		if err != nil {
			t.Fatalf("DialOptions: %v", err)
		}
		opts = append(opts, shared...)
	}
	opts = append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", opts...)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestUnaryAuthInterceptorAcceptsValidToken(t *testing.T) {
	fake := &authTestService{}
	lis := startAuthServer(t, "secret", fake)
	conn := dialTest(t, lis, "secret", true)

	resp, err := pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("response = %+v", resp)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("authorization = %q, want Bearer secret", got)
	}
}

func TestUnaryAuthInterceptorRejectsMissingToken(t *testing.T) {
	lis := startAuthServer(t, "secret", &authTestService{})
	conn := dialTest(t, lis, "", false)

	_, err := pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated (err=%v)", status.Code(err), err)
	}
}

func TestUnaryAuthInterceptorRejectsWrongToken(t *testing.T) {
	lis := startAuthServer(t, "secret", &authTestService{})
	conn := dialTest(t, lis, "wrong", true)

	_, err := pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated (err=%v)", status.Code(err), err)
	}
}

func TestHealthCheckIsExemptFromAuth(t *testing.T) {
	lis := startAuthServer(t, "secret", &authTestService{})
	conn := dialTest(t, lis, "", false)

	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{
		Service: "storage.StorageService",
	})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status = %s", resp.GetStatus())
	}
}

func TestStreamAuthInterceptorAcceptsValidToken(t *testing.T) {
	fake := &authTestService{}
	lis := startAuthServer(t, "secret", fake)
	conn := dialTest(t, lis, "secret", true)

	stream, err := pb.NewStorageServiceClient(conn).UploadFragment(context.Background())
	if err != nil {
		t.Fatalf("UploadFragment: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("response = %+v", resp)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("authorization = %q, want Bearer secret", got)
	}
}

func TestStreamAuthInterceptorRejectsMissingToken(t *testing.T) {
	lis := startAuthServer(t, "secret", &authTestService{})
	conn := dialTest(t, lis, "", false)

	stream, err := pb.NewStorageServiceClient(conn).UploadFragment(context.Background())
	if err != nil {
		t.Fatalf("UploadFragment: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated (err=%v)", status.Code(err), err)
	}
}

func TestCheckAuthRequiresInterceptors(t *testing.T) {
	open := &Server{cfg: config.Config{}}
	if err := open.CheckAuth(); err != nil {
		t.Fatalf("auth-disabled CheckAuth = %v", err)
	}

	locked := &Server{cfg: config.Config{AuthToken: "token"}}
	if err := locked.CheckAuth(); err == nil {
		t.Fatal("CheckAuth accepted a token without interceptors")
	}

	locked.EnableAuth()
	if err := locked.CheckAuth(); err != nil {
		t.Fatalf("CheckAuth after EnableAuth = %v", err)
	}
}

// authServerAndDialer starts an authenticated fake node and returns a server
// dialer wired through the production internal-client helper.
func authServerAndDialer(t *testing.T, token string, fake *authTestService) (config.Config, dialFunc, *bufconn.Listener) {
	t.Helper()
	lis := startAuthServer(t, "secret", fake)
	cfg := config.Config{
		NodeID:          "node1",
		AuthToken:       token,
		MaxMessageSize:  1 << 20,
		UploadChunkSize: 1 << 20,
		Peers:           map[string]string{"node2": "node2"},
	}.WithDefaults()

	dial := dialFuncFor(cfg, func(ctx context.Context, _ string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
		return grpc.DialContext(ctx, "bufnet", opts...)
	})
	return cfg, dial, lis
}

func TestForwardingDialerCarriesToken(t *testing.T) {
	fake := &authTestService{}
	_, dial, _ := authServerAndDialer(t, "secret", fake)

	conn, err := dial(context.Background(), "node2")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"}); err != nil {
		t.Fatalf("Put through forwarding dialer: %v", err)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("authorization = %q, want Bearer secret", got)
	}
}

func TestInternalClientWithoutHelperIsRejected(t *testing.T) {
	fake := &authTestService{}
	_, dial, _ := authServerAndDialer(t, "", fake)

	conn, err := dial(context.Background(), "node2")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, err = pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated (err=%v)", status.Code(err), err)
	}
}

func TestFragmentUploadCarriesToken(t *testing.T) {
	fake := &authTestService{}
	cfg, dial, _ := authServerAndDialer(t, "secret", fake)
	srv := &Server{cfg: cfg, store: newTestStore(), conns: make(map[string]*grpc.ClientConn), dial: dial}

	data := []byte("shard bytes")
	shard := redundancy.Shard{
		Index: 1, Data: data, Checksum: redundancy.Checksum(data), TotalSize: int64(len(data)),
	}
	placement := storage.ShardPlacement{ShardIndex: 1, NodeID: "node2"}
	if err := srv.pushShard(context.Background(), "stripe-1", shard, placement); err != nil {
		t.Fatalf("pushShard: %v", err)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("upload authorization = %q, want Bearer secret", got)
	}
}

func TestFragmentDownloadCarriesToken(t *testing.T) {
	data := []byte("download bytes")
	fake := &authTestService{data: data, checksum: redundancy.Checksum(data)}
	cfg, dial, _ := authServerAndDialer(t, "secret", fake)
	cfg.ReadTimeout = 2 * time.Second
	srv := &Server{cfg: cfg, store: newTestStore(), conns: make(map[string]*grpc.ClientConn), dial: dial}

	placement := storage.ShardPlacement{ShardIndex: 2, NodeID: "node2"}
	shard, err := srv.fetchShard(context.Background(), "stripe-1", placement)
	if err != nil {
		t.Fatalf("fetchShard: %v", err)
	}
	if string(shard.Data) != string(data) {
		t.Fatalf("shard data = %q, want %q", shard.Data, data)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("download authorization = %q, want Bearer secret", got)
	}
}

func TestFragmentDeleteCarriesToken(t *testing.T) {
	fake := &authTestService{}
	cfg, dial, _ := authServerAndDialer(t, "secret", fake)
	srv := &Server{cfg: cfg, store: newTestStore(), conns: make(map[string]*grpc.ClientConn), dial: dial}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := srv.getConn(ctx, "node2")
	if err != nil {
		t.Fatalf("getConn: %v", err)
	}
	if _, err := pb.NewStorageServiceClient(conn).DeleteFragment(ctx, &pb.DeleteFragmentRequest{
		StripeId: "stripe-1", ShardIndex: 1,
	}); err != nil {
		t.Fatalf("DeleteFragment: %v", err)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("delete authorization = %q, want Bearer secret", got)
	}
}

func TestAdminProbeCarriesToken(t *testing.T) {
	fake := &authTestService{}
	cfg, dial, _ := authServerAndDialer(t, "secret", fake)
	srv := &Server{cfg: cfg, store: newTestStore(), conns: make(map[string]*grpc.ClientConn), dial: dial}

	client, closer, err := srv.peerClient(context.Background(), "node2")
	if err != nil {
		t.Fatalf("peerClient: %v", err)
	}
	defer closer()
	if _, err := client.Stats(context.Background(), &pb.StatsRequest{}); err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got := fake.auth(); got != "Bearer secret" {
		t.Fatalf("admin authorization = %q, want Bearer secret", got)
	}
}
