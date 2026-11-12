package server_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/tests/support"

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

type authService struct {
	pb.UnimplementedStorageServiceServer

	mu   sync.Mutex
	auth string
}

func (s *authService) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(rpc.AuthMetadataKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(values) > 0 {
		s.auth = values[0]
	} else {
		s.auth = ""
	}
}

func (s *authService) Put(ctx context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	s.record(ctx)
	return &pb.PutResponse{Success: true}, nil
}

func (s *authService) UploadFragment(stream pb.StorageService_UploadFragmentServer) error {
	s.record(stream.Context())
	return stream.SendAndClose(&pb.FragmentResponse{Success: true})
}

func (s *authService) lastAuth() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.auth
}

func startAuthServer(t *testing.T, token string, impl pb.StorageServiceServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(server.AuthServerOptions(token)...)
	pb.RegisterStorageServiceServer(srv, impl)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis
}

func dialAuth(t *testing.T, lis *bufconn.Listener, token string) *grpc.ClientConn {
	t.Helper()
	opts, err := rpc.DialOptions(token, config.DefaultMaxMessageSize)
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
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
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestUnaryAuthInterceptor(t *testing.T) {
	fake := &authService{}
	lis := startAuthServer(t, "secret", fake)

	conn := dialAuth(t, lis, "secret")
	if _, err := pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"}); err != nil {
		t.Fatalf("valid token Put: %v", err)
	}
	if got := fake.lastAuth(); got != "Bearer secret" {
		t.Fatalf("authorization = %q", got)
	}

	for name, token := range map[string]string{"missing": "", "wrong": "wrong"} {
		t.Run(name, func(t *testing.T) {
			conn := dialAuth(t, lis, token)
			_, err := pb.NewStorageServiceClient(conn).Put(context.Background(), &pb.PutRequest{Key: "k"})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("code = %s, want Unauthenticated (err=%v)", status.Code(err), err)
			}
		})
	}
}

func TestStreamAuthInterceptor(t *testing.T) {
	lis := startAuthServer(t, "secret", &authService{})
	conn := dialAuth(t, lis, "secret")
	stream, err := pb.NewStorageServiceClient(conn).UploadFragment(context.Background())
	if err != nil {
		t.Fatalf("UploadFragment: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("authenticated stream: %v", err)
	}

	raw := dialAuth(t, lis, "")
	stream, err = pb.NewStorageServiceClient(raw).UploadFragment(context.Background())
	if err != nil {
		t.Fatalf("UploadFragment without token: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated", status.Code(err))
	}
}

func TestHealthCheckIsExemptFromAuth(t *testing.T) {
	lis := startAuthServer(t, "secret", &authService{})
	conn := dialAuth(t, lis, "")
	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %s", resp.GetStatus())
	}
}

func TestCheckAuthRequiresInterceptors(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	open := server.New(config.Config{}, store, nil, nil)
	if err := open.CheckAuth(); err != nil {
		t.Fatalf("auth-disabled CheckAuth = %v", err)
	}

	locked := server.New(config.Config{AuthToken: "token"}, store, nil, nil)
	if err := locked.CheckAuth(); err == nil {
		t.Fatal("CheckAuth accepted a token without interceptors")
	}
	locked.EnableAuth()
	if err := locked.CheckAuth(); err != nil {
		t.Fatalf("CheckAuth after EnableAuth = %v", err)
	}
}
