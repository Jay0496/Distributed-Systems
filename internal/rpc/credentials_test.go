package rpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type authCaptureServer struct {
	grpc_health_v1.UnimplementedHealthServer
	calls chan []string
}

func (s *authCaptureServer) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.calls <- md.Get(AuthMetadataKey)
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func TestTokenFromEnv(t *testing.T) {
	t.Setenv("AUTH_TOKEN", "  secret-token  ")
	if got := TokenFromEnv(); got != "secret-token" {
		t.Fatalf("TokenFromEnv = %q, want secret-token", got)
	}

	t.Setenv("AUTH_TOKEN", "")
	if got := TokenFromEnv(); got != "" {
		t.Fatalf("TokenFromEnv = %q, want empty", got)
	}
}

func TestAppendAuth(t *testing.T) {
	ctx := context.Background()

	if got := AppendAuth(ctx, ""); got != ctx {
		t.Fatal("AppendAuth with an empty token changed the context")
	}

	md, ok := metadata.FromOutgoingContext(AppendAuth(ctx, "abc"))
	if !ok {
		t.Fatal("AppendAuth did not attach outgoing metadata")
	}
	values := md.Get(AuthMetadataKey)
	if len(values) != 1 || values[0] != "Bearer abc" {
		t.Fatalf("authorization = %v, want Bearer abc", values)
	}
}

func TestDialOptionsRejectsNonPositiveLimit(t *testing.T) {
	for _, limit := range []int{0, -1} {
		if _, err := DialOptions("secret", limit); err == nil {
			t.Fatalf("DialOptions limit %d succeeded, want an error", limit)
		} else if strings.Contains(err.Error(), "secret") {
			t.Fatalf("DialOptions error leaked the token: %v", err)
		}
	}
}

func TestDialOptionsAttachesTokenToUnaryCalls(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	capture := &authCaptureServer{calls: make(chan []string, 1)}
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, capture)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	opts, err := DialOptions("secret-token", 1<<20)
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	opts = append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}
	select {
	case values := <-capture.calls:
		if len(values) != 1 || values[0] != "Bearer secret-token" {
			t.Fatalf("authorization = %v, want Bearer secret-token", values)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the call")
	}
}

func TestDialOptionsWithoutTokenSendsNoAuthorization(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	capture := &authCaptureServer{calls: make(chan []string, 1)}
	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, capture)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	opts, err := DialOptions("", 1<<20)
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	opts = append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}
	select {
	case values := <-capture.calls:
		if len(values) != 0 {
			t.Fatalf("authorization = %v, want none", values)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the call")
	}
}
