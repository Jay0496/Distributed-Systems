package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"

	"distributed-kv-store/internal/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// authExemptMethods are the only RPCs that bypass bearer-token checks. Container
// health checks use these methods and must keep working when AUTH_TOKEN is set.
var authExemptMethods = map[string]struct{}{
	"/grpc.health.v1.Health/Check": {},
	"/grpc.health.v1.Health/Watch": {},
}

// isAuthExempt reports whether method is an unauthenticated health check.
func isAuthExempt(method string) bool {
	_, ok := authExemptMethods[method]
	return ok
}

// UnaryAuthInterceptor returns a unary interceptor that requires
// "authorization: Bearer <token>" on every method except health checks.
func UnaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorizeToken(ctx, info.FullMethod, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamAuthInterceptor returns the streaming equivalent of
// UnaryAuthInterceptor. Only health Watch is exempt.
func StreamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorizeToken(stream.Context(), info.FullMethod, token); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

// AuthServerOptions returns the unary and stream interceptors for token. The
// process wiring appends these to the gRPC server options when AUTH_TOKEN is
// set and calls Server.EnableAuth so CheckAuth can refuse a partial setup.
func AuthServerOptions(token string) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryAuthInterceptor(token)),
		grpc.ChainStreamInterceptor(StreamAuthInterceptor(token)),
	}
}

// authorizeToken validates the presented bearer credential for one method.
// Health checks are exempt. An empty configured token rejects every
// non-health method because the interceptor is only installed for a real token.
func authorizeToken(ctx context.Context, method, token string) error {
	if isAuthExempt(method) {
		return nil
	}
	if token == "" {
		return status.Error(codes.Unauthenticated, "AUTH_TOKEN is not configured")
	}

	presented, ok := bearerToken(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing bearer token")
	}
	if !constantTimeEqual(presented, token) {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	return nil
}

// bearerToken extracts the token from one authorization metadata value.
func bearerToken(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	const prefix = "bearer "
	for _, value := range md.Get(rpc.AuthMetadataKey) {
		if len(value) < len(prefix) || !strings.EqualFold(value[:len(prefix)], prefix) {
			continue
		}
		token := strings.TrimSpace(value[len(prefix):])
		if token != "" {
			return token, true
		}
	}
	return "", false
}

// constantTimeEqual compares two tokens through fixed-size digests so the
// comparison does not leak the token length through an early exit.
func constantTimeEqual(presented, expected string) bool {
	presentedDigest := sha256.Sum256([]byte(presented))
	expectedDigest := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(presentedDigest[:], expectedDigest[:]) == 1
}

// EnableAuth records that the process installed the bearer-token interceptors.
// It must be called before CheckAuth when AUTH_TOKEN is set.
func (s *Server) EnableAuth() {
	s.authEnforced = true
}

// CheckAuth refuses to start an authenticated server without the interceptors.
// A nil/empty AUTH_TOKEN leaves authentication disabled, and the process logs a
// warning at startup.
func (s *Server) CheckAuth() error {
	if s.cfg.AuthToken == "" {
		return nil
	}
	if !s.authEnforced {
		return errors.New("AUTH_TOKEN is set but the auth interceptors are not installed")
	}
	return nil
}
