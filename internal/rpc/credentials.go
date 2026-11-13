// Package rpc contains shared gRPC client helpers used by the server's
// internal clients, the failure detector, repair, and the gateway. It owns the
// outbound bearer-token format so every component attaches AUTH_TOKEN the same
// way.
package rpc

import (
	"context"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// AuthMetadataKey is the outgoing metadata key used for bearer credentials.
const AuthMetadataKey = "authorization"

// TokenFromEnv returns AUTH_TOKEN, or "" when auth is disabled. Whitespace is
// trimmed so an empty Docker environment value does not look like a token.
func TokenFromEnv() string {
	return strings.TrimSpace(os.Getenv("AUTH_TOKEN"))
}

// AppendAuth attaches "authorization: Bearer <token>" to ctx when token is
// non-empty. An empty token returns ctx unchanged so auth-disabled paths have
// no accidental metadata.
func AppendAuth(ctx context.Context, token string) context.Context {
	token = strings.TrimSpace(token)
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, AuthMetadataKey, "Bearer "+token)
}

// DialOptions returns the shared client options for internal RPCs: an optional
// interceptor that attaches the token to every unary and streaming call, plus
// call options that raise the gRPC message limit to maxMessageBytes. The
// default gRPC receive limit is 4 MiB, below the configured value and fragment
// limits, so every internal client must set it.
//
// Callers are responsible for adding transport credentials. The returned
// error never includes the token.
func DialOptions(token string, maxMessageBytes int) ([]grpc.DialOption, error) {
	if maxMessageBytes <= 0 {
		return nil, fmt.Errorf("max message bytes must be positive, got %d", maxMessageBytes)
	}

	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	}

	token = strings.TrimSpace(token)
	opts = append(opts,
		grpc.WithChainUnaryInterceptor(func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			callOpts ...grpc.CallOption,
		) error {
			return invoker(AppendRequestID(AppendAuth(ctx, token)), method, req, reply, cc, callOpts...)
		}),
		grpc.WithChainStreamInterceptor(func(
			ctx context.Context,
			desc *grpc.StreamDesc,
			cc *grpc.ClientConn,
			method string,
			streamer grpc.Streamer,
			callOpts ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			return streamer(AppendRequestID(AppendAuth(ctx, token)), desc, cc, method, callOpts...)
		}),
	)
	return opts, nil
}
