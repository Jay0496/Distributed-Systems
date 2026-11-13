package rpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RequestIDMetadataKey is the incoming and outgoing metadata key used to
// correlate one client request across forwarding, fragment calls, and logs.
const RequestIDMetadataKey = "x-request-id"

type requestIDContextKey struct{}

// NewRequestID returns a random 128-bit hexadecimal request identifier.
func NewRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// crypto/rand failure is effectively unrecoverable, but a request
		// identifier must never block request handling.
		return ""
	}
	return hex.EncodeToString(raw[:])
}

// WithRequestID stores id on ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	id = strings.TrimSpace(id)
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDContextKey{}, id)
}

// RequestID returns the request identifier stored on ctx, or "".
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDContextKey{}).(string)
	return id
}

// EnsureRequestID reuses an incoming request ID or generates one, and returns a
// context carrying it.
func EnsureRequestID(ctx context.Context) (context.Context, string) {
	if id := RequestID(ctx); id != "" {
		return ctx, id
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, value := range md.Get(RequestIDMetadataKey) {
			if id := strings.TrimSpace(value); id != "" {
				return WithRequestID(ctx, id), id
			}
		}
	}
	id := NewRequestID()
	return WithRequestID(ctx, id), id
}

// AppendRequestID copies a request ID from ctx to outgoing metadata.
func AppendRequestID(ctx context.Context) context.Context {
	id := RequestID(ctx)
	if id == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, RequestIDMetadataKey, id)
}

// UnaryRequestIDInterceptor installs a request ID on every unary call and echoes
// it in the response header when the client did not supply one.
func UnaryRequestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, id := EnsureRequestID(ctx)
		if id != "" {
			_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDMetadataKey, id))
		}
		return handler(ctx, req)
	}
}

// StreamRequestIDInterceptor is the streaming equivalent of
// UnaryRequestIDInterceptor.
func StreamRequestIDInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, id := EnsureRequestID(stream.Context())
		if id != "" {
			_ = stream.SetHeader(metadata.Pairs(RequestIDMetadataKey, id))
		}
		return handler(srv, &requestIDStream{ServerStream: stream, ctx: ctx})
	}
}

// RequestIDServerOptions returns the unary and stream request-ID interceptors.
func RequestIDServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryRequestIDInterceptor()),
		grpc.ChainStreamInterceptor(StreamRequestIDInterceptor()),
	}
}

type requestIDStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *requestIDStream) Context() context.Context { return s.ctx }
