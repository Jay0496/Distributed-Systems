package support

import (
	"context"
	"fmt"
	"net"
	"sync"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Dialer is a registry-backed gRPC dialer for tests that need named targets
// without a Cluster. It can be used as the server's dialFunc or the gateway's
// DialFunc and supports per-call tokens for auth tests.
type Dialer struct {
	mu        sync.RWMutex
	listeners map[string]*bufconn.Listener
}

// NewDialer returns an empty Dialer.
func NewDialer() *Dialer {
	return &Dialer{listeners: make(map[string]*bufconn.Listener)}
}

// Register makes target resolvable. Re-registering replaces the listener so a
// test can model a restarted process on the same logical address.
func (d *Dialer) Register(target string, listener *bufconn.Listener) {
	d.mu.Lock()
	d.listeners[target] = listener
	d.mu.Unlock()
}

// Unregister makes target fail with Unavailable, modelling a stopped node.
func (d *Dialer) Unregister(target string) {
	d.mu.Lock()
	delete(d.listeners, target)
	d.mu.Unlock()
}

// Dial dials target without an auth token.
func (d *Dialer) Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return d.DialWithToken(ctx, target, "")
}

// DialWithToken dials target and attaches token to every unary and streaming
// call when token is non-empty.
func (d *Dialer) DialWithToken(ctx context.Context, target, token string) (*grpc.ClientConn, error) {
	d.mu.RLock()
	listener := d.listeners[target]
	d.mu.RUnlock()
	if listener == nil {
		return nil, fmt.Errorf("support: target %q is not registered", target)
	}

	opts, err := rpc.DialOptions(token, config.DefaultMaxMessageSize)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	return grpc.DialContext(ctx, "bufnet", opts...)
}
