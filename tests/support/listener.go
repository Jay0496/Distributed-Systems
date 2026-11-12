package support

import (
	"context"
	"net"
	"sync"

	"google.golang.org/grpc/test/bufconn"
)

// ControlledListener wraps a bufconn listener whose lifecycle can be closed and
// reopened independently of the owning process. Tests use it to simulate a
// listener outage while keeping the surrounding Raft transport and store alive.
type ControlledListener struct {
	mu       sync.RWMutex
	bufSize  int
	listener *bufconn.Listener
}

// NewControlledListener creates an open listener with the given buffer size. A
// non-positive size falls back to the cluster buffer size.
func NewControlledListener(bufSize int) *ControlledListener {
	if bufSize <= 0 {
		bufSize = clusterBufSize
	}
	return &ControlledListener{bufSize: bufSize, listener: bufconn.Listen(bufSize)}
}

// Listener returns the current bufconn listener. It may be nil after Close.
func (l *ControlledListener) Listener() *bufconn.Listener {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.listener
}

// DialContext dials the current listener or reports that it is stopped.
func (l *ControlledListener) DialContext(ctx context.Context) (net.Conn, error) {
	listener := l.Listener()
	if listener == nil {
		return nil, context.Canceled
	}
	return listener.DialContext(ctx)
}

// Close closes and clears the current listener. It is idempotent.
func (l *ControlledListener) Close() error {
	l.mu.Lock()
	listener := l.listener
	l.listener = nil
	l.mu.Unlock()
	if listener == nil {
		return nil
	}
	return listener.Close()
}

// Reopen installs a fresh listener, discarding the old one.
func (l *ControlledListener) Reopen() *bufconn.Listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.listener = bufconn.Listen(l.bufSize)
	return l.listener
}
