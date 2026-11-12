package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// TestGetConnDialsTargetsConcurrently proves that a dial to an unreachable
// owner cannot block dials to other owners. The read and write paths dial every
// placement in parallel, so holding connMu during a blocking dial turns one
// dead peer into a timeout for every live peer.
func TestGetConnDialsTargetsConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	dial := func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		started <- target
		select {
		case <-release:
			return nil, errors.New("test dial closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	srv := &Server{
		conns: make(map[string]*grpc.ClientConn),
		dial:  dial,
	}

	targets := []string{"node2", "node3"}
	errCh := make(chan error, len(targets))
	for _, target := range targets {
		target := target
		go func() {
			_, err := srv.getConn(context.Background(), target)
			errCh <- err
		}()
	}

	for range targets {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("dials to different targets were serialized")
		}
	}
	close(release)

	for range targets {
		if err := <-errCh; err == nil {
			t.Fatal("expected the test dialer to return an error")
		}
	}
}
