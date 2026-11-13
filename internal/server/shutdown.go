package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

// ShutdownOptions names the process-owned resources in the order they must be
// released. Server is required; the closers are optional so a component test
// can exercise a subset.
type ShutdownOptions struct {
	Server        *Server
	GRPC          *grpc.Server
	Store         io.Closer
	Raft          io.Closer
	Transport     io.Closer
	CancelProcess context.CancelFunc
	Timeout       time.Duration
	Logger        *slog.Logger
}

// Shutdown performs the documented graceful-shutdown order:
//
//  1. mark readiness NOT_SERVING;
//  2. drain gRPC with a bounded GracefulStop, then force-stop;
//  3. cancel the process context and stop detector/repair workers;
//  4. close cached peer connections and background workers;
//  5. close Raft;
//  6. close SQLite;
//  7. close the Raft transport.
//
// Store.Close is idempotent, so a repeated signal is safe.
func Shutdown(opts ShutdownOptions) error {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	if opts.Server != nil {
		opts.Server.MarkShuttingDown()
	}

	if opts.GRPC != nil {
		if err := shutdownGRPC(opts.GRPC, timeout); err != nil {
			logger.Warn("gRPC drain timed out; forced stop", "error", err)
		}
	}

	if opts.CancelProcess != nil {
		opts.CancelProcess()
	}
	if opts.Server != nil {
		_ = opts.Server.Close()
	}

	var firstErr error
	closeStep := func(name string, closer io.Closer) {
		if closer == nil {
			return
		}
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", name, err)
			logger.Error("shutdown step failed", "step", name, "error", err)
		}
	}

	closeStep("raft", opts.Raft)
	closeStep("store", opts.Store)
	closeStep("transport", opts.Transport)

	logger.Info("shutdown complete")
	return firstErr
}

// shutdownGRPC drains in-flight RPCs for at most timeout, then force-stops.
func shutdownGRPC(server *grpc.Server, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		server.Stop()
		<-done
		return context.DeadlineExceeded
	}
}
