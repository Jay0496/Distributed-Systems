package server

import (
	"context"
	"fmt"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/rpc"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const forwardedMetadataKey = "x-kv-forwarded"

// dialFunc abstracts gRPC dialing so tests can inject in-memory transports.
type dialFunc func(ctx context.Context, target string) (*grpc.ClientConn, error)

// newDialFunc returns the production dialer. TLS is used when configured, and
// the shared rpc helper attaches AUTH_TOKEN and raises the gRPC message limit
// for every internal client.
func newDialFunc(cfg config.Config) dialFunc {
	return dialFuncFor(cfg, func(ctx context.Context, target string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, target, opts...)
	})
}

// dialFuncFor builds the shared internal dialer around a transport-specific
// dial call. Tests inject a bufconn dialer here while exercising the same
// AUTH_TOKEN and MAX_MESSAGE_SIZE options as production.
func dialFuncFor(cfg config.Config, dialContext func(context.Context, string, []grpc.DialOption) (*grpc.ClientConn, error)) dialFunc {
	shared, sharedErr := rpc.DialOptions(cfg.AuthToken, cfg.MaxMessageSize)
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		if sharedErr != nil {
			return nil, sharedErr
		}
		opts := append([]grpc.DialOption(nil), shared...)
		if cfg.TLSEnabled {
			creds, err := credentials.NewClientTLSFromFile(cfg.TLSCA, "")
			if err != nil {
				return nil, fmt.Errorf("load TLS CA: %w", err)
			}
			opts = append(opts, grpc.WithTransportCredentials(creds))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		opts = append(opts, grpc.WithBlock())
		return dialContext(ctx, target, opts)
	}
}

// getConn returns a cached connection to target, dialing if necessary. The
// cache lock is not held while dialing: a slow or unreachable peer must not
// block dials to other peers. Concurrent first calls may dial the same target;
// the loser's connection is closed and the cached winner is returned.
func (s *Server) getConn(ctx context.Context, target string) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	if conn, ok := s.conns[target]; ok {
		s.connMu.Unlock()
		return conn, nil
	}
	s.connMu.Unlock()

	conn, err := s.dial(ctx, target)
	if err != nil {
		return nil, err
	}

	s.connMu.Lock()
	defer s.connMu.Unlock()
	if existing, ok := s.conns[target]; ok {
		_ = conn.Close()
		return existing, nil
	}
	s.conns[target] = conn
	return conn, nil
}

// DropConnections closes and forgets every cached peer connection. Tests use
// it when simulating a partition so a previously cached connection cannot
// bypass the injected dial failure.
func (s *Server) DropConnections() {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	for target, conn := range s.conns {
		_ = conn.Close()
		delete(s.conns, target)
	}
}

// dropConn closes and forgets a cached connection so the next attempt redials.
func (s *Server) dropConn(target string) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if conn, ok := s.conns[target]; ok {
		_ = conn.Close()
		delete(s.conns, target)
	}
}

// leaderGRPCAddr resolves the current leader's gRPC address via the peer map.
func (s *Server) leaderGRPCAddr() (string, bool) {
	_, leaderID := s.node.Raft.LeaderWithID()
	if leaderID == "" {
		return "", false
	}
	target, ok := s.cfg.Peers[string(leaderID)]
	return target, ok
}

// forwardContext derives one forwarding attempt from the incoming request. It
// preserves cancellation and the request ID, and caps the attempt at the
// configured forward limit so a slow leader cannot outlive the client deadline.
func (s *Server) forwardContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// A write forwarded to the leader must be allowed the full write budget:
	// a degraded write waits for every shard upload attempt, including dead
	// owners, before it commits.
	timeout := s.cfg.WriteTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// alreadyForwarded reports whether this request already made one forwarding hop.
func alreadyForwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	return len(md.Get(forwardedMetadataKey)) > 0
}

// forwardPut forwards a write to the leader. It retries once on Unavailable.
func (s *Server) forwardPut(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if alreadyForwarded(ctx) {
		return nil, status.Error(codes.FailedPrecondition, "request was forwarded but this node is not the leader")
	}

	target, ok := s.leaderGRPCAddr()
	if !ok {
		return &pb.PutResponse{Success: false, Message: "not the leader and no leader is known"}, nil
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := s.getConn(ctx, target)
		if err != nil {
			lastErr = err
			continue
		}

		fctx, cancel := s.forwardContext(ctx)
		fctx = rpc.AppendRequestID(metadata.AppendToOutgoingContext(fctx, forwardedMetadataKey, "true"))
		resp, err := pb.NewStorageServiceClient(conn).Put(fctx, req)
		cancel()

		if err == nil {
			return resp, nil
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable {
			break
		}
		s.dropConn(target)
	}

	return &pb.PutResponse{Success: false, Message: fmt.Sprintf("forward to leader %s failed: %v", target, lastErr)}, nil
}

// forwardToLeader performs one admin RPC against the current leader. It keeps
// the same forwarding guard and retry policy as Put/Get: a request that
// already made one hop is rejected, and only an unavailable transport is
// retried. The callback writes its response into a caller-owned variable.
func (s *Server) forwardToLeader(ctx context.Context, call func(context.Context, pb.StorageServiceClient) error) error {
	if alreadyForwarded(ctx) {
		return status.Error(codes.FailedPrecondition, "request was forwarded but this node is not the leader")
	}

	target, ok := s.leaderGRPCAddr()
	if !ok {
		return status.Error(codes.Unavailable, "not the leader and no leader is known")
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := s.getConn(ctx, target)
		if err != nil {
			lastErr = err
			continue
		}

		fctx, cancel := s.forwardContext(ctx)
		fctx = rpc.AppendRequestID(metadata.AppendToOutgoingContext(fctx, forwardedMetadataKey, "true"))
		err = call(fctx, pb.NewStorageServiceClient(conn))
		cancel()

		if err == nil {
			return nil
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable {
			return err
		}
		s.dropConn(target)
	}

	return status.Errorf(codes.Unavailable, "forward admin request to leader %s failed: %v", target, lastErr)
}

// forwardGet forwards a read to the leader. It retries once on Unavailable.
func (s *Server) forwardGet(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if alreadyForwarded(ctx) {
		return nil, status.Error(codes.FailedPrecondition, "request was forwarded but this node is not the leader")
	}

	target, ok := s.leaderGRPCAddr()
	if !ok {
		return nil, status.Error(codes.Unavailable, "not the leader and no leader is known")
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := s.getConn(ctx, target)
		if err != nil {
			lastErr = err
			continue
		}

		fctx, cancel := s.forwardContext(ctx)
		fctx = rpc.AppendRequestID(metadata.AppendToOutgoingContext(fctx, forwardedMetadataKey, "true"))
		resp, err := pb.NewStorageServiceClient(conn).Get(fctx, req)
		cancel()

		if err == nil {
			return resp, nil
		}
		lastErr = err
		// Application and integrity statuses from the leader are the answer;
		// only an unavailable transport is retried.
		if status.Code(err) != codes.Unavailable {
			return nil, err
		}
		s.dropConn(target)
	}

	return nil, status.Errorf(codes.Unavailable, "forward read to leader %s failed: %v", target, lastErr)
}
