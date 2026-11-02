package main

import (
	"context"
	"fmt"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	forwardedMetadataKey = "x-kv-forwarded"
	forwardTimeout       = 5 * time.Second
)

// dialFunc abstracts gRPC dialing so tests can inject in-memory transports.
type dialFunc func(ctx context.Context, target string) (*grpc.ClientConn, error)

// newDialFunc returns the production dialer. TLS is used when configured.
func newDialFunc(cfg Config) dialFunc {
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		var opts []grpc.DialOption
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
		return grpc.DialContext(ctx, target, opts...)
	}
}

// getConn returns a cached connection to target, dialing if necessary.
func (s *server) getConn(ctx context.Context, target string) (*grpc.ClientConn, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if conn, ok := s.conns[target]; ok {
		return conn, nil
	}
	conn, err := s.dial(ctx, target)
	if err != nil {
		return nil, err
	}
	s.conns[target] = conn
	return conn, nil
}

// dropConn closes and forgets a cached connection so the next attempt redials.
func (s *server) dropConn(target string) {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	if conn, ok := s.conns[target]; ok {
		_ = conn.Close()
		delete(s.conns, target)
	}
}

// leaderGRPCAddr resolves the current leader's gRPC address via the peer map.
func (s *server) leaderGRPCAddr() (string, bool) {
	_, leaderID := s.node.Raft.LeaderWithID()
	if leaderID == "" {
		return "", false
	}
	target, ok := s.cfg.Peers[string(leaderID)]
	return target, ok
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
func (s *server) forwardPut(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
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

		fctx, cancel := context.WithTimeout(context.Background(), forwardTimeout)
		fctx = metadata.AppendToOutgoingContext(fctx, forwardedMetadataKey, "true")
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

// forwardGet forwards a read to the leader. It retries once on Unavailable.
func (s *server) forwardGet(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if alreadyForwarded(ctx) {
		return nil, status.Error(codes.FailedPrecondition, "request was forwarded but this node is not the leader")
	}

	target, ok := s.leaderGRPCAddr()
	if !ok {
		return &pb.GetResponse{Success: false}, nil
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := s.getConn(ctx, target)
		if err != nil {
			lastErr = err
			continue
		}

		fctx, cancel := context.WithTimeout(context.Background(), forwardTimeout)
		fctx = metadata.AppendToOutgoingContext(fctx, forwardedMetadataKey, "true")
		resp, err := pb.NewStorageServiceClient(conn).Get(fctx, req)
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

	_ = lastErr
	return &pb.GetResponse{Success: false}, nil
}
