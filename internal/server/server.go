// Package server implements the gRPC front end for a storage node. It serves
// the StorageService API, forwards client requests to the current Raft leader,
// and exposes the fragment transport used by the redundancy layer.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

// Server is the gRPC front end for one storage node. It holds the local store,
// the Raft node that owns consensus, and a cache of connections to peers used
// for forwarding.
type Server struct {
	pb.UnimplementedStorageServiceServer
	store storage.Store
	node  *consensus.Node
	cfg   config.Config

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
	dial   dialFunc
}

// New builds a Server with its connection cache and dialer.
// A nil dial uses the production gRPC dialer.
func New(cfg config.Config, store storage.Store, node *consensus.Node, dial dialFunc) *Server {
	if dial == nil {
		dial = newDialFunc(cfg)
	}
	return &Server{
		store: store,
		node:  node,
		cfg:   cfg,
		conns: make(map[string]*grpc.ClientConn),
		dial:  dial,
	}
}

// Put validates the redundancy mode, forwards to the leader when necessary, and
// commits the write through Raft.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if _, err := redundancy.ParseMode(req.GetMode()); err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// Followers transparently forward writes to the current leader.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardPut(ctx, req)
	}

	// Wrap the request in a Raft command. The full value still travels through
	// the log until task 06 installs the metadata-only write path.
	// TODO(task 06): encode the value, push fragments, and commit only metadata.
	cmd := consensus.Command{
		Op: consensus.OpPut,
		Legacy: &consensus.LegacyOp{
			Key:   req.GetKey(),
			Value: req.GetValue(),
		},
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// Replicate the command through Raft and wait for consensus.
	future := s.node.Raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// Check whether the FSM returned an error while applying.
	if applyErr := future.Response(); applyErr != nil {
		return &pb.PutResponse{Success: false, Message: fmt.Sprintf("%v", applyErr)}, nil
	}

	return &pb.PutResponse{Success: true, Message: "Consensus reached and stored successfully"}, nil
}

// Get forwards to the leader when necessary and serves a linearizable read from
// the committed Raft log.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Followers transparently forward reads to the current leader.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardGet(ctx, req)
	}

	// Reads go through the Raft log so they observe all previously committed
	// writes (linearizable reads).
	// TODO(task 06): read metadata through Raft and gather fragments locally.
	cmd := consensus.Command{Op: consensus.OpGet, Key: req.GetKey()}
	data, err := json.Marshal(cmd)
	if err != nil {
		return &pb.GetResponse{Success: false}, nil
	}

	future := s.node.Raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.GetResponse{Success: false}, nil
	}

	result, ok := future.Response().(*consensus.GetResult)
	if !ok || !result.Found || result.Err != "" {
		return &pb.GetResponse{Success: false}, nil
	}
	return &pb.GetResponse{Success: true, Value: result.Value}, nil
}

// Heartbeat is the legacy liveness probe kept for compatibility. Real health
// checks use grpc.health.v1.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Alive: true}, nil
}

// Join asks the current leader to add a new voter to the Raft cluster.
func (s *Server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if req.GetNodeId() == "" || req.GetRaftAddr() == "" {
		return &pb.JoinResponse{Success: false, Message: "node_id and raft_addr are required"}, nil
	}

	if err := s.node.AddVoter(req.GetNodeId(), req.GetRaftAddr()); err != nil {
		_, leaderID := s.node.Raft.LeaderWithID()
		return &pb.JoinResponse{
			Success:  false,
			Message:  err.Error(),
			LeaderId: string(leaderID),
		}, nil
	}

	return &pb.JoinResponse{
		Success:  true,
		Message:  "voter added to cluster",
		LeaderId: s.cfg.NodeID,
	}, nil
}

// NodeStatus reports the local node's Raft state for diagnostics and tests.
func (s *Server) NodeStatus(ctx context.Context, req *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
	stats := s.node.Raft.Stats()
	leaderAddr, leaderID := s.node.Raft.LeaderWithID()

	parse := func(key string) uint64 {
		v, _ := strconv.ParseUint(stats[key], 10, 64)
		return v
	}

	return &pb.NodeStatusResponse{
		NodeId:       s.cfg.NodeID,
		State:        s.node.Raft.State().String(),
		LeaderId:     string(leaderID),
		LeaderAddr:   string(leaderAddr),
		Term:         parse("term"),
		CommitIndex:  parse("commit_index"),
		AppliedIndex: s.node.Raft.AppliedIndex(),
		LastIndex:    s.node.Raft.LastIndex(),
	}, nil
}
