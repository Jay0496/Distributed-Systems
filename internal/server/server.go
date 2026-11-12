package server

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the gRPC front end for one storage node. It holds the local store,
// the Raft node that owns consensus, and a cache of connections to peers used
// for forwarding.
type Server struct {
	pb.UnimplementedStorageServiceServer
	store storage.Store
	node  *consensus.Node
	cfg   config.Config

	connMu  sync.Mutex
	conns   map[string]*grpc.ClientConn
	dial    dialFunc
	staging *fragmentStaging

	// startedAt is the process start time used by Stats uptime reporting.
	startedAt time.Time

	// apply is the Raft Apply seam used by the write coordinator. Tests
	// replace it to force definite and ambiguous commit outcomes.
	apply applyFunc

	// placer maps stripe shards to deterministic owners. A nil placer uses the
	// default deterministic hash placer.
	placer redundancy.Placer

	// readMu guards readRand, which shuffles RS owner fetch order. Tests may
	// assign a fixed-seed source before issuing reads.
	readMu   sync.Mutex
	readRand *rand.Rand

	// repairQueue runs asynchronous read repair. It is bounded and never
	// blocks a client read; task 09 replaces it with the shared repair queue.
	repairQueue cluster.RepairQueue
}

// New builds a Server with its connection cache and dialer.
// A nil dial uses the production gRPC dialer.
func New(cfg config.Config, store storage.Store, node *consensus.Node, dial dialFunc) *Server {
	cfg = cfg.WithDefaults()
	if dial == nil {
		dial = newDialFunc(cfg)
	}
	srv := &Server{
		store:     store,
		node:      node,
		cfg:       cfg,
		conns:     make(map[string]*grpc.ClientConn),
		dial:      dial,
		staging:   newFragmentStaging(),
		placer:    redundancy.NewPlacer(),
		startedAt: time.Now(),
	}
	// The method value is bound after construction so Server can own the
	// default apply implementation without a package-level helper.
	srv.apply = srv.applyRaft
	srv.repairQueue = cluster.NewRepairQueue(
		cfg.ReadRepairWorkers,
		readRepairQueueCapacity,
		cfg.WriteTimeout,
		srv.performReadRepair,
	)
	return srv
}

// Close stops background repair workers. It is safe to call more than once.
func (s *Server) Close() error {
	if s.repairQueue != nil {
		s.repairQueue.Close()
	}
	return nil
}

// Put validates the request, forwards to the leader when necessary, and runs
// the metadata-only write path on the leader. Application failures (validation,
// quorum, conflict, commit rejection, ambiguous commit) return success=false
// with a message; gRPC errors are reserved for transport-level failures.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validatePut(req.GetKey(), req.GetValue(), req.GetMode(), req.GetExpectedVersion(), s.cfg.MaxValueSize); err != nil {
		return writeFailed("%v", err), nil
	}

	if s.node == nil || s.node.Raft == nil {
		return writeFailed("raft node is not configured"), nil
	}

	// Followers transparently forward writes to the current leader.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardPut(ctx, req)
	}

	return s.write(ctx, req)
}

// Get forwards to the leader when necessary. The leader reads metadata through
// Raft and then gathers, verifies, and decodes fragments in read.go.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.node == nil || s.node.Raft == nil {
		return nil, status.Error(codes.Internal, "raft node is not configured")
	}

	// Followers transparently forward reads to the current leader.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardGet(ctx, req)
	}
	return s.read(ctx, req)
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
