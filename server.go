package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"distributed-kv-store/consensus"
	"distributed-kv-store/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type server struct {
	pb.UnimplementedStorageServiceServer
	store storage.KVStore
	node  *consensus.Node
	cfg   Config
	redun RedundancyEngine

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
	dial   dialFunc
}

// newServer builds a server with its connection cache and redundancy hook.
// A nil dial uses the production gRPC dialer.
func newServer(cfg Config, store storage.KVStore, node *consensus.Node, dial dialFunc) *server {
	if dial == nil {
		dial = newDialFunc(cfg)
	}
	return &server{
		store: store,
		node:  node,
		cfg:   cfg,
		redun: noopRedundancy{},
		conns: make(map[string]*grpc.ClientConn),
		dial:  dial,
	}
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validateMode(req.GetMode()); err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// Followers transparently forward writes to the current leader.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardPut(ctx, req)
	}

	// Wrap the request in a Raft command.
	cmd := consensus.Command{
		Op:    "put",
		Mode:  req.GetMode(),
		Key:   req.GetKey(),
		Value: req.GetValue(),
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

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Followers transparently forward reads to the current leader.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardGet(ctx, req)
	}

	// Reads go through the Raft log so they observe all previously committed
	// writes (linearizable reads).
	cmd := consensus.Command{Op: "get", Key: req.GetKey()}
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

func (s *server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Alive: true}, nil
}

// Join asks the current leader to add a new voter to the Raft cluster.
func (s *server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
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
func (s *server) NodeStatus(ctx context.Context, req *pb.NodeStatusRequest) (*pb.NodeStatusResponse, error) {
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

func main() {
	cfg := loadConfig()

	fmt.Printf("Starting %s (gRPC on %s, Raft on %s, data dir %s)...\n", cfg.NodeID, cfg.GRPCPort, cfg.RaftPort, cfg.DataDir)

	// For now the consensus layer uses the MockStore. Once the persistence
	// team lands the SQLite implementation, this can be switched by env var.
	store := storage.NewMockStore()

	// Resolve the advertised Raft address. In Docker Compose this resolves
	// to the container name (e.g. node1:60051).
	advertiseAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", cfg.NodeID, cfg.RaftPort))
	if err != nil {
		log.Fatalf("Failed to resolve raft advertise address: %v", err)
	}

	transport, err := raft.NewTCPTransport(fmt.Sprintf("0.0.0.0:%s", cfg.RaftPort), advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create raft transport: %v", err)
	}

	bootstrapConfig := raft.Configuration{}
	if cfg.Bootstrap {
		ids := make([]string, 0, len(cfg.BootstrapServers))
		for id := range cfg.BootstrapServers {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		servers := make([]raft.Server, 0, len(ids))
		for _, id := range ids {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(cfg.BootstrapServers[id]),
			})
		}
		bootstrapConfig = raft.Configuration{Servers: servers}
	}

	node, err := consensus.StartRaft(cfg.NodeID, store, transport, cfg.DataDir, bootstrapConfig)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	var grpcOpts []grpc.ServerOption
	if cfg.TLSEnabled {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			log.Fatalf("Failed to load TLS credentials: %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	pb.RegisterStorageServiceServer(grpcServer, newServer(cfg, store, node, nil))

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("storage.StorageService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Optionally join an existing cluster through a live leader.
	joinCluster(cfg, string(transport.LocalAddr()))

	// Shut down cleanly on SIGINT/SIGTERM so the BoltDB file lock is released
	// (important for fast container restarts) and in-flight RPCs can drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("received shutdown signal, stopping %s", cfg.NodeID)
		grpcServer.GracefulStop()
		_ = node.Close()
		_ = transport.Close()
	}()

	fmt.Printf("gRPC server listening on port %s\n", cfg.GRPCPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// joinCluster asks the node configured in JOIN_ADDR to add this node as a
// voter. It retries so that startup ordering between containers does not
// matter.
func joinCluster(cfg Config, raftAddr string) {
	if cfg.JoinAddr == "" {
		return
	}

	go func() {
		for attempt := 1; attempt <= 10; attempt++ {
			conn, err := grpc.Dial(cfg.JoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				client := pb.NewStorageServiceClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := client.Join(ctx, &pb.JoinRequest{
					NodeId:   cfg.NodeID,
					RaftAddr: raftAddr,
					GrpcAddr: fmt.Sprintf("%s:%s", cfg.NodeID, cfg.GRPCPort),
				})
				cancel()
				conn.Close()

				if err == nil && resp.GetSuccess() {
					log.Printf("joined cluster via %s", cfg.JoinAddr)
					return
				}
				if err != nil {
					log.Printf("join attempt %d via %s failed: %v", attempt, cfg.JoinAddr, err)
				} else {
					log.Printf("join attempt %d via %s rejected: %s", attempt, cfg.JoinAddr, resp.GetMessage())
				}
			} else {
				log.Printf("join attempt %d via %s dial failed: %v", attempt, cfg.JoinAddr, err)
			}
			time.Sleep(2 * time.Second)
		}
		log.Printf("gave up joining cluster via %s after 10 attempts", cfg.JoinAddr)
	}()
}
