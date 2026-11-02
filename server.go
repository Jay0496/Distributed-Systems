package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"distributed-kv-store/consensus"
	"distributed-kv-store/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedStorageServiceServer
	store storage.KVStore
	node  *consensus.Node
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// 1. Only the leader can accept writes.
	if s.node.Raft.State() != raft.Leader {
		leaderAddr, _ := s.node.Raft.LeaderWithID()
		return &pb.PutResponse{
			Success: false,
			Message: fmt.Sprintf("Not the leader. Current leader: %s", leaderAddr),
		}, nil
	}

	// 2. Wrap the request in a Raft command.
	cmd := consensus.Command{
		Op:    "put",
		Key:   req.GetKey(),
		Value: req.GetValue(),
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// 3. Replicate the command through Raft and wait for consensus.
	future := s.node.Raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return &pb.PutResponse{Success: false, Message: err.Error()}, nil
	}

	// 4. Check whether the FSM returned an error while applying.
	if applyErr := future.Response(); applyErr != nil {
		return &pb.PutResponse{Success: false, Message: fmt.Sprintf("%v", applyErr)}, nil
	}

	return &pb.PutResponse{Success: true, Message: "Consensus reached and stored successfully"}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// Read directly from the local store. In a fully linearizable read path,
	// the request should first be verified by the leader.
	value, err := s.store.Get(req.GetKey())
	if err != nil {
		return &pb.GetResponse{Success: false}, nil
	}
	return &pb.GetResponse{Success: true, Value: value}, nil
}

func (s *server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Alive: true}, nil
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	raftPort := os.Getenv("RAFT_PORT")
	if raftPort == "" {
		raftPort = "60051"
	}
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node1"
	}
	bootstrap := os.Getenv("BOOTSTRAP") == "true"

	fmt.Printf("Starting %s (gRPC on %s, Raft on %s)...\n", nodeID, port, raftPort)

	// For now the consensus layer uses the MockStore. Once the persistence
	// team lands the SQLite implementation, this can be switched by env var.
	store := storage.NewMockStore()

	// Resolve the advertised Raft address. In Docker Compose this resolves
	// to the container name (e.g. node1:60051).
	advertiseAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", nodeID, raftPort))
	if err != nil {
		log.Fatalf("Failed to resolve raft advertise address: %v", err)
	}

	transport, err := raft.NewTCPTransport(fmt.Sprintf("0.0.0.0:%s", raftPort), advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create raft transport: %v", err)
	}

	var bootstrapConfig raft.Configuration
	if bootstrap {
		bootstrapConfig = raft.Configuration{
			Servers: []raft.Server{
				{ID: raft.ServerID(nodeID), Address: transport.LocalAddr()},
			},
		}
	}

	node, err := consensus.StartRaft(nodeID, store, transport, bootstrapConfig)
	if err != nil {
		log.Fatalf("Failed to start Raft: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStorageServiceServer(grpcServer, &server{store: store, node: node})

	fmt.Printf("gRPC server listening on port %s\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
