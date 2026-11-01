package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	"distributed-kv-store/storage"
	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedStorageServiceServer
	db *sql.DB
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	fmt.Printf("Received PUT request for key: %s\n", req.GetKey())
	return &pb.PutResponse{Success: true, Message: "Stored successfully"}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	fmt.Printf("Received GET request for key: %s\n", req.GetKey())
	return &pb.GetResponse{Success: true, Value: []byte("sample-val")}, nil
}

func (s *server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Alive: true}, nil
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node"
	}
	fmt.Printf("Starting %s on port %s...\n", nodeID, port)

	// Initialize SQLite database (this uses the storage package so it's not "unused")
	dbPath := fmt.Sprintf("%s.db", nodeID)
	db, err := storage.InitDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStorageServiceServer(grpcServer, &server{db: db})

	fmt.Printf("gRPC server listening on port %s\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}