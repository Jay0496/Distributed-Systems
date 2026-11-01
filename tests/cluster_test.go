package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClusterHeartbeat(t *testing.T) {
	// Since docker compose runs nodes on ports 50051-50055, 
	// we test against node1 mapped to localhost:50051 during local integration tests.
	target := "node1:50051"
	
	// Set up a context with a timeout so the test doesn't hang if the server is down
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Connect to the gRPC server using insecure credentials (standard for local docker cluster)
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("Failed to connect to cluster node at %s: %v. (Make sure 'docker compose up' is running!)", target, err)
	}
	defer conn.Close()

	client := pb.NewStorageServiceClient(conn)

	// Send a Heartbeat RPC request
	req := &pb.HeartbeatRequest{NodeId: "test-client"}
	resp, err := client.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat RPC failed: %v", err)
	}

	// Verify that the node responded and is alive
	if !resp.GetAlive() {
		t.Errorf("Expected node to be alive, but got response: %+v", resp)
	}

	fmt.Println("Successfully verified node heartbeat via gRPC!")
}