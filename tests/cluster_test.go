//go:build integration

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

// clusterNodes are the gRPC endpoints published by docker-compose.yml. Run the
// integration tests inside the compose network so these names resolve:
//
//	docker run --rm --network distributed-systems_cluster-net \
//	  -v ${PWD}:/app -w /app golang:1.22 \
//	  go test -tags=integration ./tests/...
var clusterNodes = []string{
	"node1:50051",
	"node2:50052",
	"node3:50053",
	"node4:50054",
	"node5:50055",
}

type clusterClient struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.StorageServiceClient
}

// dialCluster connects to every node in the compose cluster.
func dialCluster(t *testing.T) []clusterClient {
	t.Helper()

	clients := make([]clusterClient, 0, len(clusterNodes))
	for _, addr := range clusterNodes {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err != nil {
			t.Fatalf("dial %s: %v (is `docker compose up -d` running?)", addr, err)
		}
		clients = append(clients, clusterClient{
			addr:   addr,
			conn:   conn,
			client: pb.NewStorageServiceClient(conn),
		})
	}

	t.Cleanup(func() {
		for _, c := range clients {
			_ = c.conn.Close()
		}
	})

	return clients
}

func nodeStatus(t *testing.T, c clusterClient) *pb.NodeStatusResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status, err := c.client.NodeStatus(ctx, &pb.NodeStatusRequest{})
	if err != nil {
		t.Fatalf("NodeStatus on %s: %v", c.addr, err)
	}
	return status
}

// TestClusterHeartbeat verifies that every node answers the legacy heartbeat
// RPC.
func TestClusterHeartbeat(t *testing.T) {
	for _, c := range dialCluster(t) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := c.client.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "integration-test"})
		cancel()
		if err != nil {
			t.Fatalf("Heartbeat on %s: %v", c.addr, err)
		}
		if !resp.GetAlive() {
			t.Errorf("%s reported alive=false", c.addr)
		}
	}
}

// TestClusterLeaderAgreement checks that all five nodes converge on exactly
// one leader and report the same leader ID.
func TestClusterLeaderAgreement(t *testing.T) {
	clients := dialCluster(t)

	deadline := time.Now().Add(30 * time.Second)
	var last []*pb.NodeStatusResponse
	for time.Now().Before(deadline) {
		last = last[:0]
		leaderIDs := make(map[string]struct{})
		leaderStates := 0
		agreed := true

		for _, c := range clients {
			status := nodeStatus(t, c)
			last = append(last, status)
			if status.GetLeaderId() == "" {
				agreed = false
				continue
			}
			leaderIDs[status.GetLeaderId()] = struct{}{}
			if status.GetState() == "Leader" {
				leaderStates++
			}
		}

		if agreed && len(leaderIDs) == 1 && leaderStates == 1 {
			var leaderID string
			for id := range leaderIDs {
				leaderID = id
			}
			for _, status := range last {
				if status.GetLeaderId() != leaderID {
					t.Fatalf("%s reports leader %q, expected %q",
						status.GetNodeId(), status.GetLeaderId(), leaderID)
				}
			}
			t.Logf("all %d nodes agree on leader %s (term %d)",
				len(last), leaderID, last[0].GetTerm())
			return
		}

		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("nodes did not converge on one leader: %+v", last)
}

// TestClusterWriteThroughFollower writes through a non-leader and then reads
// the value back from every node.
func TestClusterWriteThroughFollower(t *testing.T) {
	clients := dialCluster(t)

	// Wait for leader agreement so a follower is guaranteed to exist.
	deadline := time.Now().Add(30 * time.Second)
	var follower *clusterClient
	for time.Now().Before(deadline) {
		for i := range clients {
			status := nodeStatus(t, clients[i])
			if status.GetLeaderId() != "" && status.GetState() != "Leader" {
				follower = &clients[i]
				break
			}
		}
		if follower != nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if follower == nil {
		t.Fatal("no follower available")
	}

	key := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	value := []byte("written-through-follower")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp, err := follower.client.Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: value,
		Mode:  "replication",
	})
	cancel()
	if err != nil {
		t.Fatalf("Put via follower %s: %v", follower.addr, err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Put via follower %s rejected: %s", follower.addr, resp.GetMessage())
	}
	t.Logf("wrote %q through follower %s", key, follower.addr)

	for _, c := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		got, err := c.client.Get(ctx, &pb.GetRequest{Key: key})
		cancel()
		if err != nil {
			t.Fatalf("Get on %s: %v", c.addr, err)
		}
		if !got.GetSuccess() || string(got.GetValue()) != string(value) {
			t.Fatalf("Get on %s = success=%v value=%q, want %q",
				c.addr, got.GetSuccess(), got.GetValue(), value)
		}
	}
	t.Logf("all %d nodes returned the committed value", len(clients))
}
