package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/rpc"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 1 << 20

// testNode is one Raft node plus the in-process gRPC server that fronts it.
type testNode struct {
	id        string
	raftAddr  raft.ServerAddress
	transport *raft.InmemTransport
	store     *testStore
	node      *consensus.Node
	srv       *Server

	lis       *bufconn.Listener
	grpcSrv   *grpc.Server
	conn      *grpc.ClientConn
	healthSrv    *health.Server
	healthCancel context.CancelFunc

	client pb.StorageServiceClient
	health healthpb.HealthClient

	// requestID records the most recent inbound correlation header so tests can
	// prove forwarding preserves it.
	requestIDMu sync.Mutex
	requestID   string
}

func (tn *testNode) setRequestID(id string) {
	tn.requestIDMu.Lock()
	tn.requestID = id
	tn.requestIDMu.Unlock()
}

func (tn *testNode) lastRequestID() string {
	tn.requestIDMu.Lock()
	defer tn.requestIDMu.Unlock()
	return tn.requestID
}

// testCluster is a fully meshed in-memory Raft cluster whose gRPC front ends
// talk to each other over bufconn.
type testCluster struct {
	nodes []*testNode
	byID  map[string]*testNode
	dial  dialFunc

	mu          sync.RWMutex
	unreachable map[string]bool
}

// SetUnreachable makes the named nodes fail every outbound gRPC dial. Raft is
// unaffected because it uses its own in-memory transport.
func (c *testCluster) SetUnreachable(ids ...string) {
	c.mu.Lock()
	for _, id := range ids {
		c.unreachable[id] = true
	}
	c.mu.Unlock()
	for _, tn := range c.nodes {
		if tn.srv != nil {
			tn.srv.DropConnections()
		}
	}
}

// SetReachable re-enables dialing to the named nodes.
func (c *testCluster) SetReachable(ids ...string) {
	c.mu.Lock()
	for _, id := range ids {
		delete(c.unreachable, id)
	}
	c.mu.Unlock()
	for _, tn := range c.nodes {
		if tn.srv != nil {
			tn.srv.DropConnections()
		}
	}
}

// newTestCluster starts n Raft nodes (node1 bootstraps the full configuration)
// and n gRPC servers. Forwarding between them is wired through bufconn, so no
// real TCP sockets are involved.
func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	c := &testCluster{
		nodes:       make([]*testNode, n),
		byID:        make(map[string]*testNode),
		unreachable: make(map[string]bool, n),
	}

	// 1. Create and fully mesh the Raft transports.
	transports := make([]*raft.InmemTransport, n)
	addrs := make([]raft.ServerAddress, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[i] = trans
		addrs[i] = addr

		tn := &testNode{
			id:        id,
			raftAddr:  addr,
			transport: trans,
			store:     newTestStore(),
			lis:       bufconn.Listen(testBufSize),
		}
		c.nodes[i] = tn
		c.byID[id] = tn
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	// 2. Build the shared peer table: server ID -> gRPC target (the ID itself
	// is the key used by the test dialer below).
	peers := make(map[string]string, n)
	for _, tn := range c.nodes {
		peers[tn.id] = tn.id
	}

	dial := func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		c.mu.RLock()
		down := c.unreachable[target]
		c.mu.RUnlock()
		if down {
			return nil, fmt.Errorf("support: node %s is unreachable", target)
		}
		tn, ok := c.byID[target]
		if !ok {
			return nil, fmt.Errorf("unknown peer %q", target)
		}
		return grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return tn.lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
	}
	c.dial = dial

	// 3. Start Raft.
	servers := make([]raft.Server, 0, n)
	for i := 0; i < n; i++ {
		servers = append(servers, raft.Server{ID: raft.ServerID(c.nodes[i].id), Address: addrs[i]})
	}
	bootstrap := raft.Configuration{Servers: servers}

	for i, tn := range c.nodes {
		conf := raft.Configuration{}
		if i == 0 {
			conf = bootstrap
		}
		node, err := consensus.StartRaft(tn.id, tn.store, tn.transport, t.TempDir(), conf)
		if err != nil {
			t.Fatalf("start raft %s: %v", tn.id, err)
		}
		tn.node = node
	}

	// 4. Start the gRPC front ends.
	for _, tn := range c.nodes {
		cfg := config.Config{
			NodeID:  tn.id,
			DataDir: t.TempDir(),
			Peers:   peers,
		}
		grpcOpts := rpc.RequestIDServerOptions()
		grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(func(
			ctx context.Context,
			req any,
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if values := md.Get(rpc.RequestIDMetadataKey); len(values) > 0 {
					tn.setRequestID(values[0])
				}
			}
			return handler(ctx, req)
		}))
		grpcSrv := grpc.NewServer(grpcOpts...)
		srv := New(cfg, tn.store, tn.node, dial)
		pb.RegisterStorageServiceServer(grpcSrv, srv)
		tn.srv = srv

		hs := health.NewServer()
		hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		hs.SetServingStatus(HealthServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
		healthpb.RegisterHealthServer(grpcSrv, hs)
		srv.SetHealthServer(hs)
		tn.healthSrv = hs
		healthCtx, healthCancel := context.WithCancel(context.Background())
		tn.healthCancel = healthCancel
		srv.StartHealthMonitor(healthCtx)

		go func(srv *grpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(grpcSrv, tn.lis)

		conn, err := dial(context.Background(), tn.id)
		if err != nil {
			t.Fatalf("dial %s: %v", tn.id, err)
		}
		tn.grpcSrv = grpcSrv
		tn.conn = conn
		tn.client = pb.NewStorageServiceClient(conn)
		tn.health = healthpb.NewHealthClient(conn)
	}

	t.Cleanup(func() {
		for _, tn := range c.nodes {
			if tn.healthCancel != nil {
				tn.healthCancel()
			}
			if tn.grpcSrv != nil {
				tn.grpcSrv.Stop()
			}
			if tn.conn != nil {
				_ = tn.conn.Close()
			}
			if tn.node != nil {
				_ = tn.node.Close()
			}
			if tn.transport != nil {
				_ = tn.transport.Close()
			}
			if tn.lis != nil {
				_ = tn.lis.Close()
			}
		}
	})

	return c
}

// waitLeader blocks until exactly one node reports itself leader.
func (c *testCluster) waitLeader(t *testing.T) *testNode {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leader *testNode
		leaders := 0
		for _, tn := range c.nodes {
			if tn.node.Raft.State() == raft.Leader {
				leader = tn
				leaders++
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no single leader elected within timeout")
	return nil
}

// waitFollower returns a node that is currently not the leader.
func (c *testCluster) waitFollower(t *testing.T) *testNode {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, tn := range c.nodes {
			if tn.node.Raft.State() == raft.Follower {
				return tn
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no follower available within timeout")
	return nil
}

func ctxWithTimeout(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}

func mustPut(t *testing.T, client pb.StorageServiceClient, key, value, mode string) {
	t.Helper()

	ctx, cancel := ctxWithTimeout(t, 10*time.Second)
	defer cancel()

	resp, err := client.Put(ctx, &pb.PutRequest{Key: key, Value: []byte(value), Mode: mode})
	if err != nil {
		t.Fatalf("Put(%q) RPC failed: %v", key, err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Put(%q) rejected: %s", key, resp.GetMessage())
	}
}

func waitForValue(t *testing.T, cluster *testCluster, key, want string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var lastMsg string
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range cluster.nodes {
			ctx, cancel := ctxWithTimeout(t, 5*time.Second)
			resp, err := tn.client.Get(ctx, &pb.GetRequest{Key: key})
			cancel()
			if err != nil {
				lastMsg = fmt.Sprintf("%s: RPC error: %v", tn.id, err)
				ok = false
				continue
			}
			if !resp.GetSuccess() || string(resp.GetValue()) != want {
				lastMsg = fmt.Sprintf("%s: success=%v value=%q", tn.id, resp.GetSuccess(), resp.GetValue())
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q=%q: %s", key, want, lastMsg)
}

// waitForMetaVersion polls every node until the committed metadata for key has
// the expected version. Metadata applies asynchronously on followers, so the
// test cannot assume it is visible when the leader's Put returns.
func waitForMetaVersion(t *testing.T, cluster *testCluster, key string, version int64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range cluster.nodes {
			meta, err := tn.store.GetKey(key)
			if err != nil {
				last = fmt.Sprintf("%s: %v", tn.id, err)
				ok = false
				continue
			}
			if meta.Version != version {
				last = fmt.Sprintf("%s: version %d", tn.id, meta.Version)
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for metadata %q version %d: %s", key, version, last)
}

// TestServerConsensusRPCs covers the write path, forwarding, linearizable
// reads, NodeStatus agreement, the health service, and the forwarding loop
// guard against one live 3-node cluster.
func TestServerConsensusRPCs(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	follower := c.waitFollower(t)
	t.Logf("leader=%s follower=%s", leader.id, follower.id)

	t.Run("leader accepts writes", func(t *testing.T) {
		mustPut(t, leader.client, "leader-key", "leader-value", "")
		waitForMetaVersion(t, c, "leader-key", 1)
	})

	t.Run("follower forwards writes", func(t *testing.T) {
		mustPut(t, follower.client, "forwarded-key", "forwarded-value", "replication")
		waitForMetaVersion(t, c, "forwarded-key", 1)
	})

	t.Run("reads forward to the leader and gather fragments", func(t *testing.T) {
		mustPut(t, leader.client, "read-key", "read-value", "rs32")
		waitForMetaVersion(t, c, "read-key", 1)
		waitForValue(t, c, "read-key", "read-value")
	})

	t.Run("missing key is reported as not found", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		_, err := leader.client.Get(ctx, &pb.GetRequest{Key: "does-not-exist"})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("expected NotFound for a missing key, got %v", err)
		}
	})

	t.Run("node status agrees on the leader", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			agree := true
			for _, tn := range c.nodes {
				ctx, cancel := ctxWithTimeout(t, 5*time.Second)
				resp, err := tn.client.NodeStatus(ctx, &pb.NodeStatusRequest{})
				cancel()
				if err != nil {
					t.Fatalf("%s NodeStatus failed: %v", tn.id, err)
				}
				if resp.GetNodeId() != tn.id {
					t.Fatalf("%s reported node_id %q", tn.id, resp.GetNodeId())
				}
				if resp.GetLeaderId() != leader.id {
					agree = false
					break
				}
				if resp.GetCommitIndex() == 0 {
					agree = false
					break
				}
			}
			if agree {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("nodes did not agree on the leader in time")
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	t.Run("node status reports leadership states", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		leaderStatus, err := leader.client.NodeStatus(ctx, &pb.NodeStatusRequest{})
		if err != nil {
			t.Fatalf("leader NodeStatus failed: %v", err)
		}
		if leaderStatus.GetState() != "Leader" {
			t.Fatalf("expected leader state, got %q", leaderStatus.GetState())
		}

		followerStatus, err := follower.client.NodeStatus(ctx, &pb.NodeStatusRequest{})
		if err != nil {
			t.Fatalf("follower NodeStatus failed: %v", err)
		}
		if followerStatus.GetState() != "Follower" {
			t.Fatalf("expected follower state, got %q", followerStatus.GetState())
		}
		if followerStatus.GetLeaderAddr() == "" {
			t.Fatal("follower did not report a leader address")
		}
	})

	t.Run("health check is serving", func(t *testing.T) {
		for _, tn := range c.nodes {
			ctx, cancel := ctxWithTimeout(t, 5*time.Second)
			resp, err := tn.health.Check(ctx, &healthpb.HealthCheckRequest{})
			cancel()
			if err != nil {
				t.Fatalf("%s health check failed: %v", tn.id, err)
			}
			if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				t.Fatalf("%s reported health %s", tn.id, resp.GetStatus())
			}
		}
	})

	t.Run("forwarded loop guard rejects a second hop", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, forwardedMetadataKey, "true")

		_, err := follower.client.Put(ctx, &pb.PutRequest{Key: "loop", Value: []byte("x")})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("expected FailedPrecondition for a second hop, got %v", err)
		}
	})
}

// TestServerJoin verifies that a leader can add a voter and that a follower
// refuses with a leader hint instead of silently doing nothing.
func TestServerJoin(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitLeader(t)
	follower := c.waitFollower(t)

	// The new node participates in Raft transport so AddVoter can reach it.
	newAddr, newTransport := raft.NewInmemTransport("node4")
	t.Cleanup(func() { _ = newTransport.Close() })
	for _, tn := range c.nodes {
		tn.transport.Connect(newAddr, newTransport)
		newTransport.Connect(tn.raftAddr, tn.transport)
	}

	t.Run("follower returns the leader hint", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		resp, err := follower.client.Join(ctx, &pb.JoinRequest{NodeId: "node9", RaftAddr: "node9"})
		if err != nil {
			t.Fatalf("Join RPC failed: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatalf("follower accepted a join: %+v", resp)
		}
		if resp.GetLeaderId() != leader.id {
			t.Fatalf("expected leader hint %q, got %q", leader.id, resp.GetLeaderId())
		}
	})

	t.Run("leader adds a voter", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 10*time.Second)
		defer cancel()

		resp, err := leader.client.Join(ctx, &pb.JoinRequest{NodeId: "node4", RaftAddr: string(newAddr)})
		if err != nil {
			t.Fatalf("Join RPC failed: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("leader rejected join: %s", resp.GetMessage())
		}

		future := leader.node.Raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("GetConfiguration failed: %v", err)
		}
		found := false
		for _, server := range future.Configuration().Servers {
			if server.ID == "node4" && server.Address == newAddr {
				found = true
			}
		}
		if !found {
			t.Fatalf("node4 not present in configuration: %+v", future.Configuration().Servers)
		}
	})

	t.Run("join validates its inputs", func(t *testing.T) {
		ctx, cancel := ctxWithTimeout(t, 5*time.Second)
		defer cancel()

		resp, err := leader.client.Join(ctx, &pb.JoinRequest{NodeId: "node5"})
		if err != nil {
			t.Fatalf("Join RPC failed: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatalf("join without raft_addr was accepted: %+v", resp)
		}
	})
}

// TestServerModeValidation checks that only documented redundancy modes are
// accepted and that invalid modes never reach Raft.
func TestServerModeValidation(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	for _, mode := range []string{"", "replication", "rs32"} {
		ctx, cancel := ctxWithTimeout(t, 10*time.Second)
		resp, err := leader.client.Put(ctx, &pb.PutRequest{Key: "k-" + mode, Value: []byte("v"), Mode: mode})
		cancel()
		if err != nil {
			t.Fatalf("Put(mode=%q) RPC failed: %v", mode, err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("Put(mode=%q) rejected: %s", mode, resp.GetMessage())
		}
	}

	ctx, cancel := ctxWithTimeout(t, 5*time.Second)
	defer cancel()
	resp, err := leader.client.Put(ctx, &pb.PutRequest{Key: "bad", Value: []byte("v"), Mode: "triple"})
	if err != nil {
		t.Fatalf("Put(invalid mode) RPC failed: %v", err)
	}
	if resp.GetSuccess() {
		t.Fatalf("invalid mode was accepted: %+v", resp)
	}
	if resp.GetMessage() == "" {
		t.Fatal("invalid mode rejection did not explain itself")
	}
}
