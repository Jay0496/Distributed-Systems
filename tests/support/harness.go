package support

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const clusterBufSize = 8 << 20

// Cluster is a five-node in-process deployment: real Raft over in-memory
// transports, real SQLite stores, and gRPC front ends on controllable bufconn
// listeners. Tests can make individual nodes unreachable without stopping Raft.
type Cluster struct {
	Nodes []*Node
	Peers map[string]string

	byID map[string]*Node
	dial func(context.Context, string) (*grpc.ClientConn, error)

	mu          sync.RWMutex
	unreachable map[string]bool

	closeOnce sync.Once
}

// Node is one member of a Cluster.
type Node struct {
	ID      string
	Store   *storage.SQLiteStore
	Raft    *consensus.Node
	Server  *server.Server
	Client  pb.StorageServiceClient
	Dir     string
	Addr    raft.ServerAddress
	Conn    *grpc.ClientConn
	lis     *bufconn.Listener
	grpcSrv *grpc.Server

	storePath string
	transport *raft.InmemTransport
}

// StartCluster starts count in-process nodes and waits for a leader. Every node
// uses its own SQLite file under a temporary directory. Cleanup is registered
// with t.
func StartCluster(t testing.TB, count int) *Cluster {
	t.Helper()

	c := &Cluster{
		Nodes:       make([]*Node, count),
		Peers:       make(map[string]string, count),
		byID:        make(map[string]*Node, count),
		unreachable: make(map[string]bool, count),
	}

	transports := make([]*raft.InmemTransport, count)
	addrs := make([]raft.ServerAddress, count)
	servers := make([]raft.Server, 0, count)

	for i := 0; i < count; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr, transport := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[i] = transport
		addrs[i] = addr
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: addr})

		dir := t.TempDir()
		n := &Node{ID: id, Addr: addr, Dir: dir, storePath: filepath.Join(dir, "node.db"), transport: transport}
		n.lis = bufconn.Listen(clusterBufSize)
		c.Nodes[i] = n
		c.byID[id] = n
		c.Peers[id] = id
	}

	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	dial := func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		c.mu.RLock()
		down := c.unreachable[target]
		n, ok := c.byID[target]
		c.mu.RUnlock()
		if down {
			return nil, fmt.Errorf("support: node %s is unreachable", target)
		}
		if !ok {
			return nil, fmt.Errorf("support: unknown peer %q", target)
		}
		return grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return n.lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
	}

	c.dial = dial

	bootstrap := raft.Configuration{Servers: servers}
	for i, n := range c.Nodes {
		store, err := storage.NewSQLiteStore(n.storePath)
		if err != nil {
			t.Fatalf("support: open store for %s: %v", n.ID, err)
		}
		n.Store = store

		cfg := raft.Configuration{}
		if i == 0 {
			cfg = bootstrap
		}
		node, err := consensus.StartRaft(n.ID, store, transports[i], n.Dir, cfg)
		if err != nil {
			t.Fatalf("support: start raft for %s: %v", n.ID, err)
		}
		n.Raft = node
	}

	for _, n := range c.Nodes {
		cfg := config.Config{
			NodeID:  n.ID,
			DataDir: n.Dir,
			Peers:   c.Peers,
		}.WithDefaults()

		n.Server = server.New(cfg, n.Store, n.Raft, dial)
		n.grpcSrv = grpc.NewServer(
			grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
			grpc.MaxSendMsgSize(cfg.MaxMessageSize),
		)
		pb.RegisterStorageServiceServer(n.grpcSrv, n.Server)

		go func(srv *grpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(n.grpcSrv, n.lis)

		conn, err := dial(context.Background(), n.ID)
		if err != nil {
			t.Fatalf("support: dial %s: %v", n.ID, err)
		}
		n.Conn = conn
		n.Client = pb.NewStorageServiceClient(conn)
	}

	t.Cleanup(func() {
		c.mu.Lock()
		for id := range c.byID {
			c.unreachable[id] = true
		}
		c.mu.Unlock()

		for _, n := range c.Nodes {
			if n.grpcSrv != nil {
				n.grpcSrv.Stop()
			}
			if n.Server != nil {
				_ = n.Server.Close()
			}
			if n.Conn != nil {
				_ = n.Conn.Close()
			}
			if n.Raft != nil {
				_ = n.Raft.Close()
			}
			if n.Store != nil {
				_ = n.Store.Close()
			}
			if n.lis != nil {
				_ = n.lis.Close()
			}
		}
		for _, transport := range transports {
			_ = transport.Close()
		}
	})

	if _, err := c.WaitLeader(30 * time.Second); err != nil {
		t.Fatalf("support: %v", err)
	}
	return c
}

// SetUnreachable makes the named nodes fail every outbound dial, so fragments
// cannot be uploaded to them. Raft is unaffected because it uses an independent
// in-memory transport.
func (c *Cluster) SetUnreachable(ids ...string) {
	c.mu.Lock()
	for _, id := range ids {
		c.unreachable[id] = true
	}
	c.mu.Unlock()

	// Drop cached dials so a connection opened before the partition cannot
	// bypass the injected failure.
	for _, n := range c.Nodes {
		if n.Server != nil {
			n.Server.DropConnections()
		}
	}
}

// SetReachable re-enables dialing to the named nodes.
func (c *Cluster) SetReachable(ids ...string) {
	c.mu.Lock()
	for _, id := range ids {
		delete(c.unreachable, id)
	}
	c.mu.Unlock()

	for _, n := range c.Nodes {
		if n.Server != nil {
			n.Server.DropConnections()
		}
	}
}

// StopNode shuts down one node's gRPC server, Raft instance, and SQLite store
// without closing its in-memory transport or deleting its data. A later
// StartNode uses the same directory and transport to simulate a process
// restart.
func (c *Cluster) StopNode(t testing.TB, id string) {
	t.Helper()

	c.mu.RLock()
	n, ok := c.byID[id]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("support: unknown node %q", id)
	}

	if n.Server != nil {
		_ = n.Server.Close()
	}
	if n.grpcSrv != nil {
		n.grpcSrv.Stop()
		n.grpcSrv = nil
	}
	if n.Conn != nil {
		_ = n.Conn.Close()
		n.Conn = nil
		n.Client = nil
	}
	if n.Raft != nil {
		if err := n.Raft.Close(); err != nil {
			t.Fatalf("support: close raft %s: %v", id, err)
		}
		n.Raft = nil
	}
	if n.Store != nil {
		if err := n.Store.Close(); err != nil {
			t.Fatalf("support: close store %s: %v", id, err)
		}
		n.Store = nil
	}
	if n.lis != nil {
		_ = n.lis.Close()
		n.lis = nil
	}
	n.Server = nil
}

// StartNode reopens a stopped node from its persisted SQLite and Raft state and
// restores its gRPC front end on the same in-memory transport.
func (c *Cluster) StartNode(t testing.TB, id string) {
	t.Helper()

	c.mu.RLock()
	n, ok := c.byID[id]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("support: unknown node %q", id)
	}
	if n.Raft != nil {
		t.Fatalf("support: node %s is already running", id)
	}

	store, err := storage.NewSQLiteStore(n.storePath)
	if err != nil {
		t.Fatalf("support: reopen store for %s: %v", id, err)
	}
	n.Store = store

	raftNode, err := consensus.StartRaft(n.ID, store, n.transport, n.Dir, raft.Configuration{})
	if err != nil {
		t.Fatalf("support: restart raft for %s: %v", id, err)
	}
	n.Raft = raftNode

	cfg := config.Config{
		NodeID:  n.ID,
		DataDir: n.Dir,
		Peers:   c.Peers,
	}.WithDefaults()
	n.Server = server.New(cfg, store, raftNode, c.dial)

	n.lis = bufconn.Listen(clusterBufSize)
	n.grpcSrv = grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.MaxSendMsgSize(cfg.MaxMessageSize),
	)
	pb.RegisterStorageServiceServer(n.grpcSrv, n.Server)
	go func(srv *grpc.Server, lis *bufconn.Listener) {
		_ = srv.Serve(lis)
	}(n.grpcSrv, n.lis)

	conn, err := c.dial(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("support: dial restarted %s: %v", id, err)
	}
	n.Conn = conn
	n.Client = pb.NewStorageServiceClient(conn)
}

// WaitLeader waits for exactly one node to report itself leader.
func (c *Cluster) WaitLeader(timeout time.Duration) (*Node, error) {
	deadline := time.Now().Add(timeout)
	for {
		var leader *Node
		leaders := 0
		for _, n := range c.Nodes {
			if n.Raft != nil && n.Raft.Raft.State() == raft.Leader {
				leader = n
				leaders++
			}
		}
		if leaders == 1 {
			return leader, nil
		}
		if time.Now().After(deadline) {
			return nil, errors.New("support: no single Raft leader elected")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// NonLeaderIDs returns every node ID except the current leader's.
func (c *Cluster) NonLeaderIDs(t testing.TB) []string {
	t.Helper()
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("support: %v", err)
	}
	ids := make([]string, 0, len(c.Nodes)-1)
	for _, n := range c.Nodes {
		if n.ID != leader.ID {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

// Put sends a write to the current leader.
func (c *Cluster) Put(t testing.TB, req *pb.PutRequest) *pb.PutResponse {
	t.Helper()
	leader, err := c.WaitLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("support: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := leader.Client.Put(ctx, req)
	if err != nil {
		t.Fatalf("support: Put(%q) RPC failed: %v", req.GetKey(), err)
	}
	return resp
}

// PutVia sends a write through a specific node, exercising forwarding.
func (c *Cluster) PutVia(t testing.TB, nodeID string, req *pb.PutRequest) *pb.PutResponse {
	t.Helper()
	c.mu.RLock()
	n, ok := c.byID[nodeID]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("support: unknown node %q", nodeID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := n.Client.Put(ctx, req)
	if err != nil {
		t.Fatalf("support: Put via %s failed: %v", nodeID, err)
	}
	return resp
}
