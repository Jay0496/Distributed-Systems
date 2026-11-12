package support

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

const clusterBufSize = 8 << 20

// ClusterOptions configures StartClusterWithOptions.
type ClusterOptions struct {
	// Count is the number of nodes to start. Five models the production
	// cluster; smaller counts are useful for quorum-negative cases.
	Count int
	// AuthToken enables the production bearer-token interceptors on every
	// node and attaches it to the harness's internal and client dials.
	AuthToken string
}

// trafficKey identifies one directed dial pair. An empty From means any
// caller, which models an unreachable target rather than a partition.
type trafficKey struct {
	From string
	To   string
}

// Cluster is a five-node in-process deployment: real Raft over in-memory
// transports, real SQLite stores, and gRPC front ends on controllable bufconn
// listeners. Tests can make individual nodes unreachable without stopping Raft.
type Cluster struct {
	Nodes []*Node
	Peers map[string]string

	AuthToken string

	byID map[string]*Node
	dial func(context.Context, string) (*grpc.ClientConn, error)

	mu          sync.RWMutex
	unreachable map[string]bool
	blocked     map[trafficKey]bool

	failure       bool
	failureCtx    context.Context
	failureCancel context.CancelFunc

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
	return StartClusterWithOptions(t, ClusterOptions{Count: count})
}

// StartClusterWithOptions starts an in-process deployment and waits for a
// leader. Every node uses its own SQLite file under a temporary directory.
// Cleanup, including closing Raft and SQLite, is registered with t.
func StartClusterWithOptions(t testing.TB, opts ClusterOptions) *Cluster {
	t.Helper()

	count := opts.Count
	if count <= 0 {
		count = 5
	}

	c := &Cluster{
		Nodes:       make([]*Node, count),
		Peers:       make(map[string]string, count),
		AuthToken:   opts.AuthToken,
		byID:        make(map[string]*Node, count),
		unreachable: make(map[string]bool, count),
		blocked:     make(map[trafficKey]bool),
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

	c.dial = func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		return c.dialWithToken(ctx, "", target, c.AuthToken)
	}

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

		cfg.AuthToken = c.AuthToken
		n.Server = server.New(cfg, n.Store, n.Raft, c.dialFrom(n.ID))
		if c.AuthToken != "" {
			n.Server.EnableAuth()
		}
		n.grpcSrv = grpc.NewServer(c.grpcServerOptions(cfg)...)
		pb.RegisterStorageServiceServer(n.grpcSrv, n.Server)
		registerTestHealth(n.grpcSrv)

		go func(srv *grpc.Server, lis *bufconn.Listener) {
			_ = srv.Serve(lis)
		}(n.grpcSrv, n.lis)

		conn, err := c.dial(context.Background(), n.ID)
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
		cancel := c.failureCancel
		c.failure = false
		c.failureCtx = nil
		c.failureCancel = nil
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}

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

// Dialer exposes the cluster's in-process gRPC dial function so integration
// tests can drive the real gateway client over bufconn. The returned dialer
// attaches the cluster's auth token, if any.
func (c *Cluster) Dialer() func(context.Context, string) (*grpc.ClientConn, error) {
	return c.dial
}

// DialerWithToken returns a dialer that overrides the cluster token. Pass an
// empty token to exercise missing-credential paths through a gateway client.
func (c *Cluster) DialerWithToken(token string) func(context.Context, string) (*grpc.ClientConn, error) {
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		return c.dialWithToken(ctx, "", target, token)
	}
}

// DialRaw opens a connection to one node using the supplied token, bypassing
// the cluster's cached client connections. It is used by auth tests that must
// present a missing or wrong credential.
func (c *Cluster) DialRaw(ctx context.Context, target, token string) (*grpc.ClientConn, error) {
	return c.dialWithToken(ctx, "", target, token)
}

// dialFrom returns the dial function used by one node's server. The source ID
// lets DropTraffic model a one-directional partition.
func (c *Cluster) dialFrom(source string) func(context.Context, string) (*grpc.ClientConn, error) {
	return func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		return c.dialWithToken(ctx, source, target, c.AuthToken)
	}
}

// dialWithToken resolves target to its current listener and opens a bufconn
// connection. It never bypasses an injected outage.
func (c *Cluster) dialWithToken(ctx context.Context, source, target, token string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	down := c.unreachable[target] || c.blocked[trafficKey{From: source, To: target}] ||
		c.blocked[trafficKey{From: "", To: target}]
	n, ok := c.byID[target]
	var listener *bufconn.Listener
	if ok {
		listener = n.lis
	}
	c.mu.RUnlock()
	if down {
		return nil, fmt.Errorf("support: node %s is unreachable", target)
	}
	if !ok {
		return nil, fmt.Errorf("support: unknown peer %q", target)
	}
	if listener == nil {
		return nil, fmt.Errorf("support: node %s is stopped", target)
	}

	opts, err := rpc.DialOptions(token, config.DefaultMaxMessageSize)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	return grpc.DialContext(ctx, "bufnet", opts...)
}

// grpcServerOptions builds the production message limits and, when requested,
// the bearer-token interceptors.
func (c *Cluster) grpcServerOptions(cfg config.Config) []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.MaxSendMsgSize(cfg.MaxMessageSize),
	}
	if c.AuthToken != "" {
		opts = append(opts, server.AuthServerOptions(c.AuthToken)...)
	}
	return opts
}

// DropTraffic makes dials from one node to another fail, modelling a
// one-directional partition without stopping either process. An empty from
// blocks every caller.
func (c *Cluster) DropTraffic(from, to string) {
	c.mu.Lock()
	c.blocked[trafficKey{From: from, To: to}] = true
	c.mu.Unlock()
	c.dropAllConnections()
}

// RestoreTraffic removes a directed partition installed by DropTraffic.
func (c *Cluster) RestoreTraffic(from, to string) {
	c.mu.Lock()
	delete(c.blocked, trafficKey{From: from, To: to})
	c.mu.Unlock()
	c.dropAllConnections()
}

// Partition makes the named nodes unreachable in both directions while leaving
// the rest of the cluster connected.
func (c *Cluster) Partition(ids ...string) {
	c.mu.Lock()
	for _, id := range ids {
		c.blocked[trafficKey{From: "", To: id}] = true
	}
	c.mu.Unlock()
	c.dropAllConnections()
}

// Heal removes a partition installed by Partition.
func (c *Cluster) Heal(ids ...string) {
	c.mu.Lock()
	for _, id := range ids {
		delete(c.blocked, trafficKey{From: "", To: id})
	}
	c.mu.Unlock()
	c.dropAllConnections()
}

func (c *Cluster) dropAllConnections() {
	for _, n := range c.Nodes {
		if n.Server != nil {
			n.Server.DropConnections()
		}
	}
}

// EnableFailureManagement starts a leader-gated detector and repairer on every
// node. Interval, failure, and success thresholds are short so chaos tests can
// observe transitions without waiting for production defaults.
func (c *Cluster) EnableFailureManagement(t testing.TB) {
	t.Helper()

	c.mu.Lock()
	if c.failure {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.failure = true
	c.failureCtx = ctx
	c.failureCancel = cancel
	c.mu.Unlock()

	for _, n := range c.Nodes {
		c.attachFailureManagement(t, n, ctx)
	}
}

func (c *Cluster) attachFailureManagement(t testing.TB, n *Node, ctx context.Context) {
	t.Helper()
	if n == nil || n.Server == nil || n.Raft == nil {
		return
	}

	nodes := make(map[string]string, len(c.Peers))
	for id, addr := range c.Peers {
		nodes[id] = addr
	}
	detector := cluster.NewDetector(cluster.DetectorOptions{
		Interval:  100 * time.Millisecond,
		Timeout:   250 * time.Millisecond,
		Failures:  1,
		Successes: 1,
		Nodes:     nodes,
		SelfNode:  n.ID,
		Probe:     n.Server.ProbePeer,
		IsLeader:  func() bool { return n.Raft != nil && n.Raft.Raft.State() == raft.Leader },
		LeaderCh:  n.Raft.Raft.LeaderCh(),
	})
	if repairer := n.Server.Repairer(); repairer != nil {
		repairer.SetOrphanSweeper(cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
			Store:       n.Store,
			SelfNode:    n.ID,
			GracePeriod: 200 * time.Millisecond,
		}))
	}
	n.Server.AttachFailureManagement(ctx, detector)
}

// registerTestHealth attaches the standard gRPC health service so detector
// probes work exactly as they do in production.
func registerTestHealth(srv *grpc.Server) {
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("storage.StorageService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
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
	c.mu.Lock()
	lis := n.lis
	n.lis = nil
	c.mu.Unlock()
	if lis != nil {
		_ = lis.Close()
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
	cfg.AuthToken = c.AuthToken
	n.Server = server.New(cfg, store, raftNode, c.dialFrom(n.ID))
	if c.AuthToken != "" {
		n.Server.EnableAuth()
	}

	lis := bufconn.Listen(clusterBufSize)
	c.mu.Lock()
	n.lis = lis
	c.mu.Unlock()
	n.grpcSrv = grpc.NewServer(c.grpcServerOptions(cfg)...)
	pb.RegisterStorageServiceServer(n.grpcSrv, n.Server)
	registerTestHealth(n.grpcSrv)
	go func(srv *grpc.Server, listener *bufconn.Listener) {
		_ = srv.Serve(listener)
	}(n.grpcSrv, lis)

	conn, err := c.dial(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("support: dial restarted %s: %v", id, err)
	}
	n.Conn = conn
	n.Client = pb.NewStorageServiceClient(conn)

	// bufconn listeners are recreated on restart, so previously cached client
	// connections point at a closed listener. Drop outbound caches to model a
	// TCP client reconnecting to the same address.
	for _, other := range c.Nodes {
		if other.Server != nil {
			other.Server.DropConnections()
		}
	}

	c.mu.RLock()
	failure := c.failure
	failureCtx := c.failureCtx
	c.mu.RUnlock()
	if failure && failureCtx != nil {
		c.attachFailureManagement(t, n, failureCtx)
	}
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

// WaitFor polls condition until it is true or timeout expires. It is the
// synchronization primitive for state-machine and repair assertions; tests must
// use it instead of fixed sleeps.
func (c *Cluster) WaitFor(t testing.TB, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("support: timed out waiting for %s", description)
}

// Node returns the node with the given ID.
func (c *Cluster) Node(t testing.TB, id string) *Node {
	t.Helper()
	c.mu.RLock()
	n, ok := c.byID[id]
	c.mu.RUnlock()
	if !ok {
		t.Fatalf("support: unknown node %q", id)
	}
	return n
}

// FragmentRows returns the local fragment metadata for one node. An empty
// stripeID lists every local fragment on that node.
func (c *Cluster) FragmentRows(t testing.TB, nodeID, stripeID string) []storage.FragmentRef {
	t.Helper()
	n := c.Node(t, nodeID)
	if n.Store == nil {
		return nil
	}
	refs, err := n.Store.ListFragments(stripeID)
	if err != nil {
		t.Fatalf("support: list fragments on %s: %v", nodeID, err)
	}
	return refs
}

// StorePath returns the SQLite file backing this node.
func (n *Node) StorePath() string {
	return n.storePath
}

// RemoveStore deletes a stopped node's SQLite database and WAL files. It is
// used to prove that Raft snapshot restore rebuilds metadata into a fresh
// database. The call fails the test if the node is still running.
func (c *Cluster) RemoveStore(t testing.TB, id string) {
	t.Helper()
	n := c.Node(t, id)
	if n.Raft != nil || n.Store != nil {
		t.Fatalf("support: node %s must be stopped before removing its store", id)
	}
	for _, path := range []string{n.storePath, n.storePath + "-wal", n.storePath + "-shm"} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("support: remove %s: %v", path, err)
		}
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
