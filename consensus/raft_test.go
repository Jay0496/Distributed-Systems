package consensus

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"distributed-kv-store/storage"

	"github.com/hashicorp/raft"
)

// testCluster is a reusable in-memory Raft cluster for tests.
type testCluster struct {
	ids        []string
	nodes      []*Node
	stores     []*storage.MockStore
	transports []*raft.InmemTransport
	addrs      []raft.ServerAddress
	dataDirs   []string
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	c := &testCluster{
		ids:        make([]string, n),
		nodes:      make([]*Node, n),
		stores:     make([]*storage.MockStore, n),
		transports: make([]*raft.InmemTransport, n),
		addrs:      make([]raft.ServerAddress, n),
		dataDirs:   make([]string, n),
	}

	for i := 0; i < n; i++ {
		c.ids[i] = fmt.Sprintf("node%d", i+1)
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(c.ids[i]))
		c.addrs[i] = addr
		c.transports[i] = trans
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				c.transports[i].Connect(c.addrs[j], c.transports[j])
			}
		}
	}

	servers := make([]raft.Server, 0, n)
	for i := 0; i < n; i++ {
		servers = append(servers, raft.Server{ID: raft.ServerID(c.ids[i]), Address: c.addrs[i]})
	}
	bootstrap := raft.Configuration{Servers: servers}

	for i := 0; i < n; i++ {
		c.stores[i] = storage.NewMockStore()
		c.dataDirs[i] = t.TempDir()

		cfg := raft.Configuration{}
		if i == 0 {
			cfg = bootstrap
		}

		node, err := StartRaft(c.ids[i], c.stores[i], c.transports[i], c.dataDirs[i], cfg)
		if err != nil {
			t.Fatalf("start %s: %v", c.ids[i], err)
		}
		c.nodes[i] = node
	}

	t.Cleanup(func() {
		for _, node := range c.nodes {
			if node != nil {
				_ = node.Close()
			}
		}
		for _, trans := range c.transports {
			if trans != nil {
				_ = trans.Close()
			}
		}
	})

	return c
}

// waitLeader blocks until one of the nodes (optionally excluding one index)
// reports itself as leader, and returns its index.
func (c *testCluster) waitLeader(t *testing.T, exclude int) int {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, node := range c.nodes {
			if node == nil || i == exclude {
				continue
			}
			if node.Raft.State() == raft.Leader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("no leader elected within timeout (exclude=%d)", exclude)
	return -1
}

func applyCommand(t *testing.T, node *Node, cmd Command) interface{} {
	t.Helper()

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	future := node.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("apply command: %v", err)
	}
	return future.Response()
}

func waitForValue(t *testing.T, store *storage.MockStore, key, want string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		value, err := store.Get(key)
		if err == nil && string(value) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q=%q", key, want)
}

// TestRaftClusterMockStore spins up a 3-node in-memory Raft cluster, waits
// for a leader election, applies a Put command, and verifies that the data
// is replicated to every node's MockStore.
func TestRaftClusterMockStore(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderIdx := c.waitLeader(t, -1)
	t.Logf("leader elected: %s", c.ids[leaderIdx])

	applyCommand(t, c.nodes[leaderIdx], Command{Op: "put", Key: "foo", Value: []byte("bar")})

	for i := 0; i < len(c.stores); i++ {
		waitForValue(t, c.stores[i], "foo", "bar", 5*time.Second)
	}
	t.Log("data successfully replicated to all 3 nodes")
}

// TestRaftLeaderFailover kills the leader and verifies that the remaining
// quorum elects a new leader that can still commit writes.
func TestRaftLeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)
	oldLeader := c.waitLeader(t, -1)
	t.Logf("initial leader: %s", c.ids[oldLeader])

	// Simulate a crash: stop Raft, release BoltDB, and close the transport.
	_ = c.nodes[oldLeader].Close()
	_ = c.transports[oldLeader].Close()
	c.nodes[oldLeader] = nil
	c.transports[oldLeader] = nil

	newLeader := c.waitLeader(t, oldLeader)
	t.Logf("new leader after failover: %s", c.ids[newLeader])

	applyCommand(t, c.nodes[newLeader], Command{Op: "put", Key: "after-failover", Value: []byte("ok")})

	for i, node := range c.nodes {
		if node == nil {
			continue
		}
		waitForValue(t, c.stores[i], "after-failover", "ok", 5*time.Second)
	}
	t.Log("writes still commit with 2/3 nodes alive")
}

// TestRaftPersistenceCatchUp stops a follower, commits new data, restarts the
// follower with the same BoltDB directory, and verifies log replay catches it up.
func TestRaftPersistenceCatchUp(t *testing.T) {
	c := newTestCluster(t, 3)
	leaderIdx := c.waitLeader(t, -1)

	applyCommand(t, c.nodes[leaderIdx], Command{Op: "put", Key: "before", Value: []byte("1")})
	for i := range c.stores {
		waitForValue(t, c.stores[i], "before", "1", 5*time.Second)
	}

	// Stop a follower. Closing the node releases the BoltDB file lock so the
	// same data directory can be reopened below.
	stopped := (leaderIdx + 1) % len(c.nodes)
	_ = c.nodes[stopped].Close()
	_ = c.transports[stopped].Close()
	c.nodes[stopped] = nil
	c.transports[stopped] = nil

	// Commit more data while it is down.
	applyCommand(t, c.nodes[leaderIdx], Command{Op: "put", Key: "after", Value: []byte("2")})
	waitForValue(t, c.stores[leaderIdx], "after", "2", 5*time.Second)

	// Restart the follower with the same data directory and a fresh store.
	_, trans := raft.NewInmemTransport(c.addrs[stopped])
	c.transports[stopped] = trans
	for j := range c.transports {
		if j == stopped || c.transports[j] == nil {
			continue
		}
		c.transports[j].Connect(c.addrs[stopped], trans)
		trans.Connect(c.addrs[j], c.transports[j])
	}

	c.stores[stopped] = storage.NewMockStore()
	node, err := StartRaft(c.ids[stopped], c.stores[stopped], trans, c.dataDirs[stopped], raft.Configuration{})
	if err != nil {
		t.Fatalf("restart %s: %v", c.ids[stopped], err)
	}
	c.nodes[stopped] = node

	// The restarted node must replay its log and catch up.
	waitForValue(t, c.stores[stopped], "after", "2", 10*time.Second)
	waitForValue(t, c.stores[stopped], "before", "1", 10*time.Second)
	t.Log("restarted follower caught up via persistent log")
}

// TestRaftSnapshotRestore forces a snapshot, restarts the node, and verifies
// that the FSM is restored from disk.
func TestRaftSnapshotRestore(t *testing.T) {
	dir := t.TempDir()
	addr, transport := raft.NewInmemTransport("node1")

	store := storage.NewMockStore()
	conf := raft.Configuration{Servers: []raft.Server{{ID: "node1", Address: addr}}}
	node, err := StartRaft("node1", store, transport, dir, conf)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for node.Raft.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%02d", i)
		applyCommand(t, node, Command{Op: "put", Key: key, Value: []byte(key)})
	}

	if err := node.Raft.Snapshot().Error(); err != nil {
		t.Fatalf("force snapshot: %v", err)
	}
	_ = node.Close()
	_ = transport.Close()

	// Restart with a fresh store, same data directory, no bootstrap.
	_, restartedTransport := raft.NewInmemTransport(addr)
	restartedStore := storage.NewMockStore()
	restarted, err := StartRaft("node1", restartedStore, restartedTransport, dir, raft.Configuration{})
	if err != nil {
		t.Fatalf("restart node: %v", err)
	}
	defer restarted.Close()
	defer restartedTransport.Close()

	waitForValue(t, restartedStore, "key-00", "key-00", 10*time.Second)
	waitForValue(t, restartedStore, "key-19", "key-19", 10*time.Second)
	t.Log("state restored from snapshot after restart")
}
