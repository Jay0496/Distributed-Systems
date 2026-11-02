package consensus

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"distributed-kv-store/storage"

	"github.com/hashicorp/raft"
)

// TestRaftClusterMockStore spins up a 3-node in-memory Raft cluster, waits
// for a leader election, applies a Put command, and verifies that the data
// is replicated to every node's MockStore.
func TestRaftClusterMockStore(t *testing.T) {
	const numNodes = 3

	nodes := make([]*Node, numNodes)
	stores := make([]*storage.MockStore, numNodes)
	transports := make([]*raft.InmemTransport, numNodes)
	addrs := make([]raft.ServerAddress, numNodes)

	// 1. Create and fully mesh the in-memory transports.
	for i := 0; i < numNodes; i++ {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node%d", i+1)))
		addrs[i] = addr
		transports[i] = trans
	}

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	// 2. Define the full cluster configuration.
	bootstrapConfig := raft.Configuration{
		Servers: []raft.Server{
			{ID: "node1", Address: addrs[0]},
			{ID: "node2", Address: addrs[1]},
			{ID: "node3", Address: addrs[2]},
		},
	}

	// 3. Start the nodes. Only node1 bootstraps the full cluster config.
	for i := 0; i < numNodes; i++ {
		stores[i] = storage.NewMockStore()

		var cfg raft.Configuration
		if i == 0 {
			cfg = bootstrapConfig
		}

		var err error
		nodes[i], err = StartRaft(fmt.Sprintf("node%d", i+1), stores[i], transports[i], t.TempDir(), cfg)
		if err != nil {
			t.Fatalf("Failed to start node %d: %v", i+1, err)
		}
		defer nodes[i].Raft.Shutdown()
	}

	// 4. Wait for a leader to be elected.
	var leader *Node
	for i := 0; i < 50; i++ {
		for _, n := range nodes {
			if n.Raft.State() == raft.Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("No Raft leader elected within timeout")
	}
	t.Log("Leader elected successfully")

	// 5. Apply a Put command through the leader.
	cmd := Command{Op: "put", Key: "foo", Value: []byte("bar")}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	future := leader.Raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("Failed to apply command: %v", err)
	}
	if applyErr := future.Response(); applyErr != nil {
		t.Fatalf("FSM Apply returned an error: %v", applyErr)
	}

	// 6. Give replication a moment to reach every node.
	time.Sleep(500 * time.Millisecond)

	// 7. Verify the data exists on all three mocked stores.
	for i := 0; i < numNodes; i++ {
		val, err := stores[i].Get("foo")
		if err != nil {
			t.Fatalf("Node %d failed to get key: %v", i+1, err)
		}
		if string(val) != "bar" {
			t.Fatalf("Node %d expected 'bar', got '%s'", i+1, val)
		}
	}
	t.Log("Data successfully replicated to all 3 nodes")
}
