package consensus

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"distributed-kv-store/internal/storage"

	"github.com/hashicorp/raft"
)

// TestRaftSQLiteSnapshotRestore forces a snapshot with the production SQLite
// store, restarts with a fresh SQLite file, and verifies the FSM restore path
// rebuilds the legacy KV state from the snapshot.
func TestRaftSQLiteSnapshotRestore(t *testing.T) {
	raftDir := t.TempDir()
	addr, transport := raft.NewInmemTransport("node1")

	store, err := storage.NewSQLiteStore(filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}

	conf := raft.Configuration{Servers: []raft.Server{{ID: "node1", Address: addr}}}
	node, err := StartRaft("node1", store, transport, raftDir, conf)
	if err != nil {
		_ = store.Close()
		t.Fatalf("start node: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for node.Raft.State() != raft.Leader {
		if time.Now().After(deadline) {
			_ = node.Close()
			_ = store.Close()
			t.Fatal("single node did not become leader")
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("sqlite-key-%02d", i)
		applyCommand(t, node, Command{Op: OpPut, Legacy: &LegacyOp{Key: key, Value: []byte(key)}})
	}

	if err := node.Raft.Snapshot().Error(); err != nil {
		t.Fatalf("force snapshot: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}
	_ = transport.Close()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Restart with a brand-new SQLite file. Everything must come back from the
	// Raft snapshot rather than from the old database.
	freshStore, err := storage.NewSQLiteStore(filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatalf("open fresh SQLite store: %v", err)
	}
	_, restartedTransport := raft.NewInmemTransport(addr)
	restarted, err := StartRaft("node1", freshStore, restartedTransport, raftDir, raft.Configuration{})
	if err != nil {
		_ = freshStore.Close()
		t.Fatalf("restart node: %v", err)
	}
	t.Cleanup(func() {
		_ = restarted.Close()
		_ = restartedTransport.Close()
		_ = freshStore.Close()
	})

	waitForValue(t, freshStore, "sqlite-key-00", "sqlite-key-00", 10*time.Second)
	waitForValue(t, freshStore, "sqlite-key-19", "sqlite-key-19", 10*time.Second)
	t.Log("SQLite state restored from the Raft snapshot")
}
