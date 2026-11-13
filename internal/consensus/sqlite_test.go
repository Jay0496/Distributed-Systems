package consensus

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	"github.com/hashicorp/raft"
)

// TestRaftSQLiteSnapshotRestore forces a metadata-only snapshot with the
// production SQLite store, restarts with a fresh SQLite file, and verifies the
// FSM restore path rebuilds metadata from the snapshot.
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
		meta := commandTestMeta(key, fmt.Sprintf("stripe-%02d", i), string(redundancy.ModeRS32), 1, []byte(key))
		applyCommand(t, node, Command{Op: OpPutMeta, Meta: &meta})
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

	waitForMetadata(t, freshStore, "sqlite-key-00", "stripe-00", 10*time.Second)
	waitForMetadata(t, freshStore, "sqlite-key-19", "stripe-19", 10*time.Second)
	t.Log("SQLite metadata restored from the Raft snapshot")
}

func waitSingleLeader(t *testing.T, node *Node) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for node.Raft.State() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForMetadata(t *testing.T, store *storage.SQLiteStore, key, stripeID string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		meta, err := store.GetKey(key)
		if err == nil && meta.StripeID == stripeID {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for metadata %q=%q", key, stripeID)
}

// TestMetadataReplayAfterRestart commits metadata, restarts with a fresh SQLite
// file but the same Raft log directory, and verifies the log replay rebuilds the
// metadata on the new store.
func TestMetadataReplayAfterRestart(t *testing.T) {
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
	waitSingleLeader(t, node)

	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 1, []byte("value"))
	applyCommand(t, node, Command{Op: OpPutMeta, Meta: &meta})

	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}
	_ = transport.Close()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

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

	waitForMetadata(t, freshStore, "alpha", "stripe-1", 10*time.Second)
	applied, err := freshStore.LastAppliedIndex()
	if err != nil || applied == 0 {
		t.Fatalf("applied index after replay = %d, %v", applied, err)
	}
}

// TestMetadataSnapshotRestore forces a metadata snapshot, restarts with a fresh
// SQLite file, and verifies the version 2 snapshot restores metadata, config,
// and the applied index.
func TestMetadataSnapshotRestore(t *testing.T) {
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
	waitSingleLeader(t, node)

	meta := commandTestMeta("alpha", "stripe-1", string(redundancy.ModeRS32), 2, []byte("value"))
	applyCommand(t, node, Command{Op: OpPutMeta, Meta: &meta})
	applyCommand(t, node, Command{
		Op:     OpSetConfig,
		Config: &storage.ClusterConfig{DefaultMode: string(redundancy.ModeReplication)},
	})

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

	waitForMetadata(t, freshStore, "alpha", "stripe-1", 10*time.Second)
	cfg, err := freshStore.GetConfig()
	if err != nil || cfg.DefaultMode != string(redundancy.ModeReplication) {
		t.Fatalf("restored config = %+v, %v", cfg, err)
	}
	applied, err := freshStore.LastAppliedIndex()
	if err != nil || applied == 0 {
		t.Fatalf("applied index after snapshot restore = %d, %v", applied, err)
	}
}
