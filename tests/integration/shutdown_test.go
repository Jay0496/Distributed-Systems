package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// TestGracefulShutdownReleasesLocksAndPersistsMetadata starts a real SQLite and
// Raft node, commits metadata, runs the production shutdown sequence, then
// reopens the same files without a lock error and reads the key back.
func TestGracefulShutdownReleasesLocksAndPersistsMetadata(t *testing.T) {
	base := t.TempDir()
	storePath := filepath.Join(base, "node.db")
	raftDir := filepath.Join(base, "raft")

	store, err := storage.NewSQLiteStore(storePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	addr, transport := raft.NewInmemTransport("node1")
	conf := raft.Configuration{Servers: []raft.Server{{ID: "node1", Address: addr}}}
	node, err := consensus.StartRaft("node1", store, transport, raftDir, conf)
	if err != nil {
		_ = store.Close()
		t.Fatalf("start raft: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for node.Raft.State() != raft.Leader {
		if waitCtx.Err() != nil {
			t.Fatal("single node did not become leader")
		}
		time.Sleep(25 * time.Millisecond)
	}

	cfg := config.Config{
		NodeID:  "node1",
		DataDir: base,
		Peers:   map[string]string{"node1": "node1"},
	}
	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	storageServer := server.New(cfg, store, node, nil)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() { _ = lis.Close() })

	command, err := json.Marshal(consensus.Command{
		Op:   consensus.OpPutMeta,
		Meta: shutdownTestMeta("shutdown-key", "stripe-shutdown", []byte("value")),
	})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	if err := node.Raft.Apply(command, 5*time.Second).Error(); err != nil {
		t.Fatalf("apply metadata: %v", err)
	}

	if err := server.Shutdown(server.ShutdownOptions{
		Server:    storageServer,
		GRPC:      grpcServer,
		Store:     store,
		Raft:      node,
		Transport: transport,
		Timeout:   5 * time.Second,
	}); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Reopening both files proves the SQLite connection and Raft file lock were
	// released. A missing Close leaves this blocked or erroring.
	reopened, err := storage.NewSQLiteStore(storePath)
	if err != nil {
		t.Fatalf("reopen SQLite after shutdown: %v", err)
	}
	meta, err := reopened.GetKey("shutdown-key")
	if err != nil {
		t.Fatalf("read key after shutdown: %v", err)
	}
	if meta.StripeID != "stripe-shutdown" {
		t.Fatalf("metadata after shutdown = %+v", meta)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}

	restartedStore, err := storage.NewSQLiteStore(storePath)
	if err != nil {
		t.Fatalf("open store for restart: %v", err)
	}
	_, restartedTransport := raft.NewInmemTransport(addr)
	restarted, err := consensus.StartRaft("node1", restartedStore, restartedTransport, raftDir, raft.Configuration{})
	if err != nil {
		_ = restartedStore.Close()
		t.Fatalf("restart raft without a lock error: %v", err)
	}
	t.Cleanup(func() {
		_ = restarted.Close()
		_ = restartedTransport.Close()
		_ = restartedStore.Close()
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := restartedStore.GetKey("shutdown-key"); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("key was not readable after restart")
}

func shutdownTestMeta(key, stripeID string, value []byte) *storage.KeyMeta {
	sum := sha256.Sum256(value)
	placements := make([]storage.ShardPlacement, 0, 5)
	for i := 0; i < 5; i++ {
		placements = append(placements, storage.ShardPlacement{
			ShardIndex: i,
			NodeID:     []string{"node1", "node2", "node3", "node4", "node5"}[i],
			IsParity:   i >= 3,
		})
	}
	return &storage.KeyMeta{
		Key:       key,
		StripeID:  stripeID,
		Mode:      "rs32",
		Size:      int64(len(value)),
		Checksum:  hex.EncodeToString(sum[:]),
		Version:   1,
		Placement: placements,
	}
}
