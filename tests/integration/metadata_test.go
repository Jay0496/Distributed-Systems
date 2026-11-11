package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	"github.com/hashicorp/raft"
)

const metadataLimitForTest = 64 * 1024

type metadataNode struct {
	id        string
	node      *consensus.Node
	store     *storage.SQLiteStore
	transport *raft.InmemTransport
	dir       string
}

// startMetadataCluster builds an in-process Raft cluster backed by real SQLite
// stores. It does not need Docker or the Compose network.
func startMetadataCluster(t *testing.T, count int) []*metadataNode {
	t.Helper()

	nodes := make([]*metadataNode, count)
	addrs := make([]raft.ServerAddress, count)
	transports := make([]*raft.InmemTransport, count)
	servers := make([]raft.Server, 0, count)

	for i := 0; i < count; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr, transport := raft.NewInmemTransport(raft.ServerAddress(id))
		nodes[i] = &metadataNode{id: id, transport: transport, dir: t.TempDir()}
		addrs[i] = addr
		transports[i] = transport
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: addr})
	}

	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	bootstrap := raft.Configuration{Servers: servers}
	for i := 0; i < count; i++ {
		store, err := storage.NewSQLiteStore(filepath.Join(nodes[i].dir, "node.db"))
		if err != nil {
			t.Fatalf("open store for %s: %v", nodes[i].id, err)
		}
		nodes[i].store = store

		cfg := raft.Configuration{}
		if i == 0 {
			cfg = bootstrap
		}
		node, err := consensus.StartRaft(nodes[i].id, store, nodes[i].transport, nodes[i].dir, cfg)
		if err != nil {
			t.Fatalf("start raft for %s: %v", nodes[i].id, err)
		}
		nodes[i].node = node
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			if n.node != nil {
				_ = n.node.Close()
			}
			if n.transport != nil {
				_ = n.transport.Close()
			}
			if n.store != nil {
				_ = n.store.Close()
			}
		}
	})

	return nodes
}

func metadataNodes(nodes []*metadataNode) []*consensus.Node {
	out := make([]*consensus.Node, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.node)
	}
	return out
}

func testMetadata(key, stripe string, payload []byte) storage.KeyMeta {
	sum := sha256.Sum256(payload)
	placements := make([]storage.ShardPlacement, 0, redundancy.TotalShards)
	for i := 0; i < redundancy.TotalShards; i++ {
		placements = append(placements, storage.ShardPlacement{
			ShardIndex: i,
			NodeID:     fmt.Sprintf("node%d", i+1),
			IsParity:   i >= redundancy.RSDataShards,
		})
	}
	return storage.KeyMeta{
		Key:       key,
		StripeID:  stripe,
		Mode:      string(redundancy.ModeRS32),
		Size:      int64(len(payload)),
		Checksum:  hex.EncodeToString(sum[:]),
		Version:   1,
		Placement: placements,
	}
}

func waitForMetadata(t *testing.T, store *storage.SQLiteStore, key, stripe string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		meta, err := store.GetKey(key)
		if err == nil && meta.StripeID == stripe {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for metadata %q=%q", key, stripe)
}

// TestMetadataReplicatesToFollowers commits a metadata-only command for a 1 MiB
// payload and verifies that the command stays small and every follower applies
// the metadata.
func TestMetadataReplicatesToFollowers(t *testing.T) {
	nodes := startMetadataCluster(t, 3)

	payload := bytes.Repeat([]byte("payload-bytes-"), 1<<16) // ~1 MiB
	meta := testMetadata("large", "stripe-large", payload)
	command := consensus.Command{Op: consensus.OpPutMeta, Meta: &meta}
	data, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal metadata command: %v", err)
	}

	if len(data) > metadataLimitForTest {
		t.Fatalf("metadata command is %d bytes, limit is %d", len(data), metadataLimitForTest)
	}
	if bytes.Contains(data, []byte("payload-bytes-")) {
		t.Fatal("metadata command contains payload bytes")
	}
	if !bytes.Contains(data, []byte(meta.Checksum)) {
		t.Fatal("metadata command is missing the checksum")
	}

	response, err := support.ApplyRawToLeader(metadataNodes(nodes), data, 10*time.Second)
	if err != nil {
		t.Fatalf("apply metadata: %v", err)
	}
	result, ok := response.(*consensus.Result)
	if !ok || result.Err != "" || !result.Found || result.Meta == nil {
		t.Fatalf("metadata result = %#v", response)
	}
	if result.Meta.StripeID != meta.StripeID || result.Meta.Size != int64(len(payload)) {
		t.Fatalf("metadata result = %+v", result.Meta)
	}

	for _, n := range nodes {
		waitForMetadata(t, n.store, meta.Key, meta.StripeID, 10*time.Second)
		stored, err := n.store.GetKey(meta.Key)
		if err != nil {
			t.Fatalf("GetKey on %s: %v", n.id, err)
		}
		if stored.Checksum != meta.Checksum || stored.Mode != meta.Mode || len(stored.Placement) != redundancy.TotalShards {
			t.Fatalf("metadata on %s = %+v", n.id, stored)
		}
	}
}
