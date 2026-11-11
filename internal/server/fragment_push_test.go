package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// pushHarness is a fragment-only five-node mesh. It intentionally has no Raft:
// pushShards only needs the gRPC fragment transport and the local stores.
type pushHarness struct {
	ids       []string
	stores    map[string]*testStore
	listeners map[string]*bufconn.Listener
	servers   []*grpc.Server
	down      map[string]bool
	dial      dialFunc
	dialCalls atomic.Int32
}

func newPushHarness(t *testing.T, n int) *pushHarness {
	t.Helper()

	h := &pushHarness{
		stores:    make(map[string]*testStore, n),
		listeners: make(map[string]*bufconn.Listener, n),
		down:      make(map[string]bool),
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		h.ids = append(h.ids, id)
		h.stores[id] = newTestStore()
		h.listeners[id] = bufconn.Listen(fragmentTestBufSize)
	}

	peers := make(map[string]string, n)
	for _, id := range h.ids {
		peers[id] = id
	}

	dial := func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		if h.down[target] {
			return nil, fmt.Errorf("node %s is unreachable", target)
		}
		lis, ok := h.listeners[target]
		if !ok {
			return nil, fmt.Errorf("unknown peer %q", target)
		}
		h.dialCalls.Add(1)
		return grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
	}
	h.dial = dial

	for _, id := range h.ids {
		cfg := config.Config{NodeID: id, Peers: peers, UploadChunkSize: 1024}.WithDefaults()
		srv := New(cfg, h.stores[id], nil, dial)
		grpcSrv := grpc.NewServer()
		pb.RegisterStorageServiceServer(grpcSrv, srv)
		h.servers = append(h.servers, grpcSrv)
		lis := h.listeners[id]
		go func() {
			_ = grpcSrv.Serve(lis)
		}()
	}

	t.Cleanup(func() {
		for _, srv := range h.servers {
			srv.Stop()
		}
		for _, lis := range h.listeners {
			_ = lis.Close()
		}
	})
	return h
}

func (h *pushHarness) server() *Server {
	peers := make(map[string]string, len(h.ids))
	for _, id := range h.ids {
		peers[id] = id
	}
	cfg := config.Config{NodeID: h.ids[0], Peers: peers, UploadChunkSize: 1024}.WithDefaults()
	return New(cfg, h.stores[h.ids[0]], nil, h.dial)
}

// placementsOn assigns shard i to ids[i] so tests can control success.
func placementsOn(shards []redundancy.Shard, ids []string) []storage.ShardPlacement {
	out := make([]storage.ShardPlacement, len(shards))
	for i, shard := range shards {
		out[i] = storage.ShardPlacement{
			ShardIndex: shard.Index,
			NodeID:     ids[i],
			IsParity:   shard.IsParity,
		}
	}
	return out
}

func TestPushShardsFiveSuccesses(t *testing.T) {
	h := newPushHarness(t, 5)
	shards, err := redundancy.NewRS32Engine().Encode([]byte("five shards"), redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	stored := h.server().pushShards(context.Background(), "stripe-five", shards, placementsOn(shards, h.ids))
	if len(stored) != 5 {
		t.Fatalf("stored %d shards, want 5", len(stored))
	}

	for i, id := range h.ids {
		refs, err := h.stores[id].ListFragments("stripe-five")
		if err != nil {
			t.Fatalf("list %s: %v", id, err)
		}
		if len(refs) != 1 || refs[0].ShardIndex != i {
			t.Fatalf("%s fragments = %+v, want shard %d", id, refs, i)
		}
	}
}

func TestPushShardsThreeSuccessesWithTwoUnreachable(t *testing.T) {
	h := newPushHarness(t, 5)
	h.down["node4"] = true
	h.down["node5"] = true

	shards, err := redundancy.NewReplicationEngine().Encode([]byte("degraded"), redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	stored := h.server().pushShards(context.Background(), "stripe-degraded", shards, placementsOn(shards, h.ids))
	if len(stored) != writeQuorum {
		t.Fatalf("stored %d shards, want %d", len(stored), writeQuorum)
	}
	for _, id := range []string{"node1", "node2", "node3"} {
		refs, err := h.stores[id].ListFragments("stripe-degraded")
		if err != nil || len(refs) != 1 {
			t.Fatalf("%s fragments = %+v, err=%v", id, refs, err)
		}
	}
}

func TestPushShardsBelowQuorum(t *testing.T) {
	h := newPushHarness(t, 5)
	for _, id := range []string{"node3", "node4", "node5"} {
		h.down[id] = true
	}

	shards, err := redundancy.NewRS32Engine().Encode([]byte("below quorum"), redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	stored := h.server().pushShards(context.Background(), "stripe-short", shards, placementsOn(shards, h.ids))
	if len(stored) >= writeQuorum {
		t.Fatalf("stored %d shards, want fewer than %d", len(stored), writeQuorum)
	}
	if len(stored) != 2 {
		t.Fatalf("stored %d shards, want exactly the local node and node2", len(stored))
	}
}

func TestPushShardLocalOwnerFastPath(t *testing.T) {
	h := newPushHarness(t, 5)
	shards, err := redundancy.NewRS32Engine().Encode([]byte("local"), redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	placement := storage.ShardPlacement{ShardIndex: 0, NodeID: "node1"}

	srv := h.server()
	srv.dial = func(context.Context, string) (*grpc.ClientConn, error) {
		return nil, fmt.Errorf("local owner must not dial")
	}
	if err := srv.pushShard(context.Background(), "stripe-local", shards[0], placement); err != nil {
		t.Fatalf("pushShard: %v", err)
	}

	frag, err := h.stores["node1"].GetFragment("stripe-local", 0)
	if err != nil {
		t.Fatalf("local fragment: %v", err)
	}
	if string(frag.Data) != string(shards[0].Data) {
		t.Fatalf("local fragment data mismatch")
	}
}

func TestPushShardRejectsChecksumMismatch(t *testing.T) {
	h := newPushHarness(t, 5)
	shards, err := redundancy.NewRS32Engine().Encode([]byte("checksum"), redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	bad := shards[0]
	bad.Checksum = redundancy.Checksum([]byte("different bytes"))

	stored := h.server().pushShards(context.Background(), "stripe-bad", []redundancy.Shard{bad}, []storage.ShardPlacement{
		{ShardIndex: 0, NodeID: "node1"},
	})
	if len(stored) != 0 {
		t.Fatalf("corrupt shard was stored: %+v", stored)
	}
	refs, err := h.stores["node1"].ListFragments("stripe-bad")
	if err != nil {
		t.Fatalf("list fragments: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("corrupt shard left %d fragments", len(refs))
	}
}

func TestEncodeAndPlaceUsesFiveDistinctNodes(t *testing.T) {
	h := newPushHarness(t, 5)
	srv := h.server()

	shards, placements, err := srv.encodeAndPlace(&writeIntent{
		key:      "k",
		value:    []byte("placement"),
		mode:     redundancy.ModeRS32,
		version:  1,
		stripeID: "stripe-place",
	})
	if err != nil {
		t.Fatalf("encodeAndPlace: %v", err)
	}
	if len(shards) != redundancy.TotalShards || len(placements) != redundancy.TotalShards {
		t.Fatalf("shards=%d placements=%d", len(shards), len(placements))
	}

	seen := make(map[string]struct{}, len(placements))
	for i, placement := range placements {
		if placement.ShardIndex != i {
			t.Fatalf("placement %d has shard index %d", i, placement.ShardIndex)
		}
		seen[placement.NodeID] = struct{}{}
		wantParity := i >= redundancy.RSDataShards
		if placement.IsParity != wantParity {
			t.Fatalf("shard %d parity=%v, want %v", i, placement.IsParity, wantParity)
		}
	}
	if len(seen) != redundancy.TotalShards {
		t.Fatalf("placements use %d distinct nodes, want %d", len(seen), redundancy.TotalShards)
	}
}

// Ensure the timeout helper does not silently accept a cancelled context.
func TestPushShardHonoursContext(t *testing.T) {
	h := newPushHarness(t, 5)
	shards, err := redundancy.NewRS32Engine().Encode([]byte("ctx"), redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	err = h.server().pushShard(ctx, "stripe-ctx", shards[0], storage.ShardPlacement{ShardIndex: 0, NodeID: "node1"})
	if err == nil {
		t.Fatal("expected a context error")
	}
}
