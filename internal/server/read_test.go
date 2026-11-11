package server

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newReadTestServer(store *testStore) *Server {
	srv := &Server{
		store:    store,
		cfg:      config.Config{NodeID: "node1"}.WithDefaults(),
		readRand: rand.New(rand.NewSource(1)),
	}
	srv.apply = metadataApply(store)
	return srv
}

// localPlacements puts every shard on node1 so read tests exercise decode and
// local fetch without gRPC. Placement distinctness is tested by the integration
// suite against real metadata.
func localPlacements() []storage.ShardPlacement {
	placements := make([]storage.ShardPlacement, redundancy.TotalShards)
	for i := range placements {
		placements[i] = storage.ShardPlacement{ShardIndex: i, NodeID: "node1"}
	}
	return placements
}

func seedReadStripe(t *testing.T, store *testStore, key, mode string, value []byte, shards []redundancy.Shard) storage.KeyMeta {
	t.Helper()

	meta := storage.KeyMeta{
		Key:       key,
		StripeID:  "stripe-" + key,
		Mode:      mode,
		Size:      int64(len(value)),
		Checksum:  redundancy.Checksum(value),
		Version:   1,
		Placement: localPlacements(),
	}
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	for _, shard := range shards {
		frag := storage.Fragment{
			StripeID:   meta.StripeID,
			ShardIndex: shard.Index,
			IsParity:   shard.IsParity,
			Data:       append([]byte(nil), shard.Data...),
			Checksum:   shard.Checksum,
		}
		if err := store.PutFragment(frag); err != nil {
			t.Fatalf("seed fragment %d: %v", shard.Index, err)
		}
	}
	return meta
}

func encodeForTest(t *testing.T, mode redundancy.Mode, value []byte) []redundancy.Shard {
	t.Helper()
	engine, err := engineFor(mode)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	shards, err := engine.Encode(value, mode)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return shards
}

func TestReadRS32Healthy(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := bytes.Repeat([]byte("healthy-read-"), 211)
	shards := encodeForTest(t, redundancy.ModeRS32, value)
	seedReadStripe(t, store, "healthy", string(redundancy.ModeRS32), value, shards)

	resp, err := srv.read(context.Background(), &pb.GetRequest{Key: "healthy"})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !resp.GetSuccess() || !bytes.Equal(resp.GetValue(), value) {
		t.Fatalf("read returned success=%v value=%d bytes", resp.GetSuccess(), len(resp.GetValue()))
	}
	if resp.GetMode() != string(redundancy.ModeRS32) || resp.GetStripeId() != "stripe-healthy" {
		t.Fatalf("read metadata = %+v", resp)
	}
	if resp.GetShardsUsed() != redundancy.RSDataShards {
		t.Fatalf("shards_used = %d, want %d", resp.GetShardsUsed(), redundancy.RSDataShards)
	}
	if resp.GetChecksum() != redundancy.Checksum(value) {
		t.Fatalf("checksum = %q", resp.GetChecksum())
	}
}

func TestReadRS32WithTwoMissingShards(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := bytes.Repeat([]byte("two-missing-"), 333)
	shards := encodeForTest(t, redundancy.ModeRS32, value)
	// Keep only shards 2, 3, and 4; the two data shards are gone.
	seedReadStripe(t, store, "missing", string(redundancy.ModeRS32), value, shards[2:])

	resp, err := srv.read(context.Background(), &pb.GetRequest{Key: "missing"})
	if err != nil {
		t.Fatalf("read with two missing shards: %v", err)
	}
	if !bytes.Equal(resp.GetValue(), value) {
		t.Fatal("decoded value does not match")
	}
}

func TestReadRS32RejectsCorruptShard(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := bytes.Repeat([]byte("corrupt-shard-"), 101)
	shards := encodeForTest(t, redundancy.ModeRS32, value)

	// Store four shards and corrupt the first byte of shard 0, keeping its
	// original checksum. That shard must be rejected and must not enter decode.
	corrupt := shards[0]
	corrupt.Data = append([]byte(nil), corrupt.Data...)
	corrupt.Data[0] ^= 0xff
	// store.PutFragment verifies the checksum, so write the corrupted bytes
	// through a test-local fragment that carries the matching computed digest.
	frag := storage.Fragment{
		StripeID:   "stripe-corrupt",
		ShardIndex: corrupt.Index,
		Data:       corrupt.Data,
		Checksum:   redundancy.Checksum(corrupt.Data),
		IsParity:   corrupt.IsParity,
	}
	meta := storage.KeyMeta{
		Key:       "corrupt",
		StripeID:  frag.StripeID,
		Mode:      string(redundancy.ModeRS32),
		Size:      int64(len(value)),
		Checksum:  redundancy.Checksum(value),
		Version:   1,
		Placement: localPlacements(),
	}
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	// Deliberately mismatch the stored checksum so the local fetch rejects the
	// fragment even though the bytes are internally consistent.
	frag.Checksum = shards[0].Checksum
	if err := store.PutFragment(frag); err != nil {
		t.Fatalf("seed corrupt fragment: %v", err)
	}
	for _, shard := range shards[1:4] {
		if err := store.PutFragment(storage.Fragment{
			StripeID:   meta.StripeID,
			ShardIndex: shard.Index,
			Data:       shard.Data,
			Checksum:   shard.Checksum,
			IsParity:   shard.IsParity,
		}); err != nil {
			t.Fatalf("seed shard %d: %v", shard.Index, err)
		}
	}

	valueOut, failed, err := srv.readVerified(context.Background(), meta)
	if err != nil {
		t.Fatalf("read with one corrupt shard: %v", err)
	}
	if !bytes.Equal(valueOut, value) {
		t.Fatal("decoded value does not match")
	}
	if len(failed) == 0 || failed[0].ShardIndex != 0 || !failed[0].Corrupt {
		t.Fatalf("failed = %+v, want corrupt shard 0", failed)
	}
}

func TestReadReplicationWithFourMissingCopies(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := bytes.Repeat([]byte("one-copy-left-"), 97)
	shards := encodeForTest(t, redundancy.ModeReplication, value)
	seedReadStripe(t, store, "replicated", string(redundancy.ModeReplication), value, shards[:1])

	resp, err := srv.read(context.Background(), &pb.GetRequest{Key: "replicated"})
	if err != nil {
		t.Fatalf("read with one copy: %v", err)
	}
	if !bytes.Equal(resp.GetValue(), value) {
		t.Fatal("decoded value does not match")
	}
	if resp.GetMode() != string(redundancy.ModeReplication) || resp.GetShardsUsed() != 1 {
		t.Fatalf("read response = %+v", resp)
	}
}

func TestReadNotFound(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)

	_, err := srv.read(context.Background(), &pb.GetRequest{Key: "absent"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("read missing key = %v, want NotFound", err)
	}
}

func TestReadInsufficientShards(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := []byte("only two shards")
	shards := encodeForTest(t, redundancy.ModeRS32, value)
	seedReadStripe(t, store, "short", string(redundancy.ModeRS32), value, shards[:2])

	_, err := srv.read(context.Background(), &pb.GetRequest{Key: "short"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("short read = %v, want FailedPrecondition", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("failed owners")) {
		t.Fatalf("insufficient-shard error does not list owners: %v", err)
	}
}
