package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/cluster"
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

// recordingQueue captures tasks instead of running them.
type recordingQueue struct {
	mu    sync.Mutex
	tasks []cluster.RepairTask
	err   error
}

func (q *recordingQueue) Enqueue(task cluster.RepairTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.tasks = append(q.tasks, task)
	return nil
}

func (q *recordingQueue) Close() {}

func (q *recordingQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func newRepairingServer(t *testing.T, store *testStore) *Server {
	t.Helper()
	srv := newReadTestServer(store)
	srv.repairQueue = cluster.NewRepairQueue(1, 8, 5*time.Second, srv.performReadRepair)
	t.Cleanup(srv.repairQueue.Close)
	return srv
}

func waitFragmentCount(t *testing.T, store *testStore, stripeID string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		refs, err := store.ListFragments(stripeID)
		if err != nil {
			t.Fatalf("list fragments: %v", err)
		}
		last = len(refs)
		if last == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stripe %q has %d fragments, want %d", stripeID, last, want)
}

func TestReadRepairFillsMissingRSShards(t *testing.T) {
	for _, missing := range []int{1, 2} {
		t.Run(fmt.Sprintf("%d missing", missing), func(t *testing.T) {
			store := newTestStore()
			srv := newRepairingServer(t, store)
			value := bytes.Repeat([]byte("repair-me-"), 123)
			shards := encodeForTest(t, redundancy.ModeRS32, value)
			keep := redundancy.TotalShards - missing
			meta := seedReadStripe(t, store, "repair", string(redundancy.ModeRS32), value, shards[:keep])
			// A degraded stripe makes the read inspect all five placements, so
			// every missing shard is discovered and scheduled.
			if _, err := store.ApplyMetaStatus(2, meta.Key, meta.StripeID, meta.Version, true); err != nil {
				t.Fatalf("mark degraded: %v", err)
			}

			resp, err := srv.read(context.Background(), &pb.GetRequest{Key: "repair"})
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(resp.GetValue(), value) {
				t.Fatal("read returned the wrong value")
			}
			if !resp.GetRepaired() {
				t.Fatal("read did not report an enqueued repair")
			}
			waitFragmentCount(t, store, meta.StripeID, redundancy.TotalShards)

			refs, err := store.ListFragments(meta.StripeID)
			if err != nil {
				t.Fatalf("list fragments: %v", err)
			}
			for _, ref := range refs {
				if ref.Checksum != shards[ref.ShardIndex].Checksum {
					t.Fatalf("shard %d repair checksum = %s, want %s", ref.ShardIndex, ref.Checksum, shards[ref.ShardIndex].Checksum)
				}
			}
		})
	}
}

func TestReadRepairReplacesCorruptReplicationCopy(t *testing.T) {
	store := newTestStore()
	srv := newRepairingServer(t, store)
	value := bytes.Repeat([]byte("repair-copy-"), 77)
	shards := encodeForTest(t, redundancy.ModeReplication, value)

	meta := seedReadStripe(t, store, "copy", string(redundancy.ModeReplication), value, shards[1:2])
	corrupt := shards[0]
	corrupt.Data = append([]byte(nil), corrupt.Data...)
	corrupt.Data[0] ^= 0xff
	if err := store.PutFragment(storage.Fragment{
		StripeID:   meta.StripeID,
		ShardIndex: 0,
		Data:       corrupt.Data,
		Checksum:   shards[0].Checksum,
	}); err != nil {
		t.Fatalf("seed corrupt copy: %v", err)
	}

	resp, err := srv.read(context.Background(), &pb.GetRequest{Key: "copy"})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(resp.GetValue(), value) || !resp.GetRepaired() {
		t.Fatalf("read response success=%v repaired=%v", resp.GetSuccess(), resp.GetRepaired())
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		refs, err := store.ListFragments(meta.StripeID)
		if err != nil {
			t.Fatalf("list fragments: %v", err)
		}
		for _, ref := range refs {
			if ref.ShardIndex == 0 && ref.Checksum == shards[0].Checksum {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("corrupt replication copy was not repaired")
}

func TestReadVerifiedDoesNotEnqueueRepair(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	queue := &recordingQueue{}
	srv.repairQueue = queue

	value := bytes.Repeat([]byte("verified-"), 51)
	shards := encodeForTest(t, redundancy.ModeRS32, value)
	meta := seedReadStripe(t, store, "verified", string(redundancy.ModeRS32), value, shards[:3])
	if _, err := store.ApplyMetaStatus(2, meta.Key, meta.StripeID, meta.Version, true); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}

	got, err := srv.ReadVerified(context.Background(), meta)
	if err != nil {
		t.Fatalf("ReadVerified: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatal("ReadVerified returned the wrong value")
	}
	if queue.len() != 0 {
		t.Fatalf("ReadVerified enqueued %d repair tasks", queue.len())
	}
}

func TestReadAllOwnersDown(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := []byte("all owners unreachable")
	meta := storage.KeyMeta{
		Key:      "down",
		StripeID: "stripe-down",
		Mode:     string(redundancy.ModeRS32),
		Size:     int64(len(value)),
		Checksum: redundancy.Checksum(value),
		Version:  1,
		Placement: []storage.ShardPlacement{
			{ShardIndex: 0, NodeID: "node1"},
			{ShardIndex: 1, NodeID: "node2"},
			{ShardIndex: 2, NodeID: "node3"},
			{ShardIndex: 3, NodeID: "node4", IsParity: true},
			{ShardIndex: 4, NodeID: "node5", IsParity: true},
		},
	}
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}

	_, err := srv.read(context.Background(), &pb.GetRequest{Key: "down"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("all-owners-down read = %v, want FailedPrecondition", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("live owners")) ||
		!bytes.Contains([]byte(err.Error()), []byte("failed owners")) {
		t.Fatalf("error does not list owners: %v", err)
	}
}

func TestReadAllShardsCorrupt(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := bytes.Repeat([]byte("all-corrupt-"), 43)
	shards := encodeForTest(t, redundancy.ModeRS32, value)
	meta := storage.KeyMeta{
		Key:       "corrupt-all",
		StripeID:  "stripe-corrupt-all",
		Mode:      string(redundancy.ModeRS32),
		Size:      int64(len(value)),
		Checksum:  redundancy.Checksum(value),
		Version:   1,
		Placement: localPlacements(),
	}
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	for _, shard := range shards {
		data := append([]byte(nil), shard.Data...)
		data[0] ^= 0xff
		if err := store.PutFragment(storage.Fragment{
			StripeID:   meta.StripeID,
			ShardIndex: shard.Index,
			Data:       data,
			// Keep the original checksum so every fetch rejects the fragment.
			Checksum: shard.Checksum,
			IsParity: shard.IsParity,
		}); err != nil {
			t.Fatalf("seed corrupt shard %d: %v", shard.Index, err)
		}
	}

	_, err := srv.read(context.Background(), &pb.GetRequest{Key: "corrupt-all"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("all-corrupt read = %v, want FailedPrecondition", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("corrupt")) {
		t.Fatalf("error does not identify corrupt shards: %v", err)
	}
}

func TestReadChecksumMismatchWithEnoughShards(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	want := bytes.Repeat([]byte("the-real-value-"), 37)
	shardSize := (len(want) + redundancy.RSDataShards - 1) / redundancy.RSDataShards

	meta := storage.KeyMeta{
		Key:       "mismatch",
		StripeID:  "stripe-mismatch",
		Mode:      string(redundancy.ModeRS32),
		Size:      int64(len(want)),
		Checksum:  redundancy.Checksum(want),
		Version:   1,
		Placement: localPlacements(),
	}
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	for i := 0; i < redundancy.RSDataShards; i++ {
		data := bytes.Repeat([]byte{byte('a' + i)}, shardSize)
		if err := store.PutFragment(storage.Fragment{
			StripeID:   meta.StripeID,
			ShardIndex: i,
			Data:       data,
			Checksum:   redundancy.Checksum(data),
			IsParity:   i >= redundancy.RSDataShards,
		}); err != nil {
			t.Fatalf("seed shard %d: %v", i, err)
		}
	}

	value, _, err := srv.readVerified(context.Background(), meta)
	if !errors.Is(err, ErrDataLoss) {
		t.Fatalf("readVerified = %v, want ErrDataLoss", err)
	}
	if value != nil {
		t.Fatal("readVerified returned bytes for a checksum mismatch")
	}
	if status.Code(readStatusError(err)) != codes.DataLoss {
		t.Fatalf("DataLoss mapping = %v", readStatusError(err))
	}
}

func TestReadMetadataTimeoutMapsToDeadlineExceeded(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	srv.apply = func([]byte, time.Duration) (interface{}, error) {
		return nil, context.DeadlineExceeded
	}

	_, err := srv.read(context.Background(), &pb.GetRequest{Key: "timeout"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("metadata timeout = %v, want DeadlineExceeded", err)
	}
}

func TestReadStatusErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "not found", err: storage.ErrNotFound, want: codes.NotFound},
		{name: "insufficient shards", err: ErrInsufficientShards, want: codes.FailedPrecondition},
		{name: "data loss", err: ErrDataLoss, want: codes.DataLoss},
		{name: "deadline", err: context.DeadlineExceeded, want: codes.DeadlineExceeded},
		{name: "internal", err: errors.New("disk exploded"), want: codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.Code(readStatusError(tc.err)); got != tc.want {
				t.Fatalf("status = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadQueueSaturationDoesNotFailRead(t *testing.T) {
	store := newTestStore()
	srv := newReadTestServer(store)
	value := bytes.Repeat([]byte("queue-full-"), 31)
	shards := encodeForTest(t, redundancy.ModeRS32, value)
	meta := seedReadStripe(t, store, "queue", string(redundancy.ModeRS32), value, shards[:3])
	if _, err := store.ApplyMetaStatus(2, meta.Key, meta.StripeID, meta.Version, true); err != nil {
		t.Fatalf("mark degraded: %v", err)
	}

	block := make(chan struct{})
	started := make(chan struct{})
	var startedOnce sync.Once
	queue := cluster.NewRepairQueue(1, 1, time.Minute, func(context.Context, cluster.RepairTask) error {
		startedOnce.Do(func() { close(started) })
		<-block
		return nil
	})
	srv.repairQueue = queue
	defer func() {
		close(block)
		queue.Close()
	}()

	if err := queue.Enqueue(cluster.RepairTask{Key: "first"}); err != nil {
		t.Fatalf("prefill queue: %v", err)
	}
	<-started
	if err := queue.Enqueue(cluster.RepairTask{Key: "second"}); err != nil {
		t.Fatalf("fill queue: %v", err)
	}

	resp, err := srv.read(context.Background(), &pb.GetRequest{Key: "queue"})
	if err != nil {
		t.Fatalf("read with a full repair queue: %v", err)
	}
	if !bytes.Equal(resp.GetValue(), value) {
		t.Fatal("read with a full repair queue returned the wrong value")
	}
	if resp.GetRepaired() {
		t.Fatal("read claimed a repair was queued even though the queue was full")
	}
}
