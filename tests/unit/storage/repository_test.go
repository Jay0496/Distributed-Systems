package storage_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/storage"
)

func newStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()

	store, err := storage.NewSQLiteStore(filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testPlacements(mode string) []storage.ShardPlacement {
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	placements := make([]storage.ShardPlacement, 0, 5)
	for i, node := range nodes {
		placements = append(placements, storage.ShardPlacement{
			ShardIndex: i,
			NodeID:     node,
			IsParity:   mode == "rs32" && i >= 3,
		})
	}
	return placements
}

func testMeta(key, stripe string, version int64, value []byte, mode string) storage.KeyMeta {
	return storage.KeyMeta{
		Key:       key,
		StripeID:  stripe,
		Mode:      mode,
		Size:      int64(len(value)),
		Checksum:  digest(value),
		Version:   version,
		Placement: testPlacements(mode),
	}
}

func TestMetadataRoundTrip(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "rs32")

	old, err := store.ApplyPutMeta(10, meta)
	if err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}
	if old.Key != "" {
		t.Fatalf("new key returned old metadata %+v", old)
	}

	got, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if got.Key != meta.Key || got.StripeID != meta.StripeID || got.Mode != meta.Mode ||
		got.Size != meta.Size || got.Checksum != meta.Checksum || got.Version != meta.Version ||
		got.Degraded != meta.Degraded || len(got.Placement) != 5 {
		t.Fatalf("GetKey = %+v, want %+v", got, meta)
	}

	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 1 || keys[0].Key != "alpha" {
		t.Fatalf("ListKeys = %+v", keys)
	}

	index, err := store.LastAppliedIndex()
	if err != nil {
		t.Fatalf("LastAppliedIndex: %v", err)
	}
	if index != 10 {
		t.Fatalf("LastAppliedIndex = %d, want 10", index)
	}
}

func TestApplyPutMetaOverwriteReturnsOldMetadata(t *testing.T) {
	store := newStore(t)
	first := testMeta("alpha", "stripe-1", 1, []byte("one"), "replication")
	second := testMeta("alpha", "stripe-2", 2, []byte("two"), "rs32")

	if _, err := store.ApplyPutMeta(1, first); err != nil {
		t.Fatalf("first ApplyPutMeta: %v", err)
	}
	old, err := store.ApplyPutMeta(2, second)
	if err != nil {
		t.Fatalf("second ApplyPutMeta: %v", err)
	}
	if old.Key != "alpha" || old.StripeID != "stripe-1" || old.Version != 1 || len(old.Placement) != 5 {
		t.Fatalf("old metadata = %+v", old)
	}

	got, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if got.StripeID != "stripe-2" || got.Version != 2 {
		t.Fatalf("current metadata = %+v", got)
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.StripeCount != 1 {
		t.Fatalf("StripeCount = %d, want 1 (old stripe should be removed)", stats.StripeCount)
	}
}

func TestApplyPutMetaReplayIsNoOp(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "rs32")

	if _, err := store.ApplyPutMeta(5, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}

	old, err := store.ApplyPutMeta(5, meta)
	if err != nil {
		t.Fatalf("replay ApplyPutMeta: %v", err)
	}
	if old.Key != "" {
		t.Fatalf("replay returned old metadata %+v, want empty", old)
	}

	// An older index must not mutate the current state either.
	newer := testMeta("alpha", "stripe-2", 2, []byte("new"), "rs32")
	if _, err := store.ApplyPutMeta(4, newer); err != nil {
		t.Fatalf("stale ApplyPutMeta: %v", err)
	}

	got, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if got.StripeID != "stripe-1" || got.Version != 1 {
		t.Fatalf("stale command mutated metadata: %+v", got)
	}
}

func TestApplyPutMetaVersionConflicts(t *testing.T) {
	store := newStore(t)
	current := testMeta("alpha", "stripe-2", 2, []byte("two"), "rs32")
	if _, err := store.ApplyPutMeta(1, current); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}

	older := testMeta("alpha", "stripe-1", 1, []byte("one"), "rs32")
	if _, err := store.ApplyPutMeta(2, older); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("older version error = %v, want ErrConflict", err)
	}

	different := current
	different.Checksum = digest([]byte("different"))
	if _, err := store.ApplyPutMeta(2, different); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("same version with different checksum error = %v, want ErrConflict", err)
	}
}

func TestApplyPutMetaValidatesMetadata(t *testing.T) {
	store := newStore(t)

	cases := map[string]storage.KeyMeta{
		"empty key":       testMeta("", "stripe", 1, []byte("v"), "rs32"),
		"empty stripe":    testMeta("key", "", 1, []byte("v"), "rs32"),
		"bad mode":        testMeta("key", "stripe", 1, []byte("v"), "triple"),
		"bad version":     testMeta("key", "stripe", 0, []byte("v"), "rs32"),
		"bad checksum":    {Key: "key", StripeID: "stripe", Mode: "rs32", Size: 1, Checksum: "short", Version: 1, Placement: testPlacements("rs32")},
		"few placements":  {Key: "key", StripeID: "stripe", Mode: "rs32", Size: 1, Checksum: digest([]byte("v")), Version: 1},
		"bad parity flag": {Key: "key", StripeID: "stripe", Mode: "replication", Size: 1, Checksum: digest([]byte("v")), Version: 1, Placement: testPlacements("rs32")},
	}

	for name, meta := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := store.ApplyPutMeta(1, meta); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestApplyMetaStatusPreservesVersion(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 3, []byte("value"), "rs32")
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}

	updated, err := store.ApplyMetaStatus(2, "alpha", "stripe-1", 3, true)
	if err != nil {
		t.Fatalf("ApplyMetaStatus: %v", err)
	}
	if !updated.Degraded || updated.Version != 3 {
		t.Fatalf("updated metadata = %+v", updated)
	}

	got, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if !got.Degraded || got.Version != 3 {
		t.Fatalf("stored metadata = %+v", got)
	}

	if _, err := store.ApplyMetaStatus(3, "alpha", "stripe-1", 2, false); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("wrong version error = %v, want ErrConflict", err)
	}
}

func TestApplyDeleteMeta(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "rs32")
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}

	old, err := store.ApplyDeleteMeta(2, "alpha", 1)
	if err != nil {
		t.Fatalf("ApplyDeleteMeta: %v", err)
	}
	if old.Key != "alpha" || old.StripeID != "stripe-1" {
		t.Fatalf("deleted metadata = %+v", old)
	}
	if _, err := store.GetKey("alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetKey after delete = %v, want ErrNotFound", err)
	}

	// Deleting a missing key is idempotent even with an expected version.
	if old, err := store.ApplyDeleteMeta(3, "missing", 99); err != nil || old.Key != "" {
		t.Fatalf("idempotent delete = %+v, %v", old, err)
	}

	if _, err := store.ApplyPutMeta(4, meta); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if _, err := store.ApplyDeleteMeta(5, "alpha", 99); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected-version mismatch = %v, want ErrConflict", err)
	}
	if _, err := store.GetKey("alpha"); err != nil {
		t.Fatalf("key should remain after rejected delete: %v", err)
	}
}

func TestConfigSingleton(t *testing.T) {
	store := newStore(t)

	if _, err := store.GetConfig(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetConfig on empty store = %v, want ErrNotFound", err)
	}
	if err := store.ApplySetConfig(1, storage.ClusterConfig{DefaultMode: "replication"}); err != nil {
		t.Fatalf("ApplySetConfig: %v", err)
	}
	cfg, err := store.GetConfig()
	if err != nil || cfg.DefaultMode != "replication" {
		t.Fatalf("GetConfig = %+v, %v", cfg, err)
	}

	if err := store.PutConfig(storage.ClusterConfig{DefaultMode: "rs32"}); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	cfg, err = store.GetConfig()
	if err != nil || cfg.DefaultMode != "rs32" {
		t.Fatalf("GetConfig after PutConfig = %+v, %v", cfg, err)
	}
	index, err := store.LastAppliedIndex()
	if err != nil || index != 1 {
		t.Fatalf("LastAppliedIndex = %d, %v; PutConfig must not advance it", index, err)
	}

	if err := store.ApplySetConfig(2, storage.ClusterConfig{DefaultMode: "triple"}); err == nil {
		t.Fatal("invalid mode was accepted")
	}
}

func TestFragmentRepository(t *testing.T) {
	store := newStore(t)
	data := []byte("fragment-bytes")
	created := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	frag := storage.Fragment{
		StripeID:   "stripe-1",
		ShardIndex: 2,
		IsParity:   false,
		Data:       data,
		Checksum:   digest(data),
		CreatedAt:  created,
	}

	if err := store.PutFragment(frag); err != nil {
		t.Fatalf("PutFragment: %v", err)
	}
	got, err := store.GetFragment("stripe-1", 2)
	if err != nil {
		t.Fatalf("GetFragment: %v", err)
	}
	if string(got.Data) != string(data) || got.Checksum != frag.Checksum || !got.CreatedAt.Equal(created) {
		t.Fatalf("GetFragment = %+v", got)
	}

	refs, err := store.ListFragments("stripe-1")
	if err != nil {
		t.Fatalf("ListFragments: %v", err)
	}
	if len(refs) != 1 || refs[0].Size != int64(len(data)) || refs[0].ShardIndex != 2 {
		t.Fatalf("ListFragments = %+v", refs)
	}

	// An empty stripe filter lists every local fragment for repair.
	if err := store.PutFragment(storage.Fragment{StripeID: "stripe-2", ShardIndex: 1, Data: []byte("second")}); err != nil {
		t.Fatalf("PutFragment for stripe-2: %v", err)
	}
	all, err := store.ListFragments("")
	if err != nil {
		t.Fatalf("ListFragments(all): %v", err)
	}
	if len(all) != 2 || all[0].StripeID != "stripe-1" || all[1].StripeID != "stripe-2" {
		t.Fatalf("ListFragments(all) = %+v", all)
	}
	if err := store.DeleteFragment("stripe-2", 1); err != nil {
		t.Fatalf("DeleteFragment stripe-2: %v", err)
	}

	// Updating the payload preserves created_at.
	updated := []byte("updated")
	if err := store.PutFragment(storage.Fragment{
		StripeID: "stripe-1", ShardIndex: 2, Data: updated, Checksum: digest(updated),
	}); err != nil {
		t.Fatalf("update PutFragment: %v", err)
	}
	got, err = store.GetFragment("stripe-1", 2)
	if err != nil {
		t.Fatalf("GetFragment after update: %v", err)
	}
	if string(got.Data) != "updated" || !got.CreatedAt.Equal(created) {
		t.Fatalf("updated fragment = %+v", got)
	}

	if err := store.PutFragment(storage.Fragment{StripeID: "s", ShardIndex: 5, Data: []byte("x")}); err == nil {
		t.Fatal("invalid shard index was accepted")
	}
	if err := store.PutFragment(storage.Fragment{StripeID: "s", ShardIndex: 0, Data: []byte("x"), Checksum: digest([]byte("y"))}); err == nil {
		t.Fatal("checksum mismatch was accepted")
	}
	if _, err := store.GetFragment("stripe-1", 4); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing fragment error = %v, want ErrNotFound", err)
	}
	if err := store.DeleteFragment("stripe-1", 2); err != nil {
		t.Fatalf("DeleteFragment: %v", err)
	}
	if err := store.DeleteFragment("stripe-1", 2); err != nil {
		t.Fatalf("idempotent DeleteFragment: %v", err)
	}
}

func TestDeleteStripeRemovesMetadataAndFragments(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "rs32")
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}
	frag := storage.Fragment{StripeID: "stripe-1", ShardIndex: 0, Data: []byte("shard")}
	if err := store.PutFragment(frag); err != nil {
		t.Fatalf("PutFragment: %v", err)
	}

	if err := store.DeleteStripe("stripe-1"); err != nil {
		t.Fatalf("DeleteStripe: %v", err)
	}
	if _, err := store.GetKey("alpha"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetKey after DeleteStripe = %v, want ErrNotFound", err)
	}
	refs, err := store.ListFragments("stripe-1")
	if err != nil {
		t.Fatalf("ListFragments: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("fragments remain after DeleteStripe: %+v", refs)
	}
	if err := store.DeleteStripe("missing"); err != nil {
		t.Fatalf("idempotent DeleteStripe: %v", err)
	}
}

func TestStatsCountsAndBytes(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "replication")
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}
	payload := []byte("fragment payload")
	if err := store.PutFragment(storage.Fragment{StripeID: "stripe-1", ShardIndex: 0, Data: payload}); err != nil {
		t.Fatalf("PutFragment: %v", err)
	}
	if err := store.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.KeyCount != 1 || stats.StripeCount != 1 || stats.FragmentCount != 1 {
		t.Fatalf("Stats counts = %+v", stats)
	}
	if stats.FragmentBytes != int64(len(payload)) {
		t.Fatalf("FragmentBytes = %d, want %d", stats.FragmentBytes, len(payload))
	}
	if stats.Bytes <= 0 {
		t.Fatalf("Bytes = %d, want a positive file size", stats.Bytes)
	}
}

func TestConcurrentFragmentWriters(t *testing.T) {
	store := newStore(t)

	const writers = 20
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("fragment-%d", i))
			if err := store.PutFragment(storage.Fragment{
				StripeID:   fmt.Sprintf("stripe-%d", i),
				ShardIndex: i % 5,
				Data:       data,
			}); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent PutFragment: %v", err)
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.FragmentCount != writers {
		t.Fatalf("FragmentCount = %d, want %d", stats.FragmentCount, writers)
	}
}

func TestStoreClosesCleanly(t *testing.T) {
	store := newStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, err := store.GetKey("alpha"); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("GetKey after Close = %v, want ErrClosed", err)
	}
	if err := store.Put("alpha", []byte("v"), false); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("Put after Close = %v, want ErrClosed", err)
	}
}

func TestLegacyGetMissingKeyIsNotFound(t *testing.T) {
	store := newStore(t)
	if _, err := store.Get("missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}
}
