package storage_test

import (
	"encoding/json"
	"testing"

	"distributed-kv-store/internal/storage"
)

func TestSQLiteMetadataSnapshotRestore(t *testing.T) {
	source := newStore(t)
	first := testMeta("alpha", "stripe-1", 1, []byte("one"), "rs32")
	second := testMeta("beta", "stripe-2", 2, []byte("two"), "replication")
	if _, err := source.ApplyPutMeta(3, first); err != nil {
		t.Fatalf("ApplyPutMeta alpha: %v", err)
	}
	if _, err := source.ApplyPutMeta(4, second); err != nil {
		t.Fatalf("ApplyPutMeta beta: %v", err)
	}
	if err := source.ApplySetConfig(5, storage.ClusterConfig{DefaultMode: "rs32"}); err != nil {
		t.Fatalf("ApplySetConfig: %v", err)
	}
	if err := source.PutFragment(storage.Fragment{StripeID: "stripe-1", ShardIndex: 0, Data: []byte("shard")}); err != nil {
		t.Fatalf("PutFragment: %v", err)
	}

	snapshot, err := source.SnapshotMetadata()
	if err != nil {
		t.Fatalf("SnapshotMetadata: %v", err)
	}

	target := newStore(t)
	if err := target.RestoreMetadata(snapshot); err != nil {
		t.Fatalf("RestoreMetadata: %v", err)
	}

	for _, want := range []storage.KeyMeta{first, second} {
		got, err := target.GetKey(want.Key)
		if err != nil {
			t.Fatalf("GetKey(%q): %v", want.Key, err)
		}
		if got.StripeID != want.StripeID || got.Mode != want.Mode || got.Checksum != want.Checksum ||
			got.Version != want.Version || len(got.Placement) != 5 {
			t.Fatalf("GetKey(%q) = %+v, want %+v", want.Key, got, want)
		}
	}

	cfg, err := target.GetConfig()
	if err != nil || cfg.DefaultMode != "rs32" {
		t.Fatalf("GetConfig = %+v, %v", cfg, err)
	}
	index, err := target.LastAppliedIndex()
	if err != nil || index != 5 {
		t.Fatalf("LastAppliedIndex = %d, %v", index, err)
	}

	stats, err := target.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.KeyCount != 2 || stats.StripeCount != 2 {
		t.Fatalf("metadata counts = %+v", stats)
	}
	if stats.FragmentCount != 0 {
		t.Fatalf("metadata snapshot included fragments: %+v", stats)
	}
}

func TestMetadataSnapshotEmptyStore(t *testing.T) {
	source := newStore(t)
	snapshot, err := source.SnapshotMetadata()
	if err != nil {
		t.Fatalf("SnapshotMetadata: %v", err)
	}

	target := newStore(t)
	if err := target.RestoreMetadata(snapshot); err != nil {
		t.Fatalf("RestoreMetadata: %v", err)
	}
	keys, err := target.ListKeys()
	if err != nil || len(keys) != 0 {
		t.Fatalf("ListKeys = %+v, %v", keys, err)
	}
}

func TestMetadataRestoreIsAtomic(t *testing.T) {
	store := newStore(t)
	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "rs32")
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta: %v", err)
	}

	// This payload references a key whose stripe row is missing. The foreign
	// key must abort the restore and leave the previous state in place.
	bad := map[string]any{
		"schema_version": 1,
		"kind":           "metadata",
		"payload": map[string]any{
			"stripes": []any{},
			"keys": []any{map[string]any{
				"key":       "broken",
				"stripe_id": "missing-stripe",
				"mode":      "rs32",
				"size":      1,
				"checksum":  digest([]byte("x")),
				"version":   1,
				"degraded":  false,
				"placement": testPlacements("rs32"),
			}},
			"last_applied_index": 9,
		},
	}
	encoded, err := json.Marshal(bad)
	if err != nil {
		t.Fatalf("marshal bad snapshot: %v", err)
	}
	if err := store.RestoreMetadata(encoded); err == nil {
		t.Fatal("restore with a dangling key succeeded")
	}

	if _, err := store.GetKey("alpha"); err != nil {
		t.Fatalf("previous metadata was lost: %v", err)
	}
	index, err := store.LastAppliedIndex()
	if err != nil || index != 1 {
		t.Fatalf("applied index changed after failed restore: %d, %v", index, err)
	}
}

func TestMetadataSnapshotRejectsUnknownVersion(t *testing.T) {
	store := newStore(t)
	unknown := []byte(`{"schema_version":2,"kind":"metadata","payload":{}}`)
	if err := store.RestoreMetadata(unknown); err == nil {
		t.Fatal("unknown metadata snapshot version was accepted")
	}
}
