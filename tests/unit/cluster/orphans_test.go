package cluster_test

import (
	"context"
	"testing"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/storage"
)

type orphanFragmentKey struct {
	stripeID   string
	shardIndex int
}

type orphanTestStore struct {
	storage.Store
	refs    []storage.FragmentRef
	metas   []storage.KeyMeta
	deleted []orphanFragmentKey
}

func (s *orphanTestStore) ListFragments(string) ([]storage.FragmentRef, error) {
	return append([]storage.FragmentRef(nil), s.refs...), nil
}

func (s *orphanTestStore) ListKeys() ([]storage.KeyMeta, error) {
	return append([]storage.KeyMeta(nil), s.metas...), nil
}

func (s *orphanTestStore) DeleteFragment(stripeID string, shardIndex int) error {
	s.deleted = append(s.deleted, orphanFragmentKey{stripeID: stripeID, shardIndex: shardIndex})
	return nil
}

func orphanMeta(stripeID string, placements map[int]string) storage.KeyMeta {
	meta := storage.KeyMeta{
		Key:      "key-" + stripeID,
		StripeID: stripeID,
		Mode:     "rs32",
		Size:     1,
		Checksum: "0000000000000000000000000000000000000000000000000000000000000000",
		Version:  1,
	}
	for index, node := range placements {
		meta.Placement = append(meta.Placement, storage.ShardPlacement{ShardIndex: index, NodeID: node})
	}
	return meta
}

func TestOrphanSweeperDeletesUnreferencedOldFragment(t *testing.T) {
	now := time.Now()
	store := &orphanTestStore{refs: []storage.FragmentRef{{
		StripeID: "gone", ShardIndex: 0, CreatedAt: now.Add(-time.Hour),
	}}}
	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       store,
		SelfNode:    "node1",
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...any) {},
	})

	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Deleted != 1 || len(store.deleted) != 1 {
		t.Fatalf("result = %+v deleted = %+v", result, store.deleted)
	}
}

func TestOrphanSweeperKeepsRecentFragment(t *testing.T) {
	now := time.Now()
	store := &orphanTestStore{refs: []storage.FragmentRef{{
		StripeID: "in-flight", ShardIndex: 0, CreatedAt: now.Add(-time.Second),
	}}}
	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       store,
		SelfNode:    "node1",
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...any) {},
	})

	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Deleted != 0 || len(store.deleted) != 0 {
		t.Fatalf("recent fragment was deleted: %+v", store.deleted)
	}
}

func TestOrphanSweeperKeepsReferencedFragment(t *testing.T) {
	now := time.Now()
	store := &orphanTestStore{
		refs: []storage.FragmentRef{{
			StripeID: "live", ShardIndex: 0, CreatedAt: now.Add(-time.Hour),
		}},
		metas: []storage.KeyMeta{orphanMeta("live", map[int]string{0: "node1"})},
	}
	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       store,
		SelfNode:    "node1",
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...any) {},
	})

	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Deleted != 0 || len(store.deleted) != 0 {
		t.Fatalf("referenced fragment was deleted: %+v", store.deleted)
	}
}

func TestOrphanSweeperDeletesFragmentPlacedOnAnotherNode(t *testing.T) {
	now := time.Now()
	store := &orphanTestStore{
		refs: []storage.FragmentRef{{
			StripeID: "moved", ShardIndex: 0, CreatedAt: now.Add(-time.Hour),
		}},
		metas: []storage.KeyMeta{orphanMeta("moved", map[int]string{0: "node2"})},
	}
	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       store,
		SelfNode:    "node1",
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...any) {},
	})

	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Deleted != 1 {
		t.Fatalf("result = %+v, want one deletion", result)
	}
}

func TestOrphanSweeperSkipsWhenGuardVetoes(t *testing.T) {
	now := time.Now()
	store := &orphanTestStore{refs: []storage.FragmentRef{{
		StripeID: "gone", ShardIndex: 0, CreatedAt: now.Add(-time.Hour),
	}}}
	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       store,
		SelfNode:    "node1",
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Guard:       func(context.Context) (bool, error) { return false, nil },
		Logf:        func(string, ...any) {},
	})

	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if !result.Skipped || result.Deleted != 0 {
		t.Fatalf("result = %+v, want skipped with no deletions", result)
	}
}

func TestOrphanSweeperKeepsFragmentAfterMetadataCommits(t *testing.T) {
	now := time.Now()
	store := &orphanTestStore{refs: []storage.FragmentRef{{
		StripeID: "late", ShardIndex: 0, CreatedAt: now.Add(-time.Hour),
	}}}
	sweeper := cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
		Store:       store,
		SelfNode:    "node1",
		GracePeriod: time.Minute,
		Now:         func() time.Time { return now },
		Logf:        func(string, ...any) {},
	})

	// The write's metadata commits before the sweep reads metadata, so the
	// fragment that looked unreferenced becomes referenced and is kept.
	store.metas = []storage.KeyMeta{orphanMeta("late", map[int]string{0: "node1"})}
	result, err := sweeper.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if result.Deleted != 0 {
		t.Fatalf("fragment with committed metadata was deleted: %+v", result)
	}
}
