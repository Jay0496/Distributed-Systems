package storage_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"
)

func TestMockStorePutGet(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Put("alpha", []byte("one"), false); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	got, err := store.Get("alpha")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "one" {
		t.Fatalf("expected %q, got %q", "one", got)
	}
}

func TestMockStoreMissingKey(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.Get("missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMockStoreCopiesValues(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	input := []byte("original")
	if err := store.Put("key", input, false); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	input[0] = 'X'
	got, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("store aliased caller memory: got %q", got)
	}

	got[0] = 'Y'
	again, _ := store.Get("key")
	if string(again) != "original" {
		t.Fatalf("store aliased returned memory: got %q", again)
	}
}

func TestMockStoreSnapshotRestore(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Put("a", []byte("1"), false); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Put("b", []byte("2"), true); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	restored := support.NewMockStore()
	t.Cleanup(func() { _ = restored.Close() })
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := restored.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestMockStoreConcurrentAccess(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			if err := store.Put(key, []byte("value"), false); err != nil {
				t.Errorf("Put(%q) failed: %v", key, err)
				return
			}
			if _, err := store.Get(key); err != nil {
				t.Errorf("Get(%q) failed: %v", key, err)
			}
		}(i)
	}
	wg.Wait()
}
