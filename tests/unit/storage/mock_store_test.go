package storage_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"
)

func TestMockStoreFragmentRoundTrip(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	frag := storage.Fragment{
		StripeID:   "stripe-1",
		ShardIndex: 2,
		Data:       []byte("one"),
		Checksum:   digest([]byte("one")),
	}
	if err := store.PutFragment(frag); err != nil {
		t.Fatalf("PutFragment failed: %v", err)
	}
	got, err := store.GetFragment("stripe-1", 2)
	if err != nil {
		t.Fatalf("GetFragment failed: %v", err)
	}
	if string(got.Data) != "one" {
		t.Fatalf("expected %q, got %q", "one", got.Data)
	}
}

func TestMockStoreMissingFragment(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	if _, err := store.GetFragment("missing", 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMockStoreCopiesFragments(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	input := []byte("original")
	if err := store.PutFragment(storage.Fragment{StripeID: "stripe", ShardIndex: 0, Data: input}); err != nil {
		t.Fatalf("PutFragment failed: %v", err)
	}

	input[0] = 'X'
	got, err := store.GetFragment("stripe", 0)
	if err != nil {
		t.Fatalf("GetFragment failed: %v", err)
	}
	if string(got.Data) != "original" {
		t.Fatalf("store aliased caller memory: got %q", got.Data)
	}

	got.Data[0] = 'Y'
	again, _ := store.GetFragment("stripe", 0)
	if string(again.Data) != "original" {
		t.Fatalf("store aliased returned memory: got %q", again.Data)
	}
}

func TestMockStoreMetadata(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	meta := testMeta("alpha", "stripe-1", 1, []byte("value"), "rs32")
	if _, err := store.ApplyPutMeta(1, meta); err != nil {
		t.Fatalf("ApplyPutMeta failed: %v", err)
	}
	got, err := store.GetKey("alpha")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}
	if got.StripeID != "stripe-1" || len(got.Placement) != 5 {
		t.Fatalf("GetKey = %+v", got)
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
			stripe := fmt.Sprintf("stripe-%d", i)
			if err := store.PutFragment(storage.Fragment{StripeID: stripe, ShardIndex: 0, Data: []byte("value")}); err != nil {
				t.Errorf("PutFragment(%q) failed: %v", stripe, err)
				return
			}
			if _, err := store.GetFragment(stripe, 0); err != nil {
				t.Errorf("GetFragment(%q) failed: %v", stripe, err)
			}
		}(i)
	}
	wg.Wait()
}
