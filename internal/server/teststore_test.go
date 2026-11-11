package server

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"distributed-kv-store/internal/storage"
)

// testStore is a minimal in-memory Store used by the server component tests.
// It implements the legacy KV methods and the fragment repository; the
// remaining Store methods are supplied by the embedded interface.
type testStore struct {
	storage.Store

	mu        sync.RWMutex
	data      map[string][]byte
	fragments map[string]map[int]storage.Fragment
}

func newTestStore() *testStore {
	return &testStore{
		data:      make(map[string][]byte),
		fragments: make(map[string]map[int]storage.Fragment),
	}
}

func (s *testStore) Put(key string, value []byte, _ bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(value))
	copy(copied, value)
	s.data[key] = copied
	return nil
}

func (s *testStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("%w: key %q", storage.ErrNotFound, key)
	}
	copied := make([]byte, len(value))
	copy(copied, value)
	return copied, nil
}

func (s *testStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.data)
}

func (s *testStore) Restore(data []byte) error {
	var restored map[string][]byte
	if err := json.Unmarshal(data, &restored); err != nil {
		return err
	}
	if restored == nil {
		restored = make(map[string][]byte)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = restored
	return nil
}

// PutFragment stores a fragment without verifying its checksum so tests can
// simulate corrupt local state.
func (s *testStore) PutFragment(frag storage.Fragment) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if frag.ShardIndex < 0 || frag.ShardIndex > 4 {
		return fmt.Errorf("invalid shard index %d", frag.ShardIndex)
	}
	if s.fragments[frag.StripeID] == nil {
		s.fragments[frag.StripeID] = make(map[int]storage.Fragment)
	}
	frag.Data = append([]byte(nil), frag.Data...)
	s.fragments[frag.StripeID][frag.ShardIndex] = frag
	return nil
}

func (s *testStore) GetFragment(stripeID string, shardIndex int) (storage.Fragment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	frag, ok := s.fragments[stripeID][shardIndex]
	if !ok {
		return storage.Fragment{}, fmt.Errorf("%w: fragment %q/%d", storage.ErrNotFound, stripeID, shardIndex)
	}
	frag.Data = append([]byte(nil), frag.Data...)
	return frag, nil
}

func (s *testStore) DeleteFragment(stripeID string, shardIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.fragments[stripeID], shardIndex)
	return nil
}

func (s *testStore) DeleteStripe(stripeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.fragments, stripeID)
	return nil
}

func (s *testStore) ListFragments(stripeID string) ([]storage.FragmentRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var refs []storage.FragmentRef
	for id, shards := range s.fragments {
		if stripeID != "" && id != stripeID {
			continue
		}
		for _, frag := range shards {
			refs = append(refs, storage.FragmentRef{
				StripeID:   id,
				ShardIndex: frag.ShardIndex,
				IsParity:   frag.IsParity,
				Size:       int64(len(frag.Data)),
				Checksum:   frag.Checksum,
				CreatedAt:  frag.CreatedAt,
			})
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].StripeID != refs[j].StripeID {
			return refs[i].StripeID < refs[j].StripeID
		}
		return refs[i].ShardIndex < refs[j].ShardIndex
	})
	return refs, nil
}

func (s *testStore) Close() error { return nil }
