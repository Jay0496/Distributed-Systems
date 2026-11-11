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
	metas     map[string]storage.KeyMeta
	applied   uint64
	config    storage.ClusterConfig
	hasConfig bool
}

func newTestStore() *testStore {
	return &testStore{
		data:      make(map[string][]byte),
		fragments: make(map[string]map[int]storage.Fragment),
		metas:     make(map[string]storage.KeyMeta),
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

// LastAppliedIndex reports the highest metadata index applied by the test FSM.
func (s *testStore) LastAppliedIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied, nil
}

// ApplyPutMeta mirrors the SQLite version-conflict and replay behavior closely
// enough for server component tests.
func (s *testStore) ApplyPutMeta(raftIndex uint64, meta storage.KeyMeta) (storage.KeyMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if raftIndex != 0 && raftIndex <= s.applied {
		return storage.KeyMeta{}, nil
	}

	old, exists := s.metas[meta.Key]
	if exists {
		switch {
		case meta.Version < old.Version:
			return storage.KeyMeta{}, storage.ErrConflict
		case meta.Version == old.Version:
			if meta.StripeID != old.StripeID || meta.Checksum != old.Checksum || meta.Size != old.Size {
				return storage.KeyMeta{}, storage.ErrConflict
			}
			s.applied = raftIndex
			return old, nil
		}
	}

	meta.Placement = append([]storage.ShardPlacement(nil), meta.Placement...)
	s.metas[meta.Key] = meta
	s.applied = raftIndex
	return old, nil
}

// ApplyMetaStatus updates only the degraded flag when stripe and version match.
func (s *testStore) ApplyMetaStatus(raftIndex uint64, key, stripeID string, version int64, degraded bool) (storage.KeyMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if raftIndex != 0 && raftIndex <= s.applied {
		return storage.KeyMeta{}, nil
	}
	current, ok := s.metas[key]
	if !ok {
		return storage.KeyMeta{}, storage.ErrNotFound
	}
	if current.StripeID != stripeID || current.Version != version {
		return storage.KeyMeta{}, storage.ErrConflict
	}
	current.Degraded = degraded
	s.metas[key] = current
	s.applied = raftIndex
	return current, nil
}

// ApplyDeleteMeta removes a key when the expected version matches.
func (s *testStore) ApplyDeleteMeta(raftIndex uint64, key string, expectedVersion int64) (storage.KeyMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if raftIndex != 0 && raftIndex <= s.applied {
		return storage.KeyMeta{}, nil
	}
	old, ok := s.metas[key]
	if !ok {
		s.applied = raftIndex
		return storage.KeyMeta{}, nil
	}
	if expectedVersion != 0 && expectedVersion != old.Version {
		return storage.KeyMeta{}, storage.ErrConflict
	}
	delete(s.metas, key)
	s.applied = raftIndex
	return old, nil
}

// GetKey returns the committed metadata for key.
func (s *testStore) GetKey(key string) (storage.KeyMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.metas[key]
	if !ok {
		return storage.KeyMeta{}, fmt.Errorf("%w: key %q", storage.ErrNotFound, key)
	}
	meta.Placement = append([]storage.ShardPlacement(nil), meta.Placement...)
	return meta, nil
}

// ApplySetConfig stores the singleton cluster configuration.
func (s *testStore) ApplySetConfig(raftIndex uint64, cfg storage.ClusterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if raftIndex != 0 && raftIndex <= s.applied {
		return nil
	}
	s.config = cfg
	s.hasConfig = true
	s.applied = raftIndex
	return nil
}

// GetConfig returns the singleton cluster configuration.
func (s *testStore) GetConfig() (storage.ClusterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasConfig {
		return storage.ClusterConfig{}, fmt.Errorf("%w: cluster config", storage.ErrNotFound)
	}
	return s.config, nil
}
