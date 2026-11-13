package consensus

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"distributed-kv-store/internal/storage"
)

type testStoreSnapshot struct {
	Metas   []storage.KeyMeta      `json:"metas"`
	Config  *storage.ClusterConfig `json:"config,omitempty"`
	Applied uint64                 `json:"applied"`
}

// testStore is a minimal in-memory metadata store used by consensus tests. The
// embedded interface supplies fragment and stats methods the FSM never calls.
// A test-local double keeps these tests independent of tests/support.
type testStore struct {
	storage.Store

	mu        sync.RWMutex
	metas     map[string]storage.KeyMeta
	applied   uint64
	config    storage.ClusterConfig
	hasConfig bool
}

func newTestStore() *testStore {
	return &testStore{metas: make(map[string]storage.KeyMeta)}
}

func (s *testStore) Ping() error { return nil }

func (s *testStore) ApplyPutMeta(raftIndex uint64, meta storage.KeyMeta) (storage.KeyMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if raftIndex <= s.applied {
		return storage.KeyMeta{}, nil
	}

	old := s.metas[meta.Key]
	if old.Key != "" {
		switch {
		case meta.Version < old.Version:
			return storage.KeyMeta{}, storage.ErrConflict
		case meta.Version == old.Version &&
			(meta.StripeID != old.StripeID || meta.Checksum != old.Checksum || meta.Size != old.Size):
			return storage.KeyMeta{}, storage.ErrConflict
		}
	}

	meta.Placement = append([]storage.ShardPlacement(nil), meta.Placement...)
	s.metas[meta.Key] = meta
	s.applied = raftIndex
	return old, nil
}

func (s *testStore) ApplyMetaStatus(raftIndex uint64, key, stripeID string, version int64, degraded bool) (storage.KeyMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if raftIndex <= s.applied {
		return storage.KeyMeta{}, nil
	}
	current, ok := s.metas[key]
	if !ok {
		return storage.KeyMeta{}, fmt.Errorf("%w: key %q", storage.ErrNotFound, key)
	}
	if current.StripeID != stripeID || current.Version != version {
		return storage.KeyMeta{}, storage.ErrConflict
	}
	current.Degraded = degraded
	s.metas[key] = current
	s.applied = raftIndex
	return current, nil
}

func (s *testStore) ApplyDeleteMeta(raftIndex uint64, key string, expectedVersion int64) (storage.KeyMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if raftIndex <= s.applied {
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

func (s *testStore) ApplySetConfig(raftIndex uint64, cfg storage.ClusterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if raftIndex <= s.applied {
		return nil
	}
	s.config = cfg
	s.hasConfig = true
	s.applied = raftIndex
	return nil
}

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

func (s *testStore) ListKeys() ([]storage.KeyMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]storage.KeyMeta, 0, len(s.metas))
	for _, meta := range s.metas {
		meta.Placement = append([]storage.ShardPlacement(nil), meta.Placement...)
		keys = append(keys, meta)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
	return keys, nil
}

func (s *testStore) LastAppliedIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied, nil
}

func (s *testStore) GetConfig() (storage.ClusterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasConfig {
		return storage.ClusterConfig{}, storage.ErrNotFound
	}
	return s.config, nil
}

func (s *testStore) PutConfig(cfg storage.ClusterConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = cfg
	s.hasConfig = true
	return nil
}

// SnapshotMetadataPayload serializes metadata for the FSM envelope.
func (s *testStore) SnapshotMetadataPayload() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	payload := testStoreSnapshot{Applied: s.applied}
	for _, meta := range s.metas {
		payload.Metas = append(payload.Metas, meta)
	}
	sort.Slice(payload.Metas, func(i, j int) bool { return payload.Metas[i].Key < payload.Metas[j].Key })
	if s.hasConfig {
		cfg := s.config
		payload.Config = &cfg
	}
	return json.Marshal(payload)
}

// RestoreMetadataPayload replaces metadata from the FSM envelope.
func (s *testStore) RestoreMetadataPayload(data []byte) error {
	var payload testStoreSnapshot
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.metas = make(map[string]storage.KeyMeta, len(payload.Metas))
	for _, meta := range payload.Metas {
		s.metas[meta.Key] = meta
	}
	s.applied = payload.Applied
	s.hasConfig = payload.Config != nil
	if payload.Config != nil {
		s.config = *payload.Config
	}
	return nil
}

func (s *testStore) Close() error { return nil }
