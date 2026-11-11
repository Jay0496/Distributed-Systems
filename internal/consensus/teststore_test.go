package consensus

import (
	"encoding/json"
	"fmt"
	"sync"

	"distributed-kv-store/internal/storage"
)

// testStore is a minimal in-memory Store used by consensus tests. The embedded
// interface supplies the metadata methods the transitional FSM does not call;
// only the legacy KV and snapshot methods are implemented here. Using a local
// double keeps the test independent of tests/support and its future imports.
type testStore struct {
	storage.Store

	mu   sync.RWMutex
	data map[string][]byte
}

func newTestStore() *testStore {
	return &testStore{data: make(map[string][]byte)}
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

func (s *testStore) Close() error { return nil }
