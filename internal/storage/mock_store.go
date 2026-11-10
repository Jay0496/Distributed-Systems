package storage

import (
	"encoding/json"
	"errors"
	"sync"
)

// MockStore is an in-memory implementation of KVStore. It is used to test the
// Raft consensus layer independently of the real SQLite persistence layer.
type MockStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMockStore creates a new empty MockStore.
func NewMockStore() *MockStore {
	return &MockStore{
		data: make(map[string][]byte),
	}
}

// Put stores a copy of the value under the given key.
func (m *MockStore) Put(key string, value []byte, isParity bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Copy the bytes so callers cannot mutate our stored data.
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	m.data[key] = valCopy
	return nil
}

// Get returns a copy of the value stored under the given key.
func (m *MockStore) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.data[key]
	if !ok {
		return nil, errors.New("key not found")
	}

	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, nil
}

// Snapshot serializes the entire store as JSON.
func (m *MockStore) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m.data)
}

// Restore replaces the store contents with a JSON snapshot.
func (m *MockStore) Restore(data []byte) error {
	var restored map[string][]byte
	if err := json.Unmarshal(data, &restored); err != nil {
		return err
	}
	if restored == nil {
		restored = make(map[string][]byte)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = restored
	return nil
}
