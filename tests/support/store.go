package support

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"distributed-kv-store/internal/storage"
)

// MockStore is an in-memory implementation of storage.Store for tests. It is
// deliberately kept out of production packages; package-internal tests use
// their own minimal doubles to avoid import cycles.
type MockStore struct {
	mu        sync.RWMutex
	data      map[string][]byte
	parity    map[string]bool
	keys      map[string]storage.KeyMeta
	stripes   map[string]struct{}
	fragments map[string]map[int]storage.Fragment
	config    storage.ClusterConfig
	hasConfig bool
	applied   uint64
	closed    bool
}

// NewMockStore creates a new empty MockStore.
func NewMockStore() *MockStore {
	return &MockStore{
		data:      make(map[string][]byte),
		parity:    make(map[string]bool),
		keys:      make(map[string]storage.KeyMeta),
		stripes:   make(map[string]struct{}),
		fragments: make(map[string]map[int]storage.Fragment),
	}
}

var _ storage.Store = (*MockStore)(nil)

func (m *MockStore) ready() error {
	if m == nil || m.closed {
		return storage.ErrClosed
	}
	return nil
}

func copyBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

func copyMeta(meta storage.KeyMeta) storage.KeyMeta {
	out := meta
	out.Placement = append([]storage.ShardPlacement(nil), meta.Placement...)
	return out
}

// Put stores a copy of the value under the given key.
func (m *MockStore) Put(key string, value []byte, isParity bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return err
	}
	m.data[key] = copyBytes(value)
	m.parity[key] = isParity
	return nil
}

// Get returns a copy of the value stored under the given key.
func (m *MockStore) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return nil, err
	}
	value, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("%w: key %q", storage.ErrNotFound, key)
	}
	return copyBytes(value), nil
}

// ApplyPutMeta installs metadata and returns the previous record.
func (m *MockStore) ApplyPutMeta(raftIndex uint64, meta storage.KeyMeta) (storage.KeyMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return storage.KeyMeta{}, err
	}
	if strings.TrimSpace(meta.Key) == "" || strings.TrimSpace(meta.StripeID) == "" {
		return storage.KeyMeta{}, fmt.Errorf("key and stripe ID are required")
	}
	if raftIndex <= m.applied {
		return storage.KeyMeta{}, nil
	}

	old := copyMeta(m.keys[meta.Key])
	if old.Key != "" {
		if meta.Version < old.Version {
			return storage.KeyMeta{}, fmt.Errorf("%w: older version", storage.ErrConflict)
		}
		if meta.Version == old.Version &&
			(meta.StripeID != old.StripeID || meta.Checksum != old.Checksum || meta.Size != old.Size) {
			return storage.KeyMeta{}, fmt.Errorf("%w: same version with different metadata", storage.ErrConflict)
		}
	}

	if old.StripeID != "" && old.StripeID != meta.StripeID {
		delete(m.stripes, old.StripeID)
	}
	m.stripes[meta.StripeID] = struct{}{}
	m.keys[meta.Key] = copyMeta(meta)
	m.applied = raftIndex
	return old, nil
}

// ApplyMetaStatus updates the degraded flag for an existing stripe version.
func (m *MockStore) ApplyMetaStatus(raftIndex uint64, key, stripeID string, version int64, degraded bool) (storage.KeyMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return storage.KeyMeta{}, err
	}
	if raftIndex <= m.applied {
		return storage.KeyMeta{}, nil
	}

	current, ok := m.keys[key]
	if !ok {
		return storage.KeyMeta{}, fmt.Errorf("%w: key %q", storage.ErrNotFound, key)
	}
	if current.StripeID != stripeID || current.Version != version {
		return storage.KeyMeta{}, fmt.Errorf("%w: stripe/version mismatch", storage.ErrConflict)
	}
	current.Degraded = degraded
	m.keys[key] = current
	m.applied = raftIndex
	return copyMeta(current), nil
}

// ApplyDeleteMeta removes a key and returns its old metadata.
func (m *MockStore) ApplyDeleteMeta(raftIndex uint64, key string, expectedVersion int64) (storage.KeyMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return storage.KeyMeta{}, err
	}
	if raftIndex <= m.applied {
		return storage.KeyMeta{}, nil
	}

	old, ok := m.keys[key]
	if !ok {
		m.applied = raftIndex
		return storage.KeyMeta{}, nil
	}
	if expectedVersion != 0 && expectedVersion != old.Version {
		return storage.KeyMeta{}, fmt.Errorf("%w: version mismatch", storage.ErrConflict)
	}

	delete(m.keys, key)
	delete(m.stripes, old.StripeID)
	m.applied = raftIndex
	return copyMeta(old), nil
}

// ApplySetConfig stores the singleton config and advances the applied index.
func (m *MockStore) ApplySetConfig(raftIndex uint64, cfg storage.ClusterConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return err
	}
	if raftIndex <= m.applied {
		return nil
	}
	m.config = cfg
	m.hasConfig = true
	m.applied = raftIndex
	return nil
}

// GetKey returns committed metadata for a key.
func (m *MockStore) GetKey(key string) (storage.KeyMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return storage.KeyMeta{}, err
	}
	meta, ok := m.keys[key]
	if !ok {
		return storage.KeyMeta{}, fmt.Errorf("%w: key %q", storage.ErrNotFound, key)
	}
	return copyMeta(meta), nil
}

// ListKeys returns all key metadata ordered by key.
func (m *MockStore) ListKeys() ([]storage.KeyMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(m.keys))
	for key := range m.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]storage.KeyMeta, 0, len(keys))
	for _, key := range keys {
		out = append(out, copyMeta(m.keys[key]))
	}
	return out, nil
}

// LastAppliedIndex returns the last applied metadata index.
func (m *MockStore) LastAppliedIndex() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return 0, err
	}
	return m.applied, nil
}

// PutFragment stores a fragment snapshot.
func (m *MockStore) PutFragment(frag storage.Fragment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return err
	}
	if frag.ShardIndex < 0 || frag.ShardIndex > 4 {
		return fmt.Errorf("invalid shard index %d", frag.ShardIndex)
	}
	if m.fragments[frag.StripeID] == nil {
		m.fragments[frag.StripeID] = make(map[int]storage.Fragment)
	}
	if existing, ok := m.fragments[frag.StripeID][frag.ShardIndex]; ok && frag.CreatedAt.IsZero() {
		frag.CreatedAt = existing.CreatedAt
	}
	frag.Data = copyBytes(frag.Data)
	m.fragments[frag.StripeID][frag.ShardIndex] = frag
	return nil
}

// GetFragment returns one fragment.
func (m *MockStore) GetFragment(stripeID string, shardIndex int) (storage.Fragment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return storage.Fragment{}, err
	}
	frag, ok := m.fragments[stripeID][shardIndex]
	if !ok {
		return storage.Fragment{}, fmt.Errorf("%w: fragment %q/%d", storage.ErrNotFound, stripeID, shardIndex)
	}
	frag.Data = copyBytes(frag.Data)
	return frag, nil
}

// DeleteFragment removes one fragment.
func (m *MockStore) DeleteFragment(stripeID string, shardIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return err
	}
	delete(m.fragments[stripeID], shardIndex)
	return nil
}

// DeleteStripe removes a stripe's metadata, placements, and fragments.
func (m *MockStore) DeleteStripe(stripeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return err
	}
	delete(m.stripes, stripeID)
	delete(m.fragments, stripeID)
	for key, meta := range m.keys {
		if meta.StripeID == stripeID {
			delete(m.keys, key)
		}
	}
	return nil
}

// ListFragments returns fragment metadata for a stripe, or every local
// fragment when stripeID is empty.
func (m *MockStore) ListFragments(stripeID string) ([]storage.FragmentRef, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return nil, err
	}

	var refs []storage.FragmentRef
	for id, shards := range m.fragments {
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

// PutConfig stores the singleton config for seeding and tests.
func (m *MockStore) PutConfig(cfg storage.ClusterConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ready(); err != nil {
		return err
	}
	m.config = cfg
	m.hasConfig = true
	return nil
}

// GetConfig returns the singleton config.
func (m *MockStore) GetConfig() (storage.ClusterConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return storage.ClusterConfig{}, err
	}
	if !m.hasConfig {
		return storage.ClusterConfig{}, fmt.Errorf("%w: cluster config", storage.ErrNotFound)
	}
	return m.config, nil
}

// Stats reports in-memory usage.
func (m *MockStore) Stats() (storage.StoreStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return storage.StoreStats{}, err
	}

	stats := storage.StoreStats{
		KeyCount:    int64(len(m.keys)),
		StripeCount: int64(len(m.stripes)),
	}
	for _, shards := range m.fragments {
		for _, frag := range shards {
			stats.FragmentCount++
			stats.FragmentBytes += int64(len(frag.Data))
		}
	}
	for _, value := range m.data {
		stats.Bytes += int64(len(value))
	}
	return stats, nil
}

// Snapshot serializes the legacy KV space as JSON.
func (m *MockStore) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.ready(); err != nil {
		return nil, err
	}
	return json.Marshal(m.data)
}

// Restore replaces the legacy KV space from a JSON snapshot.
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
	if err := m.ready(); err != nil {
		return err
	}
	m.data = restored
	m.parity = make(map[string]bool)
	return nil
}

// Close marks the store closed.
func (m *MockStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
