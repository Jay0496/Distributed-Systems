package server

import (
	"errors"
	"fmt"
	"sync"
)

// maxConcurrentFragmentUploads bounds in-flight staging buffers so a burst of
// uploads cannot consume memory proportional to MaxFragmentSize per stream.
const maxConcurrentFragmentUploads = 64

var (
	// errUploadInFlight means another stream is already staging the same
	// (stripe_id, shard_index) pair. The caller can retry after it finishes.
	errUploadInFlight = errors.New("fragment upload already in progress")

	// errTooManyUploads means the node is already staging the maximum number
	// of fragments.
	errTooManyUploads = errors.New("too many concurrent fragment uploads")
)

// fragmentID is the v2 primary identity of a local fragment. Shard indices are
// global: 0-2 are RS data shards, 3-4 are RS parity shards, and all indices are
// full copies in replication mode.
type fragmentID struct {
	stripeID   string
	shardIndex int
}

// uploadState holds one in-flight upload. It is owned by a single stream, so
// the staging map's mutex only protects ownership, not the buffer.
type uploadState struct {
	totalSize int64
	checksum  string
	replace   bool
	isParity  bool
	data      []byte
}

// appendData enforces contiguous offsets and the declared total size. A
// duplicate, out-of-order, or overlapping chunk is rejected instead of being
// silently applied to the wrong position.
func (u *uploadState) appendData(offset int64, data []byte) error {
	if offset != int64(len(u.data)) {
		return fmt.Errorf("chunk offset %d does not match expected offset %d", offset, len(u.data))
	}
	if int64(len(u.data))+int64(len(data)) > u.totalSize {
		return fmt.Errorf("chunk would exceed total_size %d", u.totalSize)
	}
	u.data = append(u.data, data...)
	return nil
}

// fragmentStaging tracks active uploads. A second upload for the same fragment
// identity is rejected with errUploadInFlight rather than sharing a buffer,
// because interleaved streams would corrupt each other's offsets.
type fragmentStaging struct {
	mu      sync.Mutex
	uploads map[fragmentID]*uploadState
}

func newFragmentStaging() *fragmentStaging {
	return &fragmentStaging{uploads: make(map[fragmentID]*uploadState)}
}

// begin claims the staging slot for id.
func (s *fragmentStaging) begin(id fragmentID, totalSize int64, checksum string, replace, isParity bool) (*uploadState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.uploads[id]; ok {
		return nil, errUploadInFlight
	}
	if len(s.uploads) >= maxConcurrentFragmentUploads {
		return nil, errTooManyUploads
	}

	state := &uploadState{
		totalSize: totalSize,
		checksum:  checksum,
		replace:   replace,
		isParity:  isParity,
	}
	s.uploads[id] = state
	return state, nil
}

// end releases the staging slot. It is safe to call more than once.
func (s *fragmentStaging) end(id fragmentID) {
	s.mu.Lock()
	delete(s.uploads, id)
	s.mu.Unlock()
}

// active reports the number of in-flight uploads. It is used by tests.
func (s *fragmentStaging) active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uploads)
}
