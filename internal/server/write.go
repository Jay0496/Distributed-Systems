package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
)

// errVersionConflict reports that the stored version did not match the
// requested expected_version, or that a concurrent writer won the race for the
// next version. It is an application error: Put reports it as success=false.
var errVersionConflict = errors.New("metadata version conflict")

// applyFunc abstracts a Raft Apply so commit outcomes can be tested
// deterministically. The production implementation waits for the future.
type applyFunc func(data []byte, timeout time.Duration) (interface{}, error)

// applyRaft applies one command through the local Raft node and waits for it to
// commit. A nil response with an error means the outcome is ambiguous: the
// entry may or may not have committed.
func (s *Server) applyRaft(data []byte, timeout time.Duration) (interface{}, error) {
	if s.node == nil || s.node.Raft == nil {
		return nil, errors.New("raft node is not configured")
	}
	if timeout <= 0 {
		timeout = s.cfg.WriteTimeout
	}

	// Raft.Apply's timeout only bounds enqueueing the entry. Wait on the future
	// separately so a stalled commit is reported as a deadline instead of
	// blocking the handler forever.
	future := s.node.Raft.Apply(data, timeout)

	done := make(chan struct{})
	var (
		response interface{}
		err      error
	)
	go func() {
		err = future.Error()
		if err == nil {
			response = future.Response()
		}
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return response, err
	case <-timer.C:
		return nil, context.DeadlineExceeded
	}
}

// marshalCommand encodes a command for the Raft log.
func marshalCommand(cmd consensus.Command) ([]byte, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal %s command: %w", cmd.Op, err)
	}
	return data, nil
}

// readMeta reads the committed metadata for key through Raft so the write
// coordinator observes every previously committed write. A missing key is not
// an error.
func (s *Server) readMeta(ctx context.Context, key string) (storage.KeyMeta, bool, error) {
	if err := ctx.Err(); err != nil {
		return storage.KeyMeta{}, false, err
	}
	data, err := marshalCommand(consensus.Command{Op: consensus.OpGetMeta, Key: key})
	if err != nil {
		return storage.KeyMeta{}, false, err
	}

	response, err := s.apply(data, s.cfg.WriteTimeout)
	if err != nil {
		return storage.KeyMeta{}, false, err
	}
	result, ok := response.(*consensus.Result)
	if !ok || result == nil {
		return storage.KeyMeta{}, false, fmt.Errorf("unexpected get_meta response %T", response)
	}
	if result.Err != "" {
		if result.Err == consensus.ResultErrConflict {
			return storage.KeyMeta{}, false, errVersionConflict
		}
		return storage.KeyMeta{}, false, fmt.Errorf("get_meta: %s", result.Err)
	}
	if !result.Found || result.Meta == nil {
		return storage.KeyMeta{}, false, nil
	}
	return *result.Meta, true, nil
}

// effectiveMode resolves the per-request override first, then the global mode
// committed in cluster config, then the rs32 default.
func (s *Server) effectiveMode(requested string) (redundancy.Mode, error) {
	if requested != "" {
		mode, err := redundancy.ParseMode(requested)
		if err != nil {
			return "", err
		}
		if !mode.Valid() {
			return "", fmt.Errorf("invalid redundancy mode %q", requested)
		}
		return mode, nil
	}

	cfg, err := s.store.GetConfig()
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return redundancy.ModeRS32, nil
		}
		return "", fmt.Errorf("read cluster config: %w", err)
	}
	mode, err := redundancy.ParseMode(cfg.DefaultMode)
	if err != nil {
		return "", fmt.Errorf("cluster config: %w", err)
	}
	if !mode.Valid() {
		return "", fmt.Errorf("cluster config has invalid mode %q", cfg.DefaultMode)
	}
	return mode, nil
}

// writeIntent is the identity a write will use before any shard is stored.
type writeIntent struct {
	key      string
	value    []byte
	mode     redundancy.Mode
	version  int64
	stripeID string
}

// newWriteIntent resolves the mode and allocates the next version and a unique
// stripe ID. It performs the get_meta Raft read; it never writes metadata.
func (s *Server) newWriteIntent(ctx context.Context, req *pb.PutRequest) (*writeIntent, error) {
	mode, err := s.effectiveMode(req.GetMode())
	if err != nil {
		return nil, err
	}

	current, found, err := s.readMeta(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}

	if expected := req.GetExpectedVersion(); expected != 0 {
		if !found || current.Version != expected {
			return nil, errVersionConflict
		}
	}

	version := int64(1)
	if found {
		version = current.Version + 1
	}

	stripeID, err := newStripeID(req.GetKey(), version)
	if err != nil {
		return nil, err
	}

	return &writeIntent{
		key:      req.GetKey(),
		value:    req.GetValue(),
		mode:     mode,
		version:  version,
		stripeID: stripeID,
	}, nil
}

// newStripeID derives a globally unique stripe identity from the key, the
// version, and eight random bytes. The random suffix keeps rapid overwrites
// distinct even when several writes share a key and a version.
func newStripeID(key string, version int64) (string, error) {
	var versionBytes [8]byte
	binary.BigEndian.PutUint64(versionBytes[:], uint64(version))

	random := make([]byte, 8)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return "", fmt.Errorf("generate stripe identity: %w", err)
	}

	h := sha256.New()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write(versionBytes[:])
	_, _ = h.Write(random)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// engineFor returns the encoder for a concrete mode.
func engineFor(mode redundancy.Mode) (redundancy.Engine, error) {
	switch mode {
	case redundancy.ModeRS32:
		return redundancy.NewRS32Engine(), nil
	case redundancy.ModeReplication:
		return redundancy.NewReplicationEngine(), nil
	default:
		return nil, fmt.Errorf("unsupported redundancy mode %q", mode)
	}
}

// raftErrIsAmbiguous reports whether a failed Apply proves the entry did not
// commit. Any error means the outcome is unknown, so callers must not delete
// fragments that the committed metadata might reference.
func raftErrIsAmbiguous(err error) bool {
	if err == nil {
		return false
	}
	// ErrNotLeader and ErrLeadershipLost are ambiguous: the entry may have
	// replicated before leadership changed.
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
		return true
	}
	return true
}
