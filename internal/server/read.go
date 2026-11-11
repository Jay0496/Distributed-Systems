package server

import (
	"context"
	"fmt"
	"strings"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ShardRef identifies one placement that could not supply a usable shard.
// Corrupt distinguishes a checksum or size failure from a missing,
// unreachable, or timed-out owner; read repair only overwrites corrupt copies.
type ShardRef struct {
	ShardIndex int
	NodeID     string
	Corrupt    bool
}

// read serves a leader-side read: linearizable metadata from Raft, then enough
// verified fragments to reconstruct the value.
func (s *Server) read(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	meta, found, err := s.readMeta(ctx, req.GetKey())
	if err != nil {
		return nil, readStatusError(err)
	}
	if !found {
		return nil, status.Error(codes.NotFound, "key not found")
	}

	value, failed, err := s.readVerified(ctx, meta)
	if err != nil {
		return nil, readStatusError(err)
	}
	repaired := s.enqueueReadRepair(meta, value, failed)

	return &pb.GetResponse{
		Success:    true,
		Value:      value,
		Mode:       meta.Mode,
		StripeId:   meta.StripeID,
		Version:    meta.Version,
		ShardsUsed: int32(requiredShards(meta.Mode)),
		Repaired:   repaired,
		Checksum:   meta.Checksum,
	}, nil
}

// ReadVerified reconstructs and verifies a value from a committed metadata
// record without enqueuing read repair or writing metadata. Task 09's repairer
// consumes this through its ValueReader interface.
func (s *Server) ReadVerified(ctx context.Context, meta storage.KeyMeta) ([]byte, error) {
	value, _, err := s.readVerified(ctx, meta)
	return value, err
}

// readVerified gathers fragments from the owners recorded in meta and returns
// the verified bytes plus every placement that could not supply a usable shard.
// It never returns a partially decoded or unchecksummed value.
func (s *Server) readVerified(ctx context.Context, meta storage.KeyMeta) ([]byte, []ShardRef, error) {
	mode, err := redundancy.ParseMode(meta.Mode)
	if err != nil {
		return nil, nil, fmt.Errorf("read metadata for key %q: %w", meta.Key, err)
	}
	if !mode.Valid() {
		return nil, nil, fmt.Errorf("read metadata for key %q: invalid mode %q", meta.Key, meta.Mode)
	}
	if len(meta.Placement) != redundancy.TotalShards {
		return nil, nil, fmt.Errorf("read metadata for key %q: %d placements, want %d", meta.Key, len(meta.Placement), redundancy.TotalShards)
	}

	if mode == redundancy.ModeReplication {
		return s.readReplication(ctx, meta)
	}
	return s.readRS32(ctx, meta, mode)
}

// readRS32 gathers at least three valid shards, reconstructs the value, and
// retries with other shards when the complete value checksum fails.
func (s *Server) readRS32(ctx context.Context, meta storage.KeyMeta, mode redundancy.Mode) ([]byte, []ShardRef, error) {
	valid, failed, attempted, err := s.gatherShards(ctx, meta, redundancy.RSDataShards, meta.Degraded, nil)
	if err != nil {
		return nil, failed, err
	}

	value, ok := decodeRS32(valid, meta)
	if !ok && len(attempted) < len(meta.Placement) {
		// A checksum failure can mean the first three shards were individually
		// valid but mutually inconsistent. Fetch the rest and try every subset.
		moreValid, moreFailed, _, gatherErr := s.gatherShards(ctx, meta, redundancy.TotalShards, true, attempted)
		if gatherErr != nil {
			return nil, failed, gatherErr
		}
		valid = append(valid, moreValid...)
		failed = append(failed, moreFailed...)
		value, ok = decodeRS32(valid, meta)
	}

	if len(valid) < redundancy.RSDataShards {
		live := make([]ShardRef, 0, len(valid))
		for _, shard := range valid {
			live = append(live, shard.ref)
		}
		return nil, failed, fmt.Errorf("%w: %d valid RS shards, need %d; live owners: %s; failed owners: %s",
			ErrInsufficientShards, len(valid), redundancy.RSDataShards,
			formatShardRefs(live), formatShardRefs(failed))
	}
	if !ok {
		return nil, failed, fmt.Errorf("%w: no three RS shards reconstructed the committed checksum; failed owners: %s",
			ErrDataLoss, formatShardRefs(failed))
	}
	return value, failed, nil
}

// readReplication tries copies in shard-index order and falls back to other
// placements when a copy fails the final value checksum.
func (s *Server) readReplication(ctx context.Context, meta storage.KeyMeta) ([]byte, []ShardRef, error) {
	valid, failed, attempted, err := s.gatherShards(ctx, meta, 1, meta.Degraded, nil)
	if err != nil {
		return nil, failed, err
	}

	value, corrupt, ok := decodeReplication(valid, meta)
	failed = appendUniqueRefs(failed, corrupt...)
	if !ok && len(attempted) < len(meta.Placement) {
		moreValid, moreFailed, _, gatherErr := s.gatherShards(ctx, meta, redundancy.TotalShards, true, attempted)
		if gatherErr != nil {
			return nil, failed, gatherErr
		}
		valid = append(valid, moreValid...)
		failed = appendUniqueRefs(failed, moreFailed...)
		value, corrupt, ok = decodeReplication(valid, meta)
		failed = appendUniqueRefs(failed, corrupt...)
	}

	if len(valid) == 0 {
		return nil, failed, fmt.Errorf("%w: no valid replication copy; failed owners: %s",
			ErrInsufficientShards, formatShardRefs(failed))
	}
	if !ok {
		return nil, failed, fmt.Errorf("%w: no replication copy matched the committed size and checksum; failed owners: %s",
			ErrDataLoss, formatShardRefs(failed))
	}
	return value, failed, nil
}

// decodeRS32 tries every three-shard subset. A subset succeeds only when the
// decoded value matches both the committed length and SHA-256 checksum.
func decodeRS32(valid []fetchedShard, meta storage.KeyMeta) ([]byte, bool) {
	if len(valid) < redundancy.RSDataShards {
		return nil, false
	}
	engine := redundancy.NewRS32Engine()

	for i := 0; i < len(valid)-2; i++ {
		for j := i + 1; j < len(valid)-1; j++ {
			for k := j + 1; k < len(valid); k++ {
				shards := []redundancy.Shard{valid[i].shard, valid[j].shard, valid[k].shard}
				value, err := engine.Decode(shards, redundancy.ModeRS32, meta.Size)
				if err != nil {
					continue
				}
				if valueMatches(value, meta) {
					return value, true
				}
			}
		}
	}
	return nil, false
}

// decodeReplication returns the first copy that matches the committed value
// checksum. Copies that pass their fragment checksum but fail the value
// checksum are returned as corrupt repair targets.
func decodeReplication(valid []fetchedShard, meta storage.KeyMeta) ([]byte, []ShardRef, bool) {
	engine := redundancy.NewReplicationEngine()
	var corrupt []ShardRef

	for _, candidate := range valid {
		value, err := engine.Decode([]redundancy.Shard{candidate.shard}, redundancy.ModeReplication, meta.Size)
		if err != nil {
			corrupt = appendUniqueRefs(corrupt, ShardRef{
				ShardIndex: candidate.ref.ShardIndex,
				NodeID:     candidate.ref.NodeID,
				Corrupt:    true,
			})
			continue
		}
		if !valueMatches(value, meta) {
			corrupt = appendUniqueRefs(corrupt, ShardRef{
				ShardIndex: candidate.ref.ShardIndex,
				NodeID:     candidate.ref.NodeID,
				Corrupt:    true,
			})
			continue
		}
		return value, corrupt, true
	}
	return nil, corrupt, false
}

// valueMatches verifies the original value's length and SHA-256 checksum.
func valueMatches(value []byte, meta storage.KeyMeta) bool {
	if int64(len(value)) != meta.Size {
		return false
	}
	return redundancy.Checksum(value) == meta.Checksum
}

// requiredShards is the minimum number of fragments needed for a mode.
func requiredShards(mode string) int {
	if mode == string(redundancy.ModeRS32) {
		return redundancy.RSDataShards
	}
	return 1
}

// appendUniqueRefs adds refs that are not already present.
func appendUniqueRefs(existing []ShardRef, refs ...ShardRef) []ShardRef {
	seen := make(map[ShardRef]struct{}, len(existing)+len(refs))
	for _, ref := range existing {
		seen[ref] = struct{}{}
	}
	for _, ref := range refs {
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		existing = append(existing, ref)
	}
	return existing
}

// formatShardRefs renders failed placements for an operator-visible error.
func formatShardRefs(refs []ShardRef) string {
	if len(refs) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		state := "unavailable"
		if ref.Corrupt {
			state = "corrupt"
		}
		parts = append(parts, fmt.Sprintf("%d@%s (%s)", ref.ShardIndex, ref.NodeID, state))
	}
	return strings.Join(parts, ", ")
}
