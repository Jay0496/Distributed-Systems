package server

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// nodeIDs returns the sorted cluster node IDs known to this server. The local
// node is always included so a leader can own a shard even if PEERS omits it.
func (s *Server) nodeIDs() []string {
	seen := make(map[string]struct{}, len(s.cfg.Peers)+1)
	if s.cfg.NodeID != "" {
		seen[s.cfg.NodeID] = struct{}{}
	}
	for id := range s.cfg.Peers {
		if id != "" {
			seen[id] = struct{}{}
		}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// placerOrDefault returns the configured placer or the deterministic default.
func (s *Server) placerOrDefault() redundancy.Placer {
	if s.placer == nil {
		return redundancy.NewPlacer()
	}
	return s.placer
}

// encodeAndPlace encodes the value for the intent's mode and maps every shard
// to its deterministic owner.
func (s *Server) encodeAndPlace(intent *writeIntent) ([]redundancy.Shard, []storage.ShardPlacement, error) {
	engine, err := engineFor(intent.mode)
	if err != nil {
		return nil, nil, err
	}
	shards, err := engine.Encode(intent.value, intent.mode)
	if err != nil {
		return nil, nil, fmt.Errorf("encode value: %w", err)
	}
	if len(shards) != redundancy.TotalShards {
		return nil, nil, fmt.Errorf("encoder produced %d shards, want %d", len(shards), redundancy.TotalShards)
	}

	placements, err := s.placerOrDefault().Owners(
		intent.stripeID, intent.mode, len(shards), s.nodeIDs(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("place shards: %w", err)
	}
	if len(placements) != len(shards) {
		return nil, nil, fmt.Errorf("placer returned %d owners for %d shards", len(placements), len(shards))
	}

	converted := make([]storage.ShardPlacement, len(placements))
	for i, placement := range placements {
		converted[i] = storage.ShardPlacement{
			ShardIndex: placement.ShardIndex,
			NodeID:     placement.NodeID,
			IsParity:   placement.IsParity,
		}
	}
	return shards, converted, nil
}

// pushOutcome is the result of one shard upload attempt.
type pushOutcome struct {
	placement storage.ShardPlacement
	err       error
}

// pushShards uploads every shard to its owner concurrently. It never cancels
// the remaining uploads on the first failure: three successes are enough for a
// write to proceed, so every attempt must be allowed to finish. The returned
// placements are exactly the fragments that were durably stored.
func (s *Server) pushShards(ctx context.Context, stripeID string, shards []redundancy.Shard, placements []storage.ShardPlacement) []storage.ShardPlacement {
	if len(shards) != len(placements) {
		return nil
	}

	byShard := make(map[int]storage.ShardPlacement, len(placements))
	for _, placement := range placements {
		byShard[placement.ShardIndex] = placement
	}

	results := make(chan pushOutcome, len(shards))
	var wg sync.WaitGroup
	for _, shard := range shards {
		placement, ok := byShard[shard.Index]
		if !ok {
			results <- pushOutcome{
				placement: storage.ShardPlacement{ShardIndex: shard.Index},
				err:       fmt.Errorf("no owner for shard %d", shard.Index),
			}
			continue
		}

		wg.Add(1)
		go func(shard redundancy.Shard, placement storage.ShardPlacement) {
			defer wg.Done()
			results <- pushOutcome{
				placement: placement,
				err:       s.pushShard(ctx, stripeID, shard, placement),
			}
		}(shard, placement)
	}

	wg.Wait()
	close(results)

	stored := make([]storage.ShardPlacement, 0, len(shards))
	for outcome := range results {
		if outcome.err == nil {
			stored = append(stored, outcome.placement)
		}
	}
	sort.Slice(stored, func(i, j int) bool {
		return stored[i].ShardIndex < stored[j].ShardIndex
	})
	return stored
}

// pushShard stores one shard at its owner. The leader writes its own shards
// directly through the same Store repository; remote owners receive the
// versioned fragment stream. A checksum mismatch is rejected before any bytes
// leave the process.
func (s *Server) pushShard(ctx context.Context, stripeID string, shard redundancy.Shard, placement storage.ShardPlacement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := shard.Verify(); err != nil {
		return err
	}

	ownerCtx, cancel := context.WithTimeout(ctx, s.cfg.ShardUploadTimeout)
	defer cancel()

	if placement.NodeID == s.cfg.NodeID {
		return s.store.PutFragment(storage.Fragment{
			StripeID:   stripeID,
			ShardIndex: shard.Index,
			IsParity:   shard.IsParity,
			Data:       shard.Data,
			Checksum:   shard.Checksum,
		})
	}

	target, ok := s.cfg.Peers[placement.NodeID]
	if !ok {
		return fmt.Errorf("no address for owner %q", placement.NodeID)
	}

	conn, err := s.getConn(ownerCtx, target)
	if err != nil {
		return fmt.Errorf("dial owner %q: %w", placement.NodeID, err)
	}
	client := pb.NewStorageServiceClient(conn)

	stream, err := client.UploadFragment(ownerCtx)
	if err != nil {
		s.dropConn(target)
		return fmt.Errorf("open upload to owner %q: %w", placement.NodeID, err)
	}

	total := int64(len(shard.Data))
	chunkSize := s.cfg.UploadChunkSize
	if chunkSize <= 0 {
		chunkSize = len(shard.Data)
	}
	if chunkSize <= 0 {
		if err := stream.Send(&pb.FragmentChunk{
			StripeId:   stripeID,
			ShardIndex: int32(shard.Index),
			IsParity:   shard.IsParity,
			Checksum:   shard.Checksum,
			Offset:     0,
			TotalSize:  0,
			Last:       true,
		}); err != nil {
			return fmt.Errorf("send empty shard %d: %w", shard.Index, err)
		}
	} else {
		for offset := 0; offset < len(shard.Data); offset += chunkSize {
			end := offset + chunkSize
			if end > len(shard.Data) {
				end = len(shard.Data)
			}
			chunk := &pb.FragmentChunk{
				StripeId:   stripeID,
				ShardIndex: int32(shard.Index),
				IsParity:   shard.IsParity,
				Checksum:   shard.Checksum,
				Offset:     int64(offset),
				TotalSize:  total,
				Data:       shard.Data[offset:end],
				Last:       end == len(shard.Data),
			}
			if err := stream.Send(chunk); err != nil {
				_ = stream.CloseSend()
				if status.Code(err) == codes.Unavailable {
					s.dropConn(target)
				}
				return fmt.Errorf("send shard %d to %q: %w", shard.Index, placement.NodeID, err)
			}
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		if status.Code(err) == codes.Unavailable {
			s.dropConn(target)
		}
		return fmt.Errorf("upload shard %d to %q: %w", shard.Index, placement.NodeID, err)
	}
	return nil
}
