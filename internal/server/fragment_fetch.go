package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fetchedShard is one checksum-valid shard or copy together with the placement
// that produced it.
type fetchedShard struct {
	ref   ShardRef
	shard redundancy.Shard
}

// fetchResult carries one owner fetch outcome to the gather loop.
type fetchResult struct {
	placement storage.ShardPlacement
	shard     redundancy.Shard
	err       error
}

// orderedPlacements returns the stripe's placements in fetch order. RS reads
// shuffle the order to spread load across owners; replication reads keep shard
// index order so the first stored copy is preferred. A caller can make the
// order deterministic by assigning Server.readRand.
func (s *Server) orderedPlacements(meta storage.KeyMeta) []storage.ShardPlacement {
	placements := append([]storage.ShardPlacement(nil), meta.Placement...)
	sort.Slice(placements, func(i, j int) bool {
		return placements[i].ShardIndex < placements[j].ShardIndex
	})

	mode, err := redundancy.ParseMode(meta.Mode)
	if err != nil || mode != redundancy.ModeRS32 {
		return placements
	}

	s.readMu.Lock()
	if s.readRand == nil {
		s.readRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	s.readRand.Shuffle(len(placements), func(i, j int) {
		placements[i], placements[j] = placements[j], placements[i]
	})
	s.readMu.Unlock()
	return placements
}

// gatherShards fetches placements concurrently with a bounded worker count.
// It stops issuing new fetches once need valid shards have arrived, unless
// fetchAll is set. Requests already in flight are allowed to finish and are
// drained even when they are no longer needed.
//
// skip excludes placements that an earlier pass already attempted. attempted
// reports every placement this pass actually contacted. A parent context
// cancellation stops the whole read; an individual owner failure only marks
// that shard as failed.
func (s *Server) gatherShards(ctx context.Context, meta storage.KeyMeta, need int, fetchAll bool, skip map[int]struct{}) ([]fetchedShard, []ShardRef, map[int]struct{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if need < 1 {
		need = 1
	}

	pending := make([]storage.ShardPlacement, 0, len(meta.Placement))
	for _, placement := range s.orderedPlacements(meta) {
		if skip != nil {
			if _, done := skip[placement.ShardIndex]; done {
				continue
			}
		}
		pending = append(pending, placement)
	}
	attempted := make(map[int]struct{}, len(pending))
	if len(pending) == 0 {
		return nil, nil, attempted, nil
	}

	parallelism := s.cfg.ReadParallelism
	if parallelism <= 0 {
		parallelism = 5
	}
	if parallelism > len(pending) {
		parallelism = len(pending)
	}

	results := make(chan fetchResult, len(pending))
	launch := func(placement storage.ShardPlacement) {
		go func() {
			shard, err := s.fetchShard(ctx, meta.StripeID, placement)
			results <- fetchResult{placement: placement, shard: shard, err: err}
		}()
	}

	var (
		valid      []fetchedShard
		failed     []ShardRef
		launched   int
		inFlight   int
		validCount int
	)

	for {
		for inFlight < parallelism && launched < len(pending) && (fetchAll || validCount < need) {
			launch(pending[launched])
			launched++
			inFlight++
		}
		if inFlight == 0 {
			break
		}

		result := <-results
		inFlight--
		attempted[result.placement.ShardIndex] = struct{}{}

		if result.err == nil {
			validCount++
			valid = append(valid, fetchedShard{
				ref: ShardRef{
					ShardIndex: result.placement.ShardIndex,
					NodeID:     result.placement.NodeID,
				},
				shard: result.shard,
			})
			continue
		}
		if ctx.Err() != nil {
			return nil, nil, attempted, ctx.Err()
		}
		failed = append(failed, ShardRef{
			ShardIndex: result.placement.ShardIndex,
			NodeID:     result.placement.NodeID,
			Corrupt:    corruptFetch(result.err),
		})
	}

	if err := ctx.Err(); err != nil {
		return nil, nil, attempted, err
	}
	sort.Slice(valid, func(i, j int) bool {
		return valid[i].shard.Index < valid[j].shard.Index
	})
	sort.Slice(failed, func(i, j int) bool {
		return failed[i].ShardIndex < failed[j].ShardIndex
	})
	return valid, failed, attempted, nil
}

// fetchShard reads one shard or copy from its intended owner. The leader's own
// placement is read from the local store, which avoids a loopback RPC.
func (s *Server) fetchShard(ctx context.Context, stripeID string, placement storage.ShardPlacement) (redundancy.Shard, error) {
	if placement.NodeID == s.cfg.NodeID {
		return s.fetchLocalShard(stripeID, placement)
	}

	target, ok := s.cfg.Peers[placement.NodeID]
	if !ok {
		return redundancy.Shard{}, fmt.Errorf("no address for owner %q", placement.NodeID)
	}

	fetchCtx, cancel := context.WithTimeout(ctx, s.cfg.ReadTimeout)
	defer cancel()

	conn, err := s.getConn(fetchCtx, target)
	if err != nil {
		return redundancy.Shard{}, fmt.Errorf("dial owner %q: %w", placement.NodeID, err)
	}
	stream, err := pb.NewStorageServiceClient(conn).DownloadFragment(fetchCtx, &pb.FragmentRequest{
		StripeId:   stripeID,
		ShardIndex: int32(placement.ShardIndex),
	})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			s.dropConn(target)
		}
		return redundancy.Shard{}, fmt.Errorf("open download from owner %q: %w", placement.NodeID, err)
	}

	shard, err := readDownloadedShard(stream, stripeID, placement)
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			s.dropConn(target)
		}
		return redundancy.Shard{}, err
	}
	return shard, nil
}

// fetchLocalShard reads a fragment from the local SQLite store and verifies its
// checksum before it can participate in reconstruction.
func (s *Server) fetchLocalShard(stripeID string, placement storage.ShardPlacement) (redundancy.Shard, error) {
	frag, err := s.store.GetFragment(stripeID, placement.ShardIndex)
	if err != nil {
		return redundancy.Shard{}, err
	}
	if err := redundancy.VerifyChecksum(frag.Data, frag.Checksum); err != nil {
		return redundancy.Shard{}, fmt.Errorf("local fragment %s/%d: %w: %v", stripeID, placement.ShardIndex, errFragmentCorrupt, err)
	}
	return redundancy.Shard{
		Index:    placement.ShardIndex,
		IsParity: placement.IsParity,
		Data:     frag.Data,
		Checksum: frag.Checksum,
	}, nil
}

// readDownloadedShard reassembles and verifies one streamed fragment.
func readDownloadedShard(stream pb.StorageService_DownloadFragmentClient, stripeID string, placement storage.ShardPlacement) (redundancy.Shard, error) {
	var (
		data       []byte
		checksum   string
		totalSize  int64 = -1
		sawChunk   bool
		sawLast    bool
		nextOffset int64
	)

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return redundancy.Shard{}, fmt.Errorf("download fragment %s/%d: %w", stripeID, placement.ShardIndex, err)
		}
		if sawLast {
			return redundancy.Shard{}, fmt.Errorf("download fragment %s/%d: chunk after last=true", stripeID, placement.ShardIndex)
		}
		if chunk.GetStripeId() != stripeID || int(chunk.GetShardIndex()) != placement.ShardIndex {
			return redundancy.Shard{}, fmt.Errorf(
				"download fragment %s/%d: identity changed to %s/%d",
				stripeID, placement.ShardIndex, chunk.GetStripeId(), chunk.GetShardIndex())
		}
		if !sawChunk {
			checksum = chunk.GetChecksum()
			totalSize = chunk.GetTotalSize()
		} else if chunk.GetChecksum() != checksum || chunk.GetTotalSize() != totalSize {
			return redundancy.Shard{}, fmt.Errorf("download fragment %s/%d: checksum or size changed mid-stream", stripeID, placement.ShardIndex)
		}
		sawChunk = true

		if chunk.GetOffset() != nextOffset {
			return redundancy.Shard{}, fmt.Errorf(
				"download fragment %s/%d: offset %d, want %d",
				stripeID, placement.ShardIndex, chunk.GetOffset(), nextOffset)
		}
		data = append(data, chunk.GetData()...)
		nextOffset += int64(len(chunk.GetData()))
		sawLast = chunk.GetLast()
	}

	if !sawChunk {
		return redundancy.Shard{}, fmt.Errorf("download fragment %s/%d: no chunks received", stripeID, placement.ShardIndex)
	}
	if !sawLast {
		return redundancy.Shard{}, fmt.Errorf("download fragment %s/%d: stream ended before last=true", stripeID, placement.ShardIndex)
	}
	if totalSize < 0 || int64(len(data)) != totalSize {
		return redundancy.Shard{}, fmt.Errorf(
			"download fragment %s/%d: received %d bytes, want %d",
			stripeID, placement.ShardIndex, len(data), totalSize)
	}
	if err := redundancy.VerifyChecksum(data, checksum); err != nil {
		return redundancy.Shard{}, fmt.Errorf("download fragment %s/%d: %w", stripeID, placement.ShardIndex, err)
	}

	return redundancy.Shard{
		Index:    placement.ShardIndex,
		IsParity: placement.IsParity,
		Data:     data,
		Checksum: checksum,
	}, nil
}

// corruptFetch reports whether a failed fetch proves the owner's fragment is
// corrupt. Missing, unreachable, and timed-out fragments are repair targets but
// are not overwritten, because the transport may still hold a valid copy.
func corruptFetch(err error) bool {
	return status.Code(err) == codes.DataLoss ||
		status.Code(err) == codes.Internal ||
		status.Code(err) == codes.InvalidArgument ||
		errors.Is(err, errFragmentCorrupt)
}

// errFragmentCorrupt marks a local checksum mismatch.
var errFragmentCorrupt = errors.New("fragment checksum mismatch")
