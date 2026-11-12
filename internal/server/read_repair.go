package server

import (
	"context"
	"fmt"
	"log"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// repairQueueCapacity bounds how many repair tasks may wait. The shared queue
// drops a repair, never the read that discovered it, when the queue is full.
const repairQueueCapacity = 10000

// RepairQueue is the server-facing seam over the shared repair scheduler.
// *cluster.Queue satisfies it. Tests can also supply a recording implementation.
type RepairQueue interface {
	Enqueue(cluster.Task) error
	Close()
}

// enqueueReadRepair schedules one best-effort task per failed placement. It
// returns true when at least one task was accepted. The read response never
// waits for repair.
func (s *Server) enqueueReadRepair(meta storage.KeyMeta, _ []byte, failed []ShardRef) bool {
	if len(failed) == 0 || s.repairQueue == nil {
		return false
	}

	accepted := false
	for _, ref := range failed {
		task := cluster.Task{
			Key:        meta.Key,
			StripeID:   meta.StripeID,
			Version:    meta.Version,
			ShardIndex: ref.ShardIndex,
			Owner:      ref.NodeID,
			Replace:    ref.Corrupt,
			Priority:   cluster.PriorityReadRepair,
		}
		if err := s.repairQueue.Enqueue(task); err != nil {
			log.Printf("read: repair not queued key=%s stripe=%s shard=%d error=%v",
				meta.Key, meta.StripeID, ref.ShardIndex, err)
			continue
		}
		accepted = true
	}
	if accepted {
		log.Printf("read: repair queued key=%s stripe=%s shards=%d", meta.Key, meta.StripeID, len(failed))
	}
	return accepted
}

// SetRepairQueue replaces the queue used by read repair. The previous queue is
// closed when it differs, so replacing the default with a repairer-backed queue
// does not leak workers.
func (s *Server) SetRepairQueue(queue RepairQueue) {
	if s.repairQueue != nil && s.repairQueue != queue {
		s.repairQueue.Close()
	}
	s.repairQueue = queue
}

// SetDegraded commits a set_meta_status command through the configured apply
// seam. Repair uses it only to clear the degraded flag after all five
// placements hold the committed shard checksum. It never changes version,
// stripe ID, or placement.
func (s *Server) SetDegraded(ctx context.Context, key, stripeID string, version int64, degraded bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := marshalCommand(consensus.Command{
		Op:       consensus.OpSetMetaStatus,
		Key:      key,
		StripeID: stripeID,
		Version:  version,
		Degraded: degraded,
	})
	if err != nil {
		return err
	}

	response, err := s.apply(data, s.cfg.WriteTimeout)
	if err != nil {
		return fmt.Errorf("set_meta_status %s: %w", key, err)
	}
	result, ok := response.(*consensus.Result)
	if !ok || result == nil {
		return fmt.Errorf("unexpected set_meta_status response %T", response)
	}
	if result.Err != "" {
		return fmt.Errorf("set_meta_status %s: %s", key, result.Err)
	}
	return nil
}

// UploadShard is the repairer's narrow upload seam over the task 06 fragment
// push. It reuses the same chunking, checksum, timeout, and replace handling as
// a normal write.
func (s *Server) UploadShard(ctx context.Context, nodeID, stripeID string, shard redundancy.Shard, replace bool) error {
	placement := storage.ShardPlacement{
		ShardIndex: shard.Index,
		NodeID:     nodeID,
		IsParity:   shard.IsParity,
	}
	return s.pushShardWithReplace(ctx, stripeID, shard, placement, replace)
}

// StatShard reports whether an owner stores a fragment and its stored checksum
// without transferring the payload. A missing fragment returns present=false and
// a nil error so repair can upload it with replace=false.
func (s *Server) StatShard(ctx context.Context, nodeID, stripeID string, shardIndex int) (bool, string, error) {
	if nodeID == s.cfg.NodeID {
		refs, err := s.store.ListFragments(stripeID)
		if err != nil {
			return false, "", err
		}
		for _, ref := range refs {
			if ref.ShardIndex == shardIndex {
				return true, ref.Checksum, nil
			}
		}
		return false, "", nil
	}

	target, ok := s.cfg.Peers[nodeID]
	if !ok || target == "" {
		return false, "", fmt.Errorf("no address for owner %q", nodeID)
	}
	statCtx, cancel := context.WithTimeout(ctx, s.cfg.ShardUploadTimeout)
	defer cancel()

	conn, err := s.getConn(statCtx, target)
	if err != nil {
		return false, "", fmt.Errorf("dial owner %q: %w", nodeID, err)
	}
	info, err := pb.NewStorageServiceClient(conn).StatFragment(statCtx, &pb.DeleteFragmentRequest{
		StripeId:   stripeID,
		ShardIndex: int32(shardIndex),
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, "", nil
		}
		if status.Code(err) == codes.Unavailable {
			s.dropConn(target)
		}
		return false, "", fmt.Errorf("stat fragment %s/%d on %s: %w", stripeID, shardIndex, nodeID, err)
	}
	return true, info.GetChecksum(), nil
}
