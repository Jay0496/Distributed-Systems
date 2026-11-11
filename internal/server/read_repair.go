package server

import (
	"context"
	"fmt"
	"log"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

// readRepairQueueCapacity bounds how many read-repair tasks may wait. A full
// queue drops only the repair, never the read that discovered it.
const readRepairQueueCapacity = 64

// enqueueReadRepair schedules one best-effort repair task. It returns true when
// the task was accepted. The read response never waits for repair.
func (s *Server) enqueueReadRepair(meta storage.KeyMeta, value []byte, failed []ShardRef) bool {
	if len(failed) == 0 || s.repairQueue == nil {
		return false
	}

	task := cluster.RepairTask{
		Key:      meta.Key,
		StripeID: meta.StripeID,
		Version:  meta.Version,
		Value:    append([]byte(nil), value...),
		Failed:   make([]cluster.ShardRef, 0, len(failed)),
	}
	for _, ref := range failed {
		task.Failed = append(task.Failed, cluster.ShardRef{
			ShardIndex: ref.ShardIndex,
			NodeID:     ref.NodeID,
			Corrupt:    ref.Corrupt,
		})
	}

	if err := s.repairQueue.Enqueue(task); err != nil {
		log.Printf("read: repair not queued key=%s stripe=%s error=%v", meta.Key, meta.StripeID, err)
		return false
	}
	log.Printf("read: repair queued key=%s stripe=%s shards=%d", meta.Key, meta.StripeID, len(failed))
	return true
}

// performReadRepair re-encodes the verified value and restores only the failed
// placements, on their intended owners. It re-reads metadata first so an
// overwrite or delete that won the race causes the task to be dropped instead
// of resurrecting a superseded stripe.
func (s *Server) performReadRepair(ctx context.Context, task cluster.RepairTask) error {
	current, found, err := s.readMeta(ctx, task.Key)
	if err != nil {
		return fmt.Errorf("read repair %s: re-read metadata: %w", task.Key, err)
	}
	if !found {
		return nil
	}
	if current.StripeID != task.StripeID || current.Version != task.Version {
		log.Printf("read: repair dropped key=%s stripe=%s version=%d (current stripe=%s version=%d)",
			task.Key, task.StripeID, task.Version, current.StripeID, current.Version)
		return nil
	}
	if !valueMatches(task.Value, current) {
		return fmt.Errorf("read repair %s: verified value no longer matches current metadata", task.Key)
	}

	mode, err := redundancy.ParseMode(current.Mode)
	if err != nil {
		return fmt.Errorf("read repair %s: %w", task.Key, err)
	}
	engine, err := engineFor(mode)
	if err != nil {
		return fmt.Errorf("read repair %s: %w", task.Key, err)
	}
	shards, err := engine.Encode(task.Value, mode)
	if err != nil {
		return fmt.Errorf("read repair %s: encode: %w", task.Key, err)
	}

	placements := make(map[int]storage.ShardPlacement, len(current.Placement))
	for _, placement := range current.Placement {
		placements[placement.ShardIndex] = placement
	}
	shardsByIndex := make(map[int]redundancy.Shard, len(shards))
	for _, shard := range shards {
		shardsByIndex[shard.Index] = shard
	}

	for _, ref := range task.Failed {
		placement, ok := placements[ref.ShardIndex]
		if !ok {
			continue
		}
		if placement.NodeID != ref.NodeID {
			// The deterministic placement changed or the task is stale. Never
			// write a shard to an owner that metadata does not name.
			continue
		}
		shard, ok := shardsByIndex[ref.ShardIndex]
		if !ok {
			continue
		}
		if err := shard.Verify(); err != nil {
			return fmt.Errorf("read repair %s shard %d: %w", task.Key, ref.ShardIndex, err)
		}

		// Missing fragments use the ordinary upload. Corrupt fragments set
		// replace=true so the transport may overwrite the existing checksum.
		if err := s.pushShardWithReplace(ctx, current.StripeID, shard, placement, ref.Corrupt); err != nil {
			log.Printf("read: repair upload failed key=%s stripe=%s shard=%d owner=%s corrupt=%v error=%v",
				task.Key, current.StripeID, ref.ShardIndex, placement.NodeID, ref.Corrupt, err)
			continue
		}
	}

	// Read repair never clears metadata.degraded. Task 09 owns the final
	// all-five-present check and calls set_meta_status.
	return nil
}
