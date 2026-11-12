package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

// RepairerOptions configures the leader-side reconstruction loop.
type RepairerOptions struct {
	Store      storage.Store
	Reader     ValueReader
	Uploader   ShardUploader
	Statter    ShardStatter
	MetaStatus MetaStatusWriter
	Placer     redundancy.Placer
	// Nodes is the fixed, sorted five-node membership.
	Nodes []string
	// SelfNode is the local node ID.
	SelfNode string
	// MaxAttempts is informational here; the queue owns retry limits.
	MaxAttempts int
	// SweepInterval is the metadata sweep period.
	SweepInterval time.Duration
	// RateLimit bounds uploads per owner per second.
	RateLimit int
	// Logf receives structured repair log lines. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Repairer reconstructs missing shards and restores deterministic placement.
// It is started only on every node for the local orphan sweep; all metadata
// reconstruction remains leader-gated by the detector.
type Repairer struct {
	opts  RepairerOptions
	queue *Queue
	limit *nodeLimiter

	mu       sync.RWMutex
	detector *Detector

	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	active    atomic.Bool

	orphansDeleted atomic.Uint64
}

// NewRepairer builds a repairer. The queue is attached separately so the queue
// handler and the repairer can refer to each other without a constructor cycle.
func NewRepairer(opts RepairerOptions) *Repairer {
	if opts.Placer == nil {
		opts.Placer = redundancy.NewPlacer()
	}
	if opts.SweepInterval <= 0 {
		opts.SweepInterval = time.Minute
	}
	if opts.RateLimit <= 0 {
		opts.RateLimit = 20
	}
	if opts.Logf == nil {
		opts.Logf = log.Printf
	}
	if opts.Nodes == nil {
		opts.Nodes = []string{}
	}

	r := &Repairer{
		opts:  opts,
		limit: newNodeLimiter(opts.RateLimit),
	}
	r.active.Store(true)
	return r
}

// SetQueue attaches the shared queue. It must be called before Start.
func (r *Repairer) SetQueue(queue *Queue) {
	r.queue = queue
}

// SetDetector attaches the leader-gated failure detector.
func (r *Repairer) SetDetector(detector *Detector) {
	r.mu.Lock()
	r.detector = detector
	r.mu.Unlock()
}

// Start begins the detector event loop and the leader metadata sweep. It is
// idempotent.
func (r *Repairer) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		r.ctx = runCtx
		r.cancel = cancel

		r.mu.RLock()
		detector := r.detector
		r.mu.RUnlock()
		if detector != nil {
			detector.Start(runCtx)
			r.wg.Add(1)
			go r.eventLoop(runCtx, detector)
		}

		r.wg.Add(1)
		go r.sweepLoop(runCtx)
	})
}

// Stop halts the detector and every background loop. It is idempotent.
func (r *Repairer) Stop() {
	r.stopOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
		r.mu.RLock()
		detector := r.detector
		r.mu.RUnlock()
		if detector != nil {
			detector.Stop()
		}
	})
}

// Active reports whether this node may perform reconstruction.
func (r *Repairer) Active() bool {
	r.mu.RLock()
	detector := r.detector
	r.mu.RUnlock()
	if detector != nil {
		return r.active.Load() && detector.Active()
	}
	return r.active.Load()
}

// OrphansDeleted reports how many local orphan fragments this process removed.
func (r *Repairer) OrphansDeleted() uint64 {
	return r.orphansDeleted.Load()
}

// Process restores one shard or copy. It is the shared queue handler for read
// repair, detector-driven work, and explicit repair requests.
func (r *Repairer) Process(ctx context.Context, task Task) error {
	if !r.Active() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	meta, err := r.opts.Store.GetKey(task.Key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("repair %s: read metadata: %w", task.Key, err)
	}
	if task.StripeID != "" && meta.StripeID != task.StripeID {
		return nil
	}
	if task.Version != 0 && meta.Version != task.Version {
		return nil
	}

	placement, ok := placementFor(meta, task.ShardIndex)
	if !ok {
		return nil
	}
	owner := placement.NodeID
	if owner == "" {
		return nil
	}

	present, _, err := r.opts.Statter.StatShard(ctx, owner, meta.StripeID, task.ShardIndex)
	if err != nil {
		return fmt.Errorf("repair %s shard %d: stat owner %s: %w", task.Key, task.ShardIndex, owner, err)
	}
	if present && !task.Replace {
		return r.clearDegradedIfComplete(ctx, meta)
	}

	value, err := r.opts.Reader.ReadVerified(ctx, meta)
	if err != nil {
		return fmt.Errorf("repair %s shard %d: read verified value: %w", task.Key, task.ShardIndex, err)
	}
	if int64(len(value)) != meta.Size || redundancy.Checksum(value) != meta.Checksum {
		return fmt.Errorf("repair %s shard %d: verified value does not match committed metadata", task.Key, task.ShardIndex)
	}

	shards, err := encodeValue(meta.Mode, value)
	if err != nil {
		return fmt.Errorf("repair %s shard %d: re-encode: %w", task.Key, task.ShardIndex, err)
	}
	shard, ok := shardFor(shards, task.ShardIndex)
	if !ok {
		return fmt.Errorf("repair %s shard %d: encoder omitted the shard", task.Key, task.ShardIndex)
	}
	if err := shard.Verify(); err != nil {
		return fmt.Errorf("repair %s shard %d: %w", task.Key, task.ShardIndex, err)
	}

	if err := r.limit.wait(ctx, owner); err != nil {
		return err
	}
	if err := r.opts.Uploader.UploadShard(ctx, owner, meta.StripeID, shard, task.Replace); err != nil {
		return fmt.Errorf("repair %s shard %d: upload to %s: %w", task.Key, task.ShardIndex, owner, err)
	}
	r.opts.Logf("repair: uploaded key=%s stripe=%s shard=%d owner=%s replace=%v",
		task.Key, meta.StripeID, task.ShardIndex, owner, task.Replace)

	return r.clearDegradedIfComplete(ctx, meta)
}

// clearDegradedIfComplete clears the degraded flag only when all five intended
// placements hold the shard checksum for this stripe version.
func (r *Repairer) clearDegradedIfComplete(ctx context.Context, meta storage.KeyMeta) error {
	if !meta.Degraded {
		return nil
	}
	if r.opts.MetaStatus == nil {
		return nil
	}

	// Re-encode from a verified read so the expected checksum is authoritative.
	value, readErr := r.opts.Reader.ReadVerified(ctx, meta)
	if readErr != nil {
		return nil
	}
	if int64(len(value)) != meta.Size || redundancy.Checksum(value) != meta.Checksum {
		return nil
	}
	shards, err := encodeValue(meta.Mode, value)
	if err != nil {
		return nil
	}

	for _, placement := range meta.Placement {
		expected, ok := shardFor(shards, placement.ShardIndex)
		if !ok {
			return nil
		}
		present, checksum, statErr := r.opts.Statter.StatShard(ctx, placement.NodeID, meta.StripeID, placement.ShardIndex)
		if statErr != nil || !present || !strings.EqualFold(checksum, expected.Checksum) {
			return nil
		}
	}

	if err := r.opts.MetaStatus.SetDegraded(ctx, meta.Key, meta.StripeID, meta.Version, false); err != nil {
		return fmt.Errorf("repair %s: clear degraded: %w", meta.Key, err)
	}
	r.opts.Logf("repair: cleared degraded key=%s stripe=%s version=%d", meta.Key, meta.StripeID, meta.Version)
	return nil
}

func (r *Repairer) eventLoop(ctx context.Context, detector *Detector) {
	defer r.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-detector.Events():
			if !ok {
				return
			}
			switch event.Kind {
			case EventNodeDown:
				r.enqueuePlacedOn(ctx, event.NodeID, PriorityDegraded)
			case EventNodeUp:
				r.enqueuePlacedOn(ctx, event.NodeID, PriorityReturning)
			case EventLeaderGained:
				r.active.Store(true)
				if r.queue != nil {
					r.queue.SetPaused(false)
				}
				r.enqueueDegraded(ctx)
			case EventLeaderLost:
				r.active.Store(false)
				if r.queue != nil {
					r.queue.SetPaused(true)
				}
			}
		}
	}
}

func (r *Repairer) sweepLoop(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.opts.SweepInterval)
	defer ticker.Stop()

	r.enqueueDegraded(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.Active() {
				r.enqueueDegraded(ctx)
			}
		}
	}
}

func (r *Repairer) enqueueDegraded(ctx context.Context) {
	if r.queue == nil {
		return
	}
	keys, err := r.opts.Store.ListKeys()
	if err != nil {
		r.opts.Logf("repair: list keys for degraded sweep: %v", err)
		return
	}
	for _, meta := range keys {
		if err := ctx.Err(); err != nil {
			return
		}
		if !meta.Degraded {
			continue
		}
		for _, placement := range meta.Placement {
			r.enqueue(meta, placement, PriorityDegraded)
		}
	}
}

func (r *Repairer) enqueuePlacedOn(ctx context.Context, nodeID string, priority Priority) {
	if r.queue == nil || nodeID == "" {
		return
	}
	keys, err := r.opts.Store.ListKeys()
	if err != nil {
		r.opts.Logf("repair: list keys for node %s: %v", nodeID, err)
		return
	}
	for _, meta := range keys {
		if err := ctx.Err(); err != nil {
			return
		}
		for _, placement := range meta.Placement {
			if placement.NodeID == nodeID {
				r.enqueue(meta, placement, priority)
			}
		}
	}
}

func (r *Repairer) enqueue(meta storage.KeyMeta, placement storage.ShardPlacement, priority Priority) {
	if r.queue == nil {
		return
	}
	err := r.queue.Enqueue(Task{
		Key:        meta.Key,
		StripeID:   meta.StripeID,
		Version:    meta.Version,
		ShardIndex: placement.ShardIndex,
		Owner:      placement.NodeID,
		Priority:   priority,
	})
	if err != nil && !errors.Is(err, ErrRepairQueueFull) && !errors.Is(err, ErrRepairQueueClosed) {
		r.opts.Logf("repair: enqueue key=%s stripe=%s shard=%d: %v", meta.Key, meta.StripeID, placement.ShardIndex, err)
	}
}

func placementFor(meta storage.KeyMeta, shardIndex int) (storage.ShardPlacement, bool) {
	for _, placement := range meta.Placement {
		if placement.ShardIndex == shardIndex {
			return placement, true
		}
	}
	return storage.ShardPlacement{}, false
}

func shardFor(shards []redundancy.Shard, index int) (redundancy.Shard, bool) {
	for _, shard := range shards {
		if shard.Index == index {
			return shard, true
		}
	}
	return redundancy.Shard{}, false
}

func encodeValue(mode string, value []byte) ([]redundancy.Shard, error) {
	parsed, err := redundancy.ParseMode(mode)
	if err != nil {
		return nil, err
	}
	switch parsed {
	case redundancy.ModeRS32:
		return redundancy.NewRS32Engine().Encode(value, parsed)
	case redundancy.ModeReplication:
		return redundancy.NewReplicationEngine().Encode(value, parsed)
	default:
		return nil, fmt.Errorf("unsupported redundancy mode %q", mode)
	}
}

// nodeLimiter is a small per-owner token bucket. It limits the upload rate
// without adding a dependency or letting one returning node saturate repair.
type nodeLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     map[string]time.Time
}

func newNodeLimiter(perSecond int) *nodeLimiter {
	if perSecond <= 0 {
		perSecond = 20
	}
	return &nodeLimiter{
		interval: time.Second / time.Duration(perSecond),
		next:     make(map[string]time.Time),
	}
}

func (l *nodeLimiter) wait(ctx context.Context, nodeID string) error {
	if l == nil || l.interval <= 0 {
		return ctx.Err()
	}

	l.mu.Lock()
	now := time.Now()
	at := l.next[nodeID]
	if at.Before(now) {
		at = now
	}
	l.next[nodeID] = at.Add(l.interval)
	l.mu.Unlock()

	delay := time.Until(at)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
