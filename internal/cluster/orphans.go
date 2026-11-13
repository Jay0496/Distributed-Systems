package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"distributed-kv-store/internal/storage"
)

// OrphanSweeperOptions configures the node-local orphan fragment sweep.
type OrphanSweeperOptions struct {
	Store storage.Store
	// SelfNode is the local node ID. A fragment is an orphan when metadata
	// places its shard on a different node.
	SelfNode string
	// GracePeriod protects recently uploaded fragments from an in-flight write
	// whose metadata has not committed yet (ORPHAN_GRACE_PERIOD).
	GracePeriod time.Duration
	// Guard may veto a pass when this node has no known leader or trails the
	// leader's commit index. It is evaluated at the start of every pass.
	Guard func(context.Context) (bool, error)
	// Now is injectable for tests.
	Now func() time.Time
	// Logf receives sweep errors and periodic summaries.
	Logf func(format string, args ...any)
}

// OrphanSweepResult summarises one pass.
type OrphanSweepResult struct {
	Examined int
	Kept     int
	Deleted  int
	Skipped  bool
}

// OrphanSweeper deletes local fragments that no current metadata record
// references. It runs on every node because fragments are node-local; it never
// reconstructs or uploads shards.
type OrphanSweeper struct {
	opts OrphanSweeperOptions
}

// NewOrphanSweeper builds an orphan sweeper with documented defaults.
func NewOrphanSweeper(opts OrphanSweeperOptions) *OrphanSweeper {
	if opts.GracePeriod <= 0 {
		opts.GracePeriod = 10 * time.Minute
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logf == nil {
		opts.Logf = func(format string, args ...any) {
			slog.Info(fmt.Sprintf(format, args...))
		}
	}
	return &OrphanSweeper{opts: opts}
}

// Sweep lists local fragments, keeps anything referenced by current metadata or
// younger than the grace period, and deletes the rest. Metadata is refreshed
// immediately before deletion so a write that committed during the pass keeps
// its fragments.
func (s *OrphanSweeper) Sweep(ctx context.Context) (OrphanSweepResult, error) {
	result := OrphanSweepResult{}
	if s.opts.Store == nil {
		return result, nil
	}
	if s.opts.Guard != nil {
		ok, err := s.opts.Guard(ctx)
		if err != nil {
			return result, err
		}
		if !ok {
			result.Skipped = true
			return result, nil
		}
	}

	refs, err := s.opts.Store.ListFragments("")
	if err != nil {
		return result, err
	}
	if len(refs) == 0 {
		return result, nil
	}

	cutoff := s.opts.Now().Add(-s.opts.GracePeriod)
	candidates := make([]storage.FragmentRef, 0, len(refs))
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Examined++

		// A zero timestamp is conservative: keep the fragment rather than risk
		// deleting data written by a clock-skewed node.
		if ref.CreatedAt.IsZero() || ref.CreatedAt.After(cutoff) {
			result.Kept++
			continue
		}
		candidates = append(candidates, ref)
	}
	if len(candidates) == 0 {
		return result, nil
	}

	// Refresh metadata once before deleting. This closes the common race in
	// which a write uploads fragments, then commits metadata while the sweep is
	// deciding. The predicate is re-evaluated against the fresh snapshot.
	current, err := s.opts.Store.ListKeys()
	if err != nil {
		return result, err
	}
	byStripe := make(map[string]storage.KeyMeta, len(current))
	for _, meta := range current {
		byStripe[meta.StripeID] = meta
	}

	for _, ref := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		meta, ok := byStripe[ref.StripeID]
		if ok && !s.orphan(meta, ref) {
			result.Kept++
			continue
		}
		if err := s.opts.Store.DeleteFragment(ref.StripeID, ref.ShardIndex); err != nil {
			return result, err
		}
		result.Deleted++
		s.opts.Logf("orphan: deleted stripe=%s shard=%d age=%s",
			ref.StripeID, ref.ShardIndex, s.opts.Now().Sub(ref.CreatedAt).Round(time.Second))
	}
	return result, nil
}

// orphan reports whether ref is not the current deterministic placement for its
// stripe. It is deliberately conservative.
func (s *OrphanSweeper) orphan(meta storage.KeyMeta, ref storage.FragmentRef) bool {
	placement, ok := placementFor(meta, ref.ShardIndex)
	if !ok {
		return true
	}
	return placement.NodeID != s.opts.SelfNode
}
