package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestQueueDeduplicatesByStripeAndShard(t *testing.T) {
	q := NewQueue(QueueOptions{Workers: 1, Capacity: 8, Jitter: func(d time.Duration) time.Duration { return d }}, nil)
	defer q.Close()

	if err := q.Enqueue(Task{StripeID: "s1", ShardIndex: 1, Priority: PriorityReturning}); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := q.Enqueue(Task{StripeID: "s1", ShardIndex: 1, Priority: PriorityReturning, Replace: true}); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	stats := q.Stats()
	if stats.Deduplicated != 1 {
		t.Fatalf("deduplicated = %d, want 1", stats.Deduplicated)
	}
	if stats.Upgraded != 1 {
		t.Fatalf("upgraded = %d, want 1", stats.Upgraded)
	}
}

func TestQueueRunsByPriority(t *testing.T) {
	var (
		mu    sync.Mutex
		order []Priority
	)
	// One worker is required for a strict global order: with multiple workers,
	// two tasks can be popped in priority order but finish in either order.
	q := NewQueue(QueueOptions{Workers: 1, Capacity: 8, Jitter: func(d time.Duration) time.Duration { return d }},
		func(_ context.Context, task Task) error {
			mu.Lock()
			order = append(order, task.Priority)
			mu.Unlock()
			return nil
		})
	defer q.Close()
	q.SetPaused(true)

	if err := q.Enqueue(Task{StripeID: "low", ShardIndex: 0, Priority: PriorityReturning}); err != nil {
		t.Fatalf("enqueue returning: %v", err)
	}
	if err := q.Enqueue(Task{StripeID: "urgent", ShardIndex: 0, Priority: PriorityReadRepair}); err != nil {
		t.Fatalf("enqueue read repair: %v", err)
	}
	if err := q.Enqueue(Task{StripeID: "mid", ShardIndex: 0, Priority: PriorityDegraded}); err != nil {
		t.Fatalf("enqueue degraded: %v", err)
	}
	q.SetPaused(false)

	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	if order[0] != PriorityReadRepair || order[1] != PriorityDegraded || order[2] != PriorityReturning {
		t.Fatalf("order = %v, want read repair, degraded, returning", order)
	}
}

func TestQueueOverflowDropsLowestPriority(t *testing.T) {
	q := NewQueue(QueueOptions{Workers: 1, Capacity: 2, Jitter: func(d time.Duration) time.Duration { return d }}, nil)
	defer q.Close()
	q.SetPaused(true)

	if err := q.Enqueue(Task{StripeID: "a", ShardIndex: 0, Priority: PriorityReturning}); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if err := q.Enqueue(Task{StripeID: "b", ShardIndex: 0, Priority: PriorityReturning}); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}
	if err := q.Enqueue(Task{StripeID: "urgent", ShardIndex: 0, Priority: PriorityReadRepair}); err != nil {
		t.Fatalf("enqueue urgent: %v", err)
	}
	stats := q.Stats()
	if stats.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", stats.Dropped)
	}
	if stats.Depth != 2 {
		t.Fatalf("depth = %d, want 2", stats.Depth)
	}

	// The queue now holds one urgent and one returning entry; another
	// returning task is not urgent enough to displace either.
	if err := q.Enqueue(Task{StripeID: "c", ShardIndex: 0, Priority: PriorityReturning}); !errors.Is(err, ErrRepairQueueFull) {
		t.Fatalf("enqueue c = %v, want ErrRepairQueueFull", err)
	}
}

func TestQueueRetriesWithBackoff(t *testing.T) {
	var attempts atomic.Int32
	q := NewQueue(QueueOptions{
		Workers:     1,
		Capacity:    4,
		MaxAttempts: 3,
		BackoffBase: time.Millisecond,
		BackoffMax:  5 * time.Millisecond,
		Jitter:      func(d time.Duration) time.Duration { return d },
	}, func(context.Context, Task) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	defer q.Close()

	if err := q.Enqueue(Task{StripeID: "retry", ShardIndex: 0}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return q.Stats().Completed == 1 })
	stats := q.Stats()
	if stats.Retried != 1 {
		t.Fatalf("retried = %d, want 1", stats.Retried)
	}
	if stats.Failed != 0 {
		t.Fatalf("failed = %d, want 0", stats.Failed)
	}
}

func TestQueueMarksFailedAtAttemptLimit(t *testing.T) {
	q := NewQueue(QueueOptions{
		Workers:     1,
		Capacity:    4,
		MaxAttempts: 2,
		BackoffBase: time.Millisecond,
		BackoffMax:  time.Millisecond,
		Jitter:      func(d time.Duration) time.Duration { return d },
	}, func(context.Context, Task) error {
		return errors.New("permanent")
	})
	defer q.Close()

	if err := q.Enqueue(Task{StripeID: "fail", ShardIndex: 0}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return q.Stats().Failed == 1 })
	if q.Stats().Depth != 0 {
		t.Fatalf("depth = %d after failure, want 0", q.Stats().Depth)
	}
}

func TestQueueConcurrentEnqueue(t *testing.T) {
	var handled atomic.Int32
	q := NewQueue(QueueOptions{Workers: 4, Capacity: 256, Jitter: func(d time.Duration) time.Duration { return d }},
		func(context.Context, Task) error {
			handled.Add(1)
			return nil
		})
	defer q.Close()

	const total = 100
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = q.Enqueue(Task{StripeID: "concurrent", ShardIndex: i})
		}(i)
	}
	wg.Wait()
	waitFor(t, 5*time.Second, func() bool { return handled.Load() == total })
}

func TestQueueClosedRejectsNewWork(t *testing.T) {
	q := NewQueue(QueueOptions{Workers: 1, Capacity: 1}, nil)
	q.Close()
	q.Close()
	if err := q.Enqueue(Task{StripeID: "late"}); !errors.Is(err, ErrRepairQueueClosed) {
		t.Fatalf("enqueue after close = %v, want ErrRepairQueueClosed", err)
	}
}
