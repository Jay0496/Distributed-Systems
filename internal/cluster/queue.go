package cluster

import (
	"container/heap"
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// ErrRepairQueueFull reports that the bounded repair queue has no capacity for
// a task of this priority. A read that encounters it still succeeds.
var ErrRepairQueueFull = errors.New("repair queue is full")

// ErrRepairQueueClosed reports an enqueue after Close.
var ErrRepairQueueClosed = errors.New("repair queue is closed")

// Priority orders repair work. Lower values run first.
type Priority int

const (
	// PriorityReadRepair is a shard a client read could not use. It is the most
	// urgent because the client already observed the loss.
	PriorityReadRepair Priority = iota
	// PriorityDegraded is a stripe whose metadata is marked degraded.
	PriorityDegraded
	// PriorityReturning is a best-effort refill for a node that came back.
	PriorityReturning
)

// String renders a priority for logs.
func (p Priority) String() string {
	switch p {
	case PriorityReadRepair:
		return "read_repair"
	case PriorityDegraded:
		return "degraded"
	case PriorityReturning:
		return "returning"
	default:
		return "unknown"
	}
}

// Task is one unit of repair work: a single shard or copy on its intended
// owner. Replace is set for a corrupt-shard report or an explicit force
// request, and allows the uploader to overwrite an existing fragment.
type Task struct {
	Key        string
	StripeID   string
	Version    int64
	ShardIndex int
	Owner      string
	Replace    bool
	Priority   Priority
	Attempts   int
}

// QueueStats is a point-in-time repair queue counter snapshot.
type QueueStats struct {
	Depth        int
	Enqueued     uint64
	Deduplicated uint64
	Upgraded     uint64
	Dropped      uint64
	Retried      uint64
	Completed    uint64
	Failed       uint64
}

// Handler processes one repair task. An error schedules a bounded retry with
// exponential backoff; it never blocks a client read.
type Handler func(context.Context, Task) error

// QueueOptions configures the bounded scheduled repair queue.
type QueueOptions struct {
	// Workers bounds concurrent repair handlers (REPAIR_WORKERS).
	Workers int
	// Capacity bounds queued tasks (default 10,000).
	Capacity int
	// Timeout bounds one handler invocation.
	Timeout time.Duration
	// MaxAttempts bounds retries per task (REPAIR_MAX_ATTEMPTS).
	MaxAttempts int
	// BackoffBase is the first retry delay (default 1s).
	BackoffBase time.Duration
	// BackoffMax caps the exponential retry delay (default 30s).
	BackoffMax time.Duration
	// Jitter adjusts a computed delay. Nil uses +/-20% jitter.
	Jitter func(time.Duration) time.Duration
	// RootContext is the process-lifetime context that bounds each handler
	// invocation. Nil uses context.Background().
	RootContext context.Context
}

type taskKey struct {
	id         string
	shardIndex int
}

func keyFor(task Task) taskKey {
	id := task.StripeID
	if id == "" {
		id = task.Key
	}
	return taskKey{id: id, shardIndex: task.ShardIndex}
}

type queueEntry struct {
	task  Task
	key   taskKey
	seq   uint64
	index int
}

type entryHeap []*queueEntry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if h[i].task.Priority != h[j].task.Priority {
		return h[i].task.Priority < h[j].task.Priority
	}
	return h[i].seq < h[j].seq
}
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *entryHeap) Push(x any) {
	entry := x.(*queueEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*h = old[:n-1]
	return entry
}

// Queue is the bounded, deduplicating, priority-aware repair scheduler shared by
// read repair, the failure detector, the repairer, and explicit repair RPCs.
type Queue struct {
	opts    QueueOptions
	handler Handler

	mu       sync.Mutex
	heap     entryHeap
	queued   map[taskKey]*queueEntry
	inflight map[taskKey]*queueEntry
	timers   map[taskKey]*time.Timer
	seq      uint64
	closed   bool
	paused   bool
	counters QueueStats

	done      chan struct{}
	notify    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewQueue starts a bounded repair worker pool.
func NewQueue(opts QueueOptions, handler Handler) *Queue {
	if opts.Workers <= 0 {
		opts.Workers = 2
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 10000
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = time.Second
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = 30 * time.Second
	}
	if opts.BackoffMax < opts.BackoffBase {
		opts.BackoffMax = opts.BackoffBase
	}
	if opts.Jitter == nil {
		opts.Jitter = func(d time.Duration) time.Duration {
			// +/-20% keeps returning nodes from synchronising a repair storm.
			factor := 0.8 + rand.Float64()*0.4
			return time.Duration(float64(d) * factor)
		}
	}

	q := &Queue{
		opts:     opts,
		handler:  handler,
		queued:   make(map[taskKey]*queueEntry),
		inflight: make(map[taskKey]*queueEntry),
		timers:   make(map[taskKey]*time.Timer),
		done:     make(chan struct{}),
		notify:   make(chan struct{}, 1),
	}
	heap.Init(&q.heap)
	for i := 0; i < opts.Workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

// Enqueue adds a task without blocking. A task already queued or in flight is
// deduplicated; a replace or higher-priority request upgrades it in place. When
// the queue is full, the lowest-priority entry may be dropped to admit more
// urgent work, and a task that is not urgent enough returns ErrRepairQueueFull.
func (q *Queue) Enqueue(task Task) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrRepairQueueClosed
	}

	key := keyFor(task)
	if existing, ok := q.queued[key]; ok {
		q.upgradeLocked(existing, task)
		q.counters.Deduplicated++
		q.mu.Unlock()
		q.signal()
		return nil
	}
	if existing, ok := q.inflight[key]; ok {
		q.upgradeLocked(existing, task)
		q.counters.Deduplicated++
		q.mu.Unlock()
		q.signal()
		return nil
	}

	if len(q.heap) >= q.opts.Capacity {
		worst := q.worstLocked()
		if worst == nil || task.Priority >= worst.task.Priority {
			q.counters.Dropped++
			q.mu.Unlock()
			return ErrRepairQueueFull
		}
		heap.Remove(&q.heap, worst.index)
		delete(q.queued, worst.key)
		q.counters.Dropped++
	}

	q.seq++
	entry := &queueEntry{task: task, key: key, seq: q.seq}
	heap.Push(&q.heap, entry)
	q.queued[key] = entry
	q.counters.Enqueued++
	q.mu.Unlock()
	q.signal()
	return nil
}

// upgradeLocked applies replace and priority upgrades to an existing entry.
func (q *Queue) upgradeLocked(entry *queueEntry, task Task) {
	changed := false
	if task.Replace && !entry.task.Replace {
		entry.task.Replace = true
		changed = true
	}
	if task.Priority < entry.task.Priority {
		entry.task.Priority = task.Priority
		changed = true
		if entry.index >= 0 {
			heap.Fix(&q.heap, entry.index)
		}
	}
	if entry.task.Key == "" {
		entry.task.Key = task.Key
	}
	if entry.task.Version == 0 {
		entry.task.Version = task.Version
	}
	if entry.task.Owner == "" {
		entry.task.Owner = task.Owner
	}
	if changed {
		q.counters.Upgraded++
	}
}

func (q *Queue) worstLocked() *queueEntry {
	var worst *queueEntry
	for _, entry := range q.heap {
		if worst == nil || entry.task.Priority > worst.task.Priority {
			worst = entry
		}
	}
	return worst
}

// SetPaused stops workers from taking new tasks without discarding the queue.
// Losing leadership pauses repair; regaining it resumes and usually triggers an
// immediate sweep.
func (q *Queue) SetPaused(paused bool) {
	q.mu.Lock()
	changed := q.paused != paused
	q.paused = paused
	q.mu.Unlock()
	if changed {
		q.signal()
	}
}

// Stats returns the current depth and lifetime counters.
func (q *Queue) Stats() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	stats := q.counters
	stats.Depth = len(q.heap) + len(q.inflight)
	return stats
}

// Close stops the workers and waits for any handler in flight. It is
// idempotent; enqueues after Close return ErrRepairQueueClosed.
func (q *Queue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		for key, timer := range q.timers {
			timer.Stop()
			delete(q.timers, key)
		}
		q.mu.Unlock()

		close(q.done)
		q.wg.Wait()
	})
}

func (q *Queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *Queue) worker() {
	defer q.wg.Done()

	for {
		entry, ok := q.pop()
		if !ok {
			return
		}

		root := q.opts.RootContext
		if root == nil {
			root = context.Background()
		}
		ctx, cancel := context.WithTimeout(root, q.opts.Timeout)
		var err error
		if q.handler != nil {
			err = q.handler(ctx, entry.task)
		}
		cancel()

		q.finish(entry, err)
	}
}

func (q *Queue) pop() (*queueEntry, bool) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, false
		}
		if !q.paused && len(q.heap) > 0 {
			entry := heap.Pop(&q.heap).(*queueEntry)
			delete(q.queued, entry.key)
			q.inflight[entry.key] = entry
			q.mu.Unlock()
			return entry, true
		}
		q.mu.Unlock()

		select {
		case <-q.notify:
		case <-q.done:
			return nil, false
		}
	}
}

// finish records a handler outcome. Success drops the task; failure either
// schedules a jittered exponential retry or marks the task failed at the
// attempt limit.
func (q *Queue) finish(entry *queueEntry, err error) {
	q.mu.Lock()
	current, ok := q.inflight[entry.key]
	if !ok || current != entry {
		q.mu.Unlock()
		return
	}

	if err == nil {
		delete(q.inflight, entry.key)
		q.counters.Completed++
		q.mu.Unlock()
		return
	}

	entry.task.Attempts++
	if entry.task.Attempts >= q.opts.MaxAttempts {
		delete(q.inflight, entry.key)
		q.counters.Failed++
		q.mu.Unlock()
		return
	}

	delay := q.backoff(entry.task.Attempts)
	q.counters.Retried++
	q.timers[entry.key] = time.AfterFunc(delay, func() {
		q.retryReady(entry)
	})
	q.mu.Unlock()
}

func (q *Queue) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := q.opts.BackoffBase
	for i := 1; i < attempt; i++ {
		if delay >= q.opts.BackoffMax/2 {
			delay = q.opts.BackoffMax
			break
		}
		delay *= 2
	}
	if delay > q.opts.BackoffMax {
		delay = q.opts.BackoffMax
	}
	if q.opts.Jitter != nil {
		delay = q.opts.Jitter(delay)
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (q *Queue) retryReady(entry *queueEntry) {
	q.mu.Lock()
	if q.closed {
		delete(q.inflight, entry.key)
		q.mu.Unlock()
		return
	}
	current, ok := q.inflight[entry.key]
	if !ok || current != entry {
		q.mu.Unlock()
		return
	}
	delete(q.inflight, entry.key)
	delete(q.timers, entry.key)
	heap.Push(&q.heap, entry)
	q.queued[entry.key] = entry
	q.mu.Unlock()
	q.signal()
}
