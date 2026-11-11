package cluster

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrRepairQueueFull reports that the bounded repair queue has no capacity. A
// read that encounters it still succeeds; the repair is left for task 09's
// background repair loop.
var ErrRepairQueueFull = errors.New("repair queue is full")

// ErrRepairQueueClosed reports an enqueue after Close.
var ErrRepairQueueClosed = errors.New("repair queue is closed")

// ShardRef identifies one shard or copy that a read could not use. Corrupt
// distinguishes a checksum failure from a missing or unreachable owner.
type ShardRef struct {
	ShardIndex int
	NodeID     string
	Corrupt    bool
}

// RepairTask is one asynchronous read-repair request. Value is the verified
// value that produced it, so the worker can re-encode without another read.
type RepairTask struct {
	Key      string
	StripeID string
	Version  int64
	Failed   []ShardRef
	Value    []byte
}

// RepairHandler performs one repair task. Returning an error is logged by the
// worker and never affects the client read that scheduled the task.
type RepairHandler func(context.Context, RepairTask) error

// RepairQueue is the bounded asynchronous repair seam. Task 09 replaces the
// in-process implementation with the shared scheduled repair queue.
type RepairQueue interface {
	Enqueue(RepairTask) error
	Close()
}

// WorkQueue is a bounded in-process RepairQueue with a fixed worker pool.
type WorkQueue struct {
	tasks   chan RepairTask
	done    chan struct{}
	handler RepairHandler
	timeout time.Duration

	closeOnce sync.Once
	mu        sync.RWMutex
	closed    bool
	wg        sync.WaitGroup

	// onError receives worker errors. Tests replace it to observe outcomes.
	onError func(error)
}

var _ RepairQueue = (*WorkQueue)(nil)

// NewRepairQueue starts workers consumers of a bounded queue. Capacity and
// workers fall back to values that keep failures from blocking reads.
func NewRepairQueue(workers, capacity int, timeout time.Duration, handler RepairHandler) *WorkQueue {
	if workers <= 0 {
		workers = 1
	}
	if capacity <= 0 {
		capacity = 64
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	q := &WorkQueue{
		tasks:   make(chan RepairTask, capacity),
		done:    make(chan struct{}),
		handler: handler,
		timeout: timeout,
	}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

// Enqueue adds a task without blocking. A full or closed queue returns an
// error; callers must not fail the read because repair is best effort.
func (q *WorkQueue) Enqueue(task RepairTask) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrRepairQueueClosed
	}
	select {
	case q.tasks <- task:
		return nil
	default:
		return ErrRepairQueueFull
	}
}

// Close stops the workers and waits for the task in flight. It is idempotent.
func (q *WorkQueue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		close(q.done)
		q.mu.Unlock()
		q.wg.Wait()
	})
}

func (q *WorkQueue) worker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.done:
			return
		case task := <-q.tasks:
			ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
			var err error
			if q.handler != nil {
				err = q.handler(ctx, task)
			}
			cancel()
			if err != nil && q.onError != nil {
				q.onError(err)
			}
		}
	}
}
