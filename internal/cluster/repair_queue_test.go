package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWorkQueueRunsTasks(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []string
	)
	q := NewRepairQueue(2, 4, time.Second, func(_ context.Context, task RepairTask) error {
		mu.Lock()
		seen = append(seen, task.Key)
		mu.Unlock()
		return nil
	})
	defer q.Close()

	if err := q.Enqueue(RepairTask{Key: "a"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(seen) == 1
		mu.Unlock()
		if done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("handler did not run")
}

func TestWorkQueueIsBounded(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	q := NewRepairQueue(1, 1, time.Minute, func(context.Context, RepairTask) error {
		once.Do(func() { close(started) })
		<-block
		return nil
	})
	defer func() {
		close(block)
		q.Close()
	}()

	if err := q.Enqueue(RepairTask{Key: "running"}); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	<-started
	if err := q.Enqueue(RepairTask{Key: "waiting"}); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if err := q.Enqueue(RepairTask{Key: "dropped"}); !errors.Is(err, ErrRepairQueueFull) {
		t.Fatalf("third enqueue = %v, want ErrRepairQueueFull", err)
	}
}

func TestWorkQueueCloseIsIdempotent(t *testing.T) {
	q := NewRepairQueue(1, 1, time.Second, nil)
	q.Close()
	q.Close()
	if err := q.Enqueue(RepairTask{Key: "late"}); !errors.Is(err, ErrRepairQueueClosed) {
		t.Fatalf("enqueue after close = %v, want ErrRepairQueueClosed", err)
	}
}
