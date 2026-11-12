package integration

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

// TestConcurrentClients runs 50 writer/reader pairs with unique, seeded keys.
// Every goroutine performs its own write and read, so writes and reads overlap
// across clients without relying on a fixed sleep.
func TestConcurrentClients(t *testing.T) {
	support.LogSeed(t, support.DefaultSeed)
	c := support.StartCluster(t, 5)
	leader, err := c.WaitLeader(15 * time.Second)
	if err != nil {
		t.Fatalf("leader: %v", err)
	}

	const clients = 50
	start := make(chan struct{})
	errCh := make(chan error, clients)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			key := support.Key("concurrent-client", i)
			value := support.RandomValue(support.DefaultSeed+int64(i), 256)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			put, err := leader.Client.Put(ctx, &pb.PutRequest{Key: key, Value: value, Mode: "rs32"})
			if err != nil {
				errCh <- fmt.Errorf("Put(%q): %w", key, err)
				return
			}
			if !put.GetSuccess() {
				errCh <- fmt.Errorf("Put(%q) rejected: %s", key, put.GetMessage())
				return
			}
			if put.GetDegraded() {
				errCh <- fmt.Errorf("Put(%q) unexpectedly degraded", key)
				return
			}

			got, err := leader.Client.Get(ctx, &pb.GetRequest{Key: key})
			if err != nil {
				errCh <- fmt.Errorf("Get(%q): %w", key, err)
				return
			}
			if !got.GetSuccess() || !bytes.Equal(got.GetValue(), value) {
				errCh <- fmt.Errorf("Get(%q) returned success=%v and %d bytes", key, got.GetSuccess(), len(got.GetValue()))
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		t.Fatal("concurrent clients failed")
	}
}
