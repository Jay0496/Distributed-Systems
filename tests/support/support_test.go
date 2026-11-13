package support_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/storage"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestMockStoreConcurrentAccess(t *testing.T) {
	store := support.NewMockStore()
	t.Cleanup(func() { _ = store.Close() })

	const workers = 64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stripe := support.Key("concurrent", i)
			value := support.RandomValue(int64(i), 128)
			if err := store.PutFragment(storage.Fragment{StripeID: stripe, ShardIndex: 0, Data: value}); err != nil {
				t.Errorf("PutFragment(%q): %v", stripe, err)
				return
			}
			got, err := store.GetFragment(stripe, 0)
			if err != nil {
				t.Errorf("GetFragment(%q): %v", stripe, err)
				return
			}
			if !bytes.Equal(got.Data, value) {
				t.Errorf("GetFragment(%q) = %d bytes, want %d", stripe, len(got.Data), len(value))
			}
		}(i)
	}
	wg.Wait()
}

func TestFakeClockAndProber(t *testing.T) {
	clock := support.NewFakeClock(time.Time{})
	start := clock.Now()
	clock.Advance(2 * time.Second)
	if got := clock.Now().Sub(start); got != 2*time.Second {
		t.Fatalf("clock advanced by %s, want 2s", got)
	}

	prober := support.NewFakeProber()
	state, err := prober.Probe(context.Background(), "node2", "node2")
	if err != nil || !state.Healthy {
		t.Fatalf("default probe = %+v, %v", state, err)
	}
	prober.SetHealthy("node2", false)
	if _, err := prober.Probe(context.Background(), "node2", "node2"); err == nil {
		t.Fatal("unhealthy probe succeeded")
	}
	if prober.Calls("node2") != 2 {
		t.Fatalf("calls = %d, want 2", prober.Calls("node2"))
	}
}

func TestDialerAndControlledListener(t *testing.T) {
	listener := support.NewControlledListener(1 << 20)
	srv := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(listener.Listener()) }()
	t.Cleanup(srv.Stop)

	dialer := support.NewDialer()
	dialer.Register("node1", listener.Listener())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialer.Dial(ctx, "node1")
	if err != nil {
		t.Fatalf("dial registered target: %v", err)
	}
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health check = %+v, %v", resp, err)
	}
	_ = conn.Close()

	dialer.Unregister("node1")
	if _, err := dialer.Dial(ctx, "node1"); err == nil {
		t.Fatal("dial of an unregistered target succeeded")
	}

	// Reopening a controlled listener must make the address usable again.
	_ = listener.Close()
	if _, err := listener.DialContext(ctx); err == nil {
		t.Fatal("dial after Close succeeded")
	}
	reopened := listener.Reopen()
	if reopened == nil {
		t.Fatal("Reopen returned a nil listener")
	}
}

func TestClusterHarnessWritesReadsAndRestarts(t *testing.T) {
	c := support.StartCluster(t, 5)
	value := []byte("harness-value")

	resp := c.Put(t, &pb.PutRequest{Key: "harness", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	got, err := c.Get(t, "harness")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got.GetValue(), value) {
		t.Fatalf("Get returned %d bytes, want %d", len(got.GetValue()), len(value))
	}

	meta, err := c.Node(t, c.NonLeaderIDs(t)[0]).Store.GetKey("harness")
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if len(c.FragmentRows(t, c.Nodes[0].ID, meta.StripeID)) > 1 {
		t.Fatal("a node stores more than one fragment for a stripe")
	}

	follower := c.NonLeaderIDs(t)[0]
	c.StopNode(t, follower)
	c.WaitFor(t, 10*time.Second, "stopped node to clear its client", func() bool {
		return c.Node(t, follower).Client == nil
	})
	c.StartNode(t, follower)

	c.WaitFor(t, 15*time.Second, "restarted follower to report leader", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		status, err := c.Node(t, follower).Client.NodeStatus(ctx, &pb.NodeStatusRequest{})
		return err == nil && status.GetLeaderId() != ""
	})
	if _, err := c.Get(t, "harness"); err != nil {
		t.Fatalf("Get after restart: %v", err)
	}
}

func TestMockStoreSentinelErrors(t *testing.T) {
	store := support.NewMockStore()
	if _, err := store.GetFragment("missing", 0); err == nil {
		t.Fatal("missing fragment returned no error")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := store.GetFragment("stripe", 0); err != storage.ErrClosed {
		t.Fatalf("GetFragment after Close = %v, want ErrClosed", err)
	}
	if _, err := store.LastAppliedIndex(); err != storage.ErrClosed {
		t.Fatalf("LastAppliedIndex after Close = %v, want ErrClosed", err)
	}
	if err := store.Ping(); err != storage.ErrClosed {
		t.Fatalf("Ping after Close = %v, want ErrClosed", err)
	}
}
