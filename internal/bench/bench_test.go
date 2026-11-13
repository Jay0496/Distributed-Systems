package bench

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPercentileSortedNearestRank(t *testing.T) {
	samples := []time.Duration{}
	for i := 1; i <= 100; i++ {
		samples = append(samples, time.Duration(i)*time.Millisecond)
	}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0, 1 * time.Millisecond},
		{50, 50 * time.Millisecond},
		{95, 95 * time.Millisecond},
		{99, 99 * time.Millisecond},
		{100, 100 * time.Millisecond},
		{-5, 1 * time.Millisecond},
		{150, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := PercentileSorted(samples, tc.p); got != tc.want {
			t.Errorf("p%v = %s, want %s", tc.p, got, tc.want)
		}
	}
	if got := PercentileSorted(nil, 50); got != 0 {
		t.Errorf("empty percentile = %s", got)
	}
}

func TestPercentileDoesNotMutateInput(t *testing.T) {
	samples := []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	_ = Percentile(samples, 50)
	if samples[0] != 30*time.Millisecond {
		t.Fatalf("input slice was reordered: %v", samples)
	}
}

func TestLoadOptionsValidation(t *testing.T) {
	valid := LoadOptions{Mode: "rs32", Concurrency: 1, Duration: time.Second, ValueSize: 8, Keys: 1}
	if _, err := valid.Normalize(); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	cases := map[string]LoadOptions{
		"zero duration":   {Mode: "rs32", Concurrency: 1, Duration: 0, ValueSize: 8, Keys: 1},
		"bad concurrency": {Mode: "rs32", Concurrency: 0, Duration: time.Second, ValueSize: 8, Keys: 1},
		"bad mode":        {Mode: "triple", Concurrency: 1, Duration: time.Second, ValueSize: 8, Keys: 1},
		"bad read ratio":  {Mode: "rs32", Concurrency: 1, Duration: time.Second, ValueSize: 8, Keys: 1, ReadRatio: 2},
	}
	for name, opts := range cases {
		if _, err := opts.Normalize(); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestWorkerKeysDisjointAndDeterministic(t *testing.T) {
	seen := map[int]bool{}
	for worker := 0; worker < 8; worker++ {
		keys := workerKeys(worker, 8, 100, false)
		if len(keys) == 0 {
			t.Fatalf("worker %d has no keys", worker)
		}
		for _, key := range keys {
			if seen[key] {
				t.Fatalf("key %d assigned twice", key)
			}
			seen[key] = true
		}
	}
	if len(seen) != 100 {
		t.Fatalf("covered %d keys, want 100", len(seen))
	}

	hot := workerKeys(0, 8, 5, true)
	if len(hot) != 5 {
		t.Fatalf("hot keys = %d, want 5", len(hot))
	}
}

func TestGenerateValuesDeterministic(t *testing.T) {
	first := generateValues(3, 16, 42)
	second := generateValues(3, 16, 42)
	for i := range first {
		if string(first[i]) != string(second[i]) {
			t.Fatalf("value %d differs for the same seed", i)
		}
	}
	other := generateValues(3, 16, 43)
	if string(first[0]) == string(other[0]) {
		t.Fatal("different seeds produced identical values")
	}
}

func TestLoadRunnerRecordsWritesAndReads(t *testing.T) {
	api := &fakeAPI{}
	nodes := fakeNodes(3, api)
	runner, err := NewLoadRunner(nodes, LoadOptions{
		Mode:        "rs32",
		Concurrency: 2,
		Duration:    60 * time.Millisecond,
		ValueSize:   16,
		Keys:        40,
		ReadRatio:   0.5,
		Seed:        7,
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewLoadRunner: %v", err)
	}
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Put.Count == 0 {
		t.Fatalf("no successful puts: %+v", result)
	}
	if result.Get.Count == 0 {
		t.Fatalf("no successful gets: %+v", result)
	}
	if result.TotalErrors != 0 {
		t.Fatalf("unexpected errors: %+v", result.Put.ErrorKinds)
	}
	if result.Put.P50 == 0 || result.Put.P99 == 0 {
		t.Fatalf("latency percentiles not populated: %+v", result.Put)
	}
}

func TestLoadRunnerCountsRetries(t *testing.T) {
	api := &fakeAPI{unavail: 1}
	runner, err := NewLoadRunner(fakeNodes(3, api), LoadOptions{
		Mode:        "rs32",
		Concurrency: 1,
		Duration:    40 * time.Millisecond,
		ValueSize:   8,
		Keys:        1,
		ReadRatio:   0,
		Seed:        1,
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewLoadRunner: %v", err)
	}
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Retries == 0 {
		t.Fatalf("expected a counted retry, got %+v", result)
	}
}

func TestLoadRunnerCancellation(t *testing.T) {
	api := &fakeAPI{block: make(chan struct{})}
	defer close(api.block)
	runner, err := NewLoadRunner(fakeNodes(2, api), LoadOptions{
		Mode:        "rs32",
		Concurrency: 2,
		Duration:    time.Hour,
		ValueSize:   8,
		Keys:        2,
		ReadRatio:   0,
		Seed:        1,
		Timeout:     time.Hour,
	})
	if err != nil {
		t.Fatalf("NewLoadRunner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := runner.Run(ctx); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}
