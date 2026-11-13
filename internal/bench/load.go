package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoadOptions configures one load-generator run.
type LoadOptions struct {
	Mode        string
	Concurrency int
	Duration    time.Duration
	Warmup      time.Duration
	ValueSize   int
	Keys        int
	ReadRatio   float64
	HotKeys     bool
	Seed        int64
	Timeout     time.Duration
}

// LoadResult is the measured outcome of one run. Put and Get are separate rows
// so a mixed workload is never averaged into a misleading single latency.
type LoadResult struct {
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Measured     string    `json:"measured_duration"`
	Mode         string    `json:"mode"`
	Concurrency  int       `json:"concurrency"`
	ValueSize    int       `json:"value_size"`
	Keys         int       `json:"keys"`
	ReadRatio    float64   `json:"read_ratio"`
	Warmup       string    `json:"warmup"`
	Nodes        int       `json:"nodes"`
	Degraded     int64     `json:"degraded_writes"`
	Retries      int64     `json:"retries"`
	Put          OpStats   `json:"put"`
	Get          OpStats   `json:"get"`
	TotalErrors  int64     `json:"total_errors"`
	TotalAttempt int64     `json:"total_attempts"`
	ErrorRate    float64   `json:"error_rate"`
}

// Normalize fills defaults and validates a LoadOptions value.
func (o LoadOptions) Normalize() (LoadOptions, error) {
	mode, err := NormalizeMode(o.Mode)
	if err != nil {
		return LoadOptions{}, err
	}
	o.Mode = mode
	if o.Concurrency <= 0 {
		return LoadOptions{}, fmt.Errorf("concurrency must be positive, got %d", o.Concurrency)
	}
	if o.Duration <= 0 {
		return LoadOptions{}, fmt.Errorf("duration must be positive, got %s", o.Duration)
	}
	if o.Warmup < 0 {
		return LoadOptions{}, fmt.Errorf("warmup must not be negative, got %s", o.Warmup)
	}
	if o.ValueSize <= 0 {
		return LoadOptions{}, fmt.Errorf("value size must be positive, got %d", o.ValueSize)
	}
	if o.Keys <= 0 {
		return LoadOptions{}, fmt.Errorf("key count must be positive, got %d", o.Keys)
	}
	if o.ReadRatio < 0 || o.ReadRatio > 1 {
		return LoadOptions{}, fmt.Errorf("read ratio must be between 0 and 1, got %g", o.ReadRatio)
	}
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Second
	}
	return o, nil
}

// workerResult accumulates one worker's samples. Workers own disjoint key
// ranges, so no two workers write the same key unless hot keys were requested.
type workerResult struct {
	putSamples []time.Duration
	getSamples []time.Duration
	putAttempts int64
	getAttempts int64
	putCount   int64
	getCount   int64
	putBytes   int64
	getBytes   int64
	putErrors  map[string]int64
	getErrors  map[string]int64
	degraded   int64
	retries    int64
}

// LoadRunner executes the deterministic mixed workload.
type LoadRunner struct {
	nodes   Nodes
	opts    LoadOptions
	values  [][]byte
	workers int
}

// NewLoadRunner validates options and pre-generates values so allocation cost is
// not part of the measured operation.
func NewLoadRunner(nodes Nodes, opts LoadOptions) (*LoadRunner, error) {
	if err := nodes.Validate(); err != nil {
		return nil, err
	}
	normalized, err := opts.Normalize()
	if err != nil {
		return nil, err
	}
	values := generateValues(normalized.Keys, normalized.ValueSize, normalized.Seed)
	return &LoadRunner{
		nodes:   nodes,
		opts:    normalized,
		values:  values,
		workers: normalized.Concurrency,
	}, nil
}

// Run starts all workers and returns the merged result. Cancelling ctx stops
// workers promptly and returns the partial result together with ctx.Err().
func (r *LoadRunner) Run(ctx context.Context) (*LoadResult, error) {
	if r == nil {
		return nil, errors.New("nil load runner")
	}
	start := time.Now()
	warmupEnd := start.Add(r.opts.Warmup)
	deadline := warmupEnd.Add(r.opts.Duration)

	results := make([]workerResult, r.workers)
	var wg sync.WaitGroup
	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			results[worker] = r.runWorker(ctx, worker, warmupEnd, deadline)
		}(i)
	}
	wg.Wait()
	finished := time.Now()

	measured := deadline.Sub(warmupEnd)
	if finished.Before(deadline) {
		measured = finished.Sub(warmupEnd)
	}
	if measured < 0 {
		measured = 0
	}

	mergedPut := SampleSet{ErrorKinds: map[string]int64{}}
	mergedGet := SampleSet{ErrorKinds: map[string]int64{}}
	var degraded, retries int64
	for _, worker := range results {
		mergedPut.Latencies = append(mergedPut.Latencies, worker.putSamples...)
		mergedGet.Latencies = append(mergedGet.Latencies, worker.getSamples...)
		mergedPut.Attempts += worker.putAttempts
		mergedGet.Attempts += worker.getAttempts
		mergedPut.Count += worker.putCount
		mergedGet.Count += worker.getCount
		mergedPut.Errors += sumErrors(worker.putErrors)
		mergedGet.Errors += sumErrors(worker.getErrors)
		mergedPut.Bytes += worker.putBytes
		mergedGet.Bytes += worker.getBytes
		mergeErrors(mergedPut.ErrorKinds, worker.putErrors)
		mergeErrors(mergedGet.ErrorKinds, worker.getErrors)
		degraded += worker.degraded
		retries += worker.retries
	}
	put := BuildOpStats("put", mergedPut, measured)
	get := BuildOpStats("get", mergedGet, measured)

	result := &LoadResult{
		StartedAt:    start,
		FinishedAt:   finished,
		Measured:     measured.String(),
		Mode:         r.opts.Mode,
		Concurrency:  r.opts.Concurrency,
		ValueSize:    r.opts.ValueSize,
		Keys:         r.opts.Keys,
		ReadRatio:    r.opts.ReadRatio,
		Warmup:       r.opts.Warmup.String(),
		Nodes:        len(r.nodes),
		Degraded:     degraded,
		Retries:      retries,
		Put:          put,
		Get:          get,
		TotalAttempt: put.Attempts + get.Attempts,
		TotalErrors:  put.Errors + get.Errors,
	}
	if result.TotalAttempt > 0 {
		result.ErrorRate = float64(result.TotalErrors) / float64(result.TotalAttempt)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func (r *LoadRunner) runWorker(ctx context.Context, worker int, warmupEnd, deadline time.Time) workerResult {
	result := workerResult{
		putSamples: make([]time.Duration, 0, 4096),
		getSamples: make([]time.Duration, 0, 4096),
		putErrors:  make(map[string]int64),
		getErrors:  make(map[string]int64),
	}
	rng := rand.New(rand.NewSource(r.opts.Seed + int64(worker)*7919))
	keyIndexes := workerKeys(worker, r.workers, r.opts.Keys, r.opts.HotKeys)
	written := make(map[int]bool, len(keyIndexes))
	startNode := worker % len(r.nodes)

	for {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return result
		}
		keyIndex := keyIndexes[rng.Intn(len(keyIndexes))]
		wantRead := rng.Float64() < r.opts.ReadRatio
		// A read of a key this worker has not written yet would measure a
		// NotFound path rather than a read. Write first, then use the key.
		if !written[keyIndex] {
			wantRead = false
		}

		opStart := time.Now()
		record := !opStart.Before(warmupEnd)

		if wantRead {
			key := fmt.Sprintf("key-%d", keyIndex)
			resp, err, retries := r.get(ctx, startNode, key)
			result.retries += retries
			result.getAttempts++
			if record {
				result.getSamples = append(result.getSamples, time.Since(opStart))
			}
			if err != nil {
				result.getErrors[ClassifyError(err)]++
				continue
			}
			if resp == nil {
				result.getErrors["nil_response"]++
				continue
			}
			result.getCount++
			result.getBytes += int64(len(resp.GetValue()))
			continue
		}

		key := fmt.Sprintf("key-%d", keyIndex)
		resp, err, retries := r.put(ctx, startNode, key, r.values[keyIndex])
		result.retries += retries
		result.putAttempts++
		if record {
			result.putSamples = append(result.putSamples, time.Since(opStart))
		}
		if err != nil {
			result.putErrors[ClassifyError(err)]++
			continue
		}
		if resp == nil {
			result.putErrors["nil_response"]++
			continue
		}
		if !resp.GetSuccess() {
			result.putErrors[ClassifyPutResponse(resp)]++
			continue
		}
		result.putCount++
		result.putBytes += int64(len(r.values[keyIndex]))
		written[keyIndex] = true
		if resp.GetDegraded() {
			result.degraded++
		}
	}
}

// put sends a write, retrying only on Unavailable so measurement isolates server
// behaviour from transient leader changes. Every other error is final.
func (r *LoadRunner) put(ctx context.Context, startNode int, key string, value []byte) (*pb.PutResponse, error, int64) {
	var lastErr error
	for attempt := 0; attempt < len(r.nodes); attempt++ {
		node := r.nodes[(startNode+attempt)%len(r.nodes)]
		callCtx, cancel := WithTimeout(ctx, r.opts.Timeout)
		resp, err := node.API.Put(callCtx, &pb.PutRequest{Key: key, Value: value, Mode: r.opts.Mode})
		cancel()
		if err == nil {
			return resp, nil, int64(attempt)
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable {
			return nil, err, int64(attempt)
		}
	}
	return nil, lastErr, int64(len(r.nodes) - 1)
}

// get is the read-side equivalent of put.
func (r *LoadRunner) get(ctx context.Context, startNode int, key string) (*pb.GetResponse, error, int64) {
	var lastErr error
	for attempt := 0; attempt < len(r.nodes); attempt++ {
		node := r.nodes[(startNode+attempt)%len(r.nodes)]
		callCtx, cancel := WithTimeout(ctx, r.opts.Timeout)
		resp, err := node.API.Get(callCtx, &pb.GetRequest{Key: key})
		cancel()
		if err == nil {
			return resp, nil, int64(attempt)
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable {
			return nil, err, int64(attempt)
		}
	}
	return nil, lastErr, int64(len(r.nodes) - 1)
}

// workerKeys assigns a disjoint key range. With hot keys enabled every worker
// shares the whole range, which deliberately measures contention.
func workerKeys(worker, workers, keys int, hot bool) []int {
	if hot {
		keysList := make([]int, keys)
		for i := range keysList {
			keysList[i] = i
		}
		return keysList
	}
	if keys < workers {
		return []int{worker % keys}
	}
	start := keys * worker / workers
	end := keys * (worker + 1) / workers
	if end <= start {
		end = start + 1
	}
	out := make([]int, end-start)
	for i := range out {
		out[i] = start + i
	}
	return out
}

// generateValues pre-generates deterministic random payloads. The seed makes
// checksums and sizes stable across runs.
func generateValues(keys, size int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	values := make([][]byte, keys)
	for i := range values {
		payload := make([]byte, size)
		for j := range payload {
			payload[j] = byte(rng.Intn(256))
		}
		values[i] = payload
	}
	return values
}

func sumErrors(kinds map[string]int64) int64 {
	var total int64
	for _, count := range kinds {
		total += count
	}
	return total
}

func mergeErrors(dst, src map[string]int64) {
	for kind, count := range src {
		dst[kind] += count
	}
}
