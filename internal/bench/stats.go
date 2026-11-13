package bench

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/status"
)

// OpStats is one operation's latency, throughput, and error summary.
type OpStats struct {
	Operation      string           `json:"operation"`
	Attempts       int64            `json:"attempts"`
	Count          int64            `json:"count"`
	Errors         int64            `json:"errors"`
	P50            time.Duration    `json:"-"`
	P95            time.Duration    `json:"-"`
	P99            time.Duration    `json:"-"`
	Max            time.Duration    `json:"-"`
	Mean           time.Duration    `json:"-"`
	OpsPerSecond   float64          `json:"ops_per_sec"`
	BytesPerSecond float64          `json:"bytes_per_sec"`
	ErrorKinds     map[string]int64 `json:"error_kinds,omitempty"`
}

// ErrorRate reports failed attempts as a fraction of all attempts.
func (s OpStats) ErrorRate() float64 {
	if s.Attempts == 0 {
		return 0
	}
	return float64(s.Errors) / float64(s.Attempts)
}

// PercentileSorted returns the nearest-rank percentile of an ascending slice.
// Rank is ceil(p/100*n), clamped to [1, n]. p is clamped to [0, 100]. An empty
// slice returns zero. Nearest-rank is used instead of interpolation because it
// always returns an observed latency and is easy to reproduce from a result CSV.
func PercentileSorted(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}
	rank := int(math.Ceil(p / 100 * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// Percentile computes a percentile from an unsorted sample slice without
// modifying the caller's slice.
func Percentile(samples []time.Duration, p float64) time.Duration {
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return PercentileSorted(sorted, p)
}

// SampleSet is the raw input for one operation's summary.
type SampleSet struct {
	Latencies []time.Duration
	Attempts  int64
	Count     int64
	Errors    int64
	Bytes     int64
	ErrorKinds map[string]int64
}

// BuildOpStats computes percentiles from a raw sample set. operation names the
// row (put or get); measured is the wall-clock window used for throughput and
// excludes warmup.
func BuildOpStats(operation string, samples SampleSet, measured time.Duration) OpStats {
	stats := OpStats{
		Operation:  operation,
		Attempts:   samples.Attempts,
		Count:      samples.Count,
		Errors:     samples.Errors,
		ErrorKinds: copyErrorKinds(samples.ErrorKinds),
	}
	if len(samples.Latencies) > 0 {
		sorted := append([]time.Duration(nil), samples.Latencies...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		stats.P50 = PercentileSorted(sorted, 50)
		stats.P95 = PercentileSorted(sorted, 95)
		stats.P99 = PercentileSorted(sorted, 99)
		stats.Max = sorted[len(sorted)-1]
		stats.Mean = meanDuration(sorted)
	}
	if measured > 0 {
		stats.OpsPerSecond = float64(stats.Count) / measured.Seconds()
		stats.BytesPerSecond = float64(samples.Bytes) / measured.Seconds()
	}
	return stats
}

func meanDuration(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	var total int64
	for _, sample := range sorted {
		total += int64(sample)
	}
	return time.Duration(total / int64(len(sorted)))
}

func copyErrorKinds(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// ClassifyError maps a gRPC error to a stable category. The category is the
// gRPC code name when available, otherwise "unknown".
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	code := grpcCodeName(err)
	if code == "" {
		return "unknown"
	}
	return code
}

// grpcCodeName returns the status code name for an RPC error.
func grpcCodeName(err error) string {
	code := status.Code(err)
	if code == 0 {
		// A plain Go context cancellation is not a gRPC status; classify it
		// with the same names used by the wire protocol.
		if err != nil {
			return "unknown"
		}
		return ""
	}
	return code.String()
}

// ClassifyPutResponse maps an application-level Put rejection to a stable
// message category. Put returns success=false for validation, quorum, conflict,
// and commit failures rather than a gRPC error.
func ClassifyPutResponse(resp *pb.PutResponse) string {
	if resp == nil {
		return "nil_response"
	}
	message := strings.ToLower(strings.TrimSpace(resp.GetMessage()))
	switch {
	case strings.Contains(message, "conflict"), strings.Contains(message, "version"):
		return "conflict"
	case strings.Contains(message, "quorum"), strings.Contains(message, "stored"),
		strings.Contains(message, "shards"):
		return "quorum"
	case strings.Contains(message, "invalid"), strings.Contains(message, "required"),
		strings.Contains(message, "must"), strings.Contains(message, "too large"),
		strings.Contains(message, "empty"):
		return "invalid"
	case strings.Contains(message, "timeout"), strings.Contains(message, "timed out"),
		strings.Contains(message, "deadline"):
		return "timeout"
	case strings.Contains(message, "not configured"):
		return "not_configured"
	default:
		return "rejected"
	}
}

// FormatDuration renders a duration in the report-friendly millisecond form.
func FormatDuration(d time.Duration) string {
	return fmt.Sprintf("%.3f", float64(d)/float64(time.Millisecond))
}
