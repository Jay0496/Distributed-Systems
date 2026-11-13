//go:build bench

package bench_test

import (
	"context"
	"math"
	"testing"
	"time"

	"distributed-kv-store/internal/bench"
	"distributed-kv-store/tests/support"
)

// clusterNodes adapts the five-node in-process deployment to the benchmark's
// client interface.
func clusterNodes(c *support.Cluster) bench.Nodes {
	nodes := make(bench.Nodes, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		nodes = append(nodes, bench.Node{ID: node.ID, API: node.Client})
	}
	return nodes
}

// TestStorageRatiosAgainstInProcessCluster proves the storage accounting on a
// real SQLite + Raft deployment. Each mode gets a fresh cluster because a
// storage measurement must not include a previous dataset.
func TestStorageRatiosAgainstInProcessCluster(t *testing.T) {
	cases := []struct {
		mode string
		want float64
	}{
		{mode: "rs32", want: 5.0 / 3.0},
		{mode: "replication", want: 5.0},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			cluster := support.StartCluster(t, 5)
			result, err := bench.MeasureStorage(context.Background(), clusterNodes(cluster), bench.DatasetOptions{
				Mode:      tc.mode,
				Values:    8,
				ValueSize: 512,
				Prefix:    "bench-" + tc.mode,
				Seed:      11,
			})
			if err != nil {
				t.Fatalf("MeasureStorage(%s): %v", tc.mode, err)
			}
			if result.LogicalBytes != 8*512 {
				t.Fatalf("logical bytes = %d, want %d", result.LogicalBytes, 8*512)
			}
			if math.Abs(result.PayloadRatio-tc.want) > 0.05 {
				t.Fatalf("%s payload ratio = %.3f, want about %.3f", tc.mode, result.PayloadRatio, tc.want)
			}
			if result.DiskRatio < result.PayloadRatio {
				t.Fatalf("%s disk ratio %.3f < payload ratio %.3f", tc.mode, result.DiskRatio, result.PayloadRatio)
			}
			if result.FragmentBytes <= result.LogicalBytes {
				t.Fatalf("%s fragment bytes %d did not amplify logical bytes %d", tc.mode, result.FragmentBytes, result.LogicalBytes)
			}
		})
	}
}

// TestLoadRunnerAgainstInProcessCluster exercises the real Put/Get paths and
// asserts that a healthy cluster produces no errors.
func TestLoadRunnerAgainstInProcessCluster(t *testing.T) {
	cluster := support.StartCluster(t, 5)
	runner, err := bench.NewLoadRunner(clusterNodes(cluster), bench.LoadOptions{
		Mode:        "rs32",
		Concurrency: 3,
		Duration:    300 * time.Millisecond,
		Warmup:      50 * time.Millisecond,
		ValueSize:   256,
		Keys:        12,
		ReadRatio:   0.5,
		Seed:        5,
		Timeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLoadRunner: %v", err)
	}
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Put.Count == 0 {
		t.Fatalf("no puts completed: %+v", result.Put)
	}
	if result.Get.Count == 0 {
		t.Fatalf("no gets completed: %+v", result.Get)
	}
	if result.TotalErrors != 0 {
		t.Fatalf("healthy cluster reported errors: put=%v get=%v", result.Put.ErrorKinds, result.Get.ErrorKinds)
	}
}
