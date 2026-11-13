package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteRunAndReportRoundTrip(t *testing.T) {
	dir := t.TempDir()
	artifacts := sampleArtifacts()
	if err := WriteRun(dir, artifacts); err != nil {
		t.Fatalf("WriteRun: %v", err)
	}
	for _, name := range []string{"meta.json", "run.json", "latency.csv", "throughput.csv", "storage.csv", "recovery.csv", "cpu.csv", "summary.md"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("missing %s: %v", name, err)
		}
	}
	if err := RegenerateSummary(dir); err != nil {
		t.Fatalf("RegenerateSummary: %v", err)
	}
	summary, err := os.ReadFile(filepath.Join(dir, "summary.md"))
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if !strings.Contains(string(summary), "1.667x") || !strings.Contains(string(summary), "5.000x") {
		t.Fatalf("summary missing storage ratios:\n%s", summary)
	}
}

func sampleArtifacts() RunArtifacts {
	return RunArtifacts{
		Meta: Meta{
			RunID:     "test-run",
			Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			GitCommit: "abc123",
			GoVersion: "go1.22",
			GOOS:      "windows",
			GOARCH:    "amd64",
			CPUCount:  8,
			Hostname:  "bench-host",
			Flags:     map[string]string{"command": "run"},
		},
		Load: &LoadResult{
			Put: OpStats{Operation: "put", Count: 100, Attempts: 100, P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 3 * time.Millisecond, Max: 4 * time.Millisecond, OpsPerSecond: 1000},
			Get: OpStats{Operation: "get", Count: 100, Attempts: 100, P50: time.Millisecond, P95: 2 * time.Millisecond, P99: 3 * time.Millisecond, Max: 4 * time.Millisecond, OpsPerSecond: 1000},
		},
		Storage: []StorageResult{
			{Mode: "rs32", Keys: 10, ValueSize: 100, LogicalBytes: 1000, FragmentBytes: 1667, DiskBytes: 1700, PayloadRatio: 1.667, DiskRatio: 1.7},
			{Mode: "replication", Keys: 10, ValueSize: 100, LogicalBytes: 1000, FragmentBytes: 5000, DiskBytes: 5100, PayloadRatio: 5, DiskRatio: 5.1},
		},
		Recovery: []RecoveryResult{{
			Scenario:       "one-node",
			StartTime:      time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC),
			DegradedAt:     time.Date(2026, 1, 2, 3, 0, 2, 0, time.UTC),
			RestartTime:    time.Date(2026, 1, 2, 3, 0, 10, 0, time.UTC),
			RepairedAt:     time.Date(2026, 1, 2, 3, 0, 15, 0, time.UTC),
			TimeToDegraded: 2 * time.Second,
			RepairDuration: 5 * time.Second,
			TotalDuration:  15 * time.Second,
			HealthyNodes:   5, TotalNodes: 5,
		}},
		CPU: []CPUStat{{Node: "node1", AvgCPUPercent: 12.5, Samples: 3}},
	}
}
