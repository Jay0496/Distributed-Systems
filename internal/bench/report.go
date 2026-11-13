package bench

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CPUStat is the average CPU usage of one node while a scenario ran.
type CPUStat struct {
	Node          string  `json:"node"`
	AvgCPUPercent float64 `json:"avg_cpu_percent"`
	Samples       int     `json:"samples"`
}

// Meta describes one benchmark run's environment and flags.
type Meta struct {
	RunID     string            `json:"run_id"`
	Timestamp time.Time         `json:"timestamp"`
	GitCommit string            `json:"git_commit,omitempty"`
	GitDirty  bool              `json:"git_dirty"`
	Image     string            `json:"image,omitempty"`
	GoVersion string            `json:"go_version"`
	GOOS      string            `json:"go_os"`
	GOARCH    string            `json:"go_arch"`
	CPUCount  int               `json:"cpu_count"`
	Hostname  string            `json:"hostname,omitempty"`
	Flags     map[string]string `json:"flags,omitempty"`
	Notes     []string          `json:"notes,omitempty"`
}

// RunArtifacts is everything written into one run directory.
type RunArtifacts struct {
	Meta     Meta             `json:"meta"`
	Load     *LoadResult      `json:"load,omitempty"`
	Storage  []StorageResult  `json:"storage,omitempty"`
	Recovery []RecoveryResult `json:"recovery,omitempty"`
	CPU      []CPUStat        `json:"cpu,omitempty"`
}

// RunID returns a filesystem-safe timestamp identifier.
func RunID(now time.Time) string {
	return now.UTC().Format("20060102-150405")
}

// NewMeta builds run metadata for this process.
func NewMeta(runID string, flags map[string]string) Meta {
	hostname, _ := os.Hostname()
	commit, dirty := gitState()
	return Meta{
		RunID:     runID,
		Timestamp: time.Now().UTC(),
		GitCommit: commit,
		GitDirty:  dirty,
		Image:     os.Getenv("BENCH_IMAGE"),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		CPUCount:  runtime.NumCPU(),
		Hostname:  hostname,
		Flags:     flags,
	}
}

// WriteRun creates dir and writes every artifact that is present: meta.json,
// run.json, latency.csv, throughput.csv, storage.csv, recovery.csv, cpu.csv,
// and summary.md. It is safe to run once; use RegenerateSummary after a script
// adds cpu.csv.
func WriteRun(dir string, artifacts RunArtifacts) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	if artifacts.Meta.RunID == "" {
		artifacts.Meta.RunID = filepath.Base(dir)
	}
	if artifacts.Meta.Timestamp.IsZero() {
		artifacts.Meta.Timestamp = time.Now().UTC()
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), artifacts.Meta); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "run.json"), artifacts); err != nil {
		return err
	}
	if artifacts.Load != nil {
		if err := writeLatencyCSV(filepath.Join(dir, "latency.csv"), artifacts.Load); err != nil {
			return err
		}
		if err := writeThroughputCSV(filepath.Join(dir, "throughput.csv"), artifacts.Load); err != nil {
			return err
		}
	} else {
		_ = removeIfExists(filepath.Join(dir, "latency.csv"))
		_ = removeIfExists(filepath.Join(dir, "throughput.csv"))
	}
	if len(artifacts.Storage) > 0 {
		if err := writeStorageCSV(filepath.Join(dir, "storage.csv"), artifacts.Storage); err != nil {
			return err
		}
	}
	if len(artifacts.Recovery) > 0 {
		if err := writeRecoveryCSV(filepath.Join(dir, "recovery.csv"), artifacts.Recovery); err != nil {
			return err
		}
	}
	if len(artifacts.CPU) > 0 {
		if err := writeCPUCSV(filepath.Join(dir, "cpu.csv"), artifacts.CPU); err != nil {
			return err
		}
	}
	return writeFile(filepath.Join(dir, "summary.md"), RenderSummary(artifacts))
}

// RegenerateSummary reads a run directory and rewrites summary.md from the
// stored metadata and CSVs. Missing files are treated as empty sections so a
// load-only or storage-only run still summarizes cleanly.
func RegenerateSummary(dir string) error {
	artifacts, err := ReadRun(dir)
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(dir, "summary.md"), RenderSummary(artifacts))
}

// ReadRun loads the artifacts that exist in dir.
func ReadRun(dir string) (RunArtifacts, error) {
	var artifacts RunArtifacts
	metaPath := filepath.Join(dir, "meta.json")
	if err := readJSON(metaPath, &artifacts.Meta); err != nil {
		return artifacts, err
	}
	latency, throughput, err := readLoadCSVs(dir)
	if err != nil {
		return artifacts, err
	}
	if latency != nil || throughput != nil {
		artifacts.Load = &LoadResult{Put: OpStats{Operation: "put"}, Get: OpStats{Operation: "get"}}
		if latency != nil {
			applyLatencyRows(artifacts.Load, latency)
		}
		if throughput != nil {
			applyThroughputRows(artifacts.Load, throughput)
		}
	}
	if rows, err := readStorageCSV(filepath.Join(dir, "storage.csv")); err == nil {
		artifacts.Storage = rows
	} else if !os.IsNotExist(err) {
		return artifacts, err
	}
	if rows, err := readRecoveryCSV(filepath.Join(dir, "recovery.csv")); err == nil {
		artifacts.Recovery = rows
	} else if !os.IsNotExist(err) {
		return artifacts, err
	}
	if rows, err := readCPUCSV(filepath.Join(dir, "cpu.csv")); err == nil {
		artifacts.CPU = rows
	} else if !os.IsNotExist(err) {
		return artifacts, err
	}
	return artifacts, nil
}

// RenderSummary renders the human-readable run summary. It is deterministic for
// fixed artifacts, which keeps golden-file tests stable.
func RenderSummary(artifacts RunArtifacts) string {
	var b strings.Builder
	meta := artifacts.Meta
	fmt.Fprintf(&b, "# Benchmark Run %s\n\n", valueOr(meta.RunID, "unnamed"))
	fmt.Fprintf(&b, "- Timestamp: %s\n", meta.Timestamp.UTC().Format(time.RFC3339))
	if meta.GitCommit != "" {
		dirty := ""
		if meta.GitDirty {
			dirty = " (dirty working tree)"
		}
		fmt.Fprintf(&b, "- Git commit: `%s`%s\n", meta.GitCommit, dirty)
	}
	if meta.Image != "" {
		fmt.Fprintf(&b, "- Image: `%s`\n", meta.Image)
	}
	fmt.Fprintf(&b, "- Platform: %s/%s, %d CPUs, %s\n", valueOr(meta.GOOS, runtime.GOOS), valueOr(meta.GOARCH, runtime.GOARCH), meta.CPUCount, valueOr(meta.GoVersion, runtime.Version()))
	if meta.Hostname != "" {
		fmt.Fprintf(&b, "- Host: `%s`\n", meta.Hostname)
	}
	if len(meta.Flags) > 0 {
		keys := make([]string, 0, len(meta.Flags))
		for key := range meta.Flags {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fmt.Fprintln(&b, "- Flags:")
		for _, key := range keys {
			fmt.Fprintf(&b, "  - `%s=%s`\n", key, meta.Flags[key])
		}
	}
	b.WriteString("\n")

	if artifacts.Load != nil {
		load := artifacts.Load
		fmt.Fprintf(&b, "## Load: %s\n\n", load.Mode)
		fmt.Fprintf(&b, "Concurrency %d, value size %d B, %d keys, %.0f%% reads, measured %s after %s warmup, %d nodes.\n\n",
			load.Concurrency, load.ValueSize, load.Keys, load.ReadRatio*100, load.Measured, load.Warmup, load.Nodes)
		fmt.Fprintln(&b, "| Operation | Count | Errors | p50 ms | p95 ms | p99 ms | max ms | ops/s | MiB/s |")
		fmt.Fprintln(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
		for _, op := range []OpStats{load.Put, load.Get} {
			fmt.Fprintf(&b, "| %s | %d | %d | %s | %s | %s | %s | %.2f | %.2f |\n",
				op.Operation, op.Count, op.Errors,
				FormatDuration(op.P50), FormatDuration(op.P95), FormatDuration(op.P99), FormatDuration(op.Max),
				op.OpsPerSecond, op.BytesPerSecond/(1<<20))
		}
		fmt.Fprintf(&b, "\nOverall error rate: %.3f%% (%d/%d attempts). Degraded writes: %d. Retries: %d.\n\n",
			load.ErrorRate*100, load.TotalErrors, load.TotalAttempt, load.Degraded, load.Retries)
	}

	if len(artifacts.Storage) > 0 {
		b.WriteString("## Storage\n\n")
		fmt.Fprintln(&b, "| Mode | Keys | Value B | Logical B | Fragment B | Disk B | Payload ratio | Disk ratio |")
		fmt.Fprintln(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
		for _, row := range artifacts.Storage {
			fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %.3fx | %.3fx |\n",
				row.Mode, row.Keys, row.ValueSize, row.LogicalBytes, row.FragmentBytes, row.DiskBytes,
				row.PayloadRatio, row.DiskRatio)
		}
		b.WriteString("\nExpected: RS(3,2) stores five shards per value, so payload bytes are about 1.67x the logical bytes (67% extra). Replication stores five full copies, so payload bytes are 5.00x the logical bytes (400% extra). Disk ratios include SQLite pages and are reported separately; they are never described as a percentage of the original.\n\n")
	}

	if len(artifacts.Recovery) > 0 {
		b.WriteString("## Recovery\n\n")
		fmt.Fprintln(&b, "| Scenario | Start | Degraded | Restart | Repaired | Stop to degraded s | Repair s | Total s |")
		fmt.Fprintln(&b, "| --- | --- | --- | --- | --- | ---: | ---: | ---: |")
		for _, row := range artifacts.Recovery {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %.3f | %.3f | %.3f |\n",
				row.Scenario, formatTime(row.StartTime), formatTime(row.DegradedAt), formatTime(row.RestartTime),
				formatTime(row.RepairedAt), row.TimeToDegraded.Seconds(), row.RepairDuration.Seconds(), row.TotalDuration.Seconds())
		}
		b.WriteString("\n")
	}

	if len(artifacts.CPU) > 0 {
		b.WriteString("## CPU\n\n")
		fmt.Fprintln(&b, "| Node | Average CPU % | Samples |")
		fmt.Fprintln(&b, "| --- | ---: | ---: |")
		for _, row := range artifacts.CPU {
			fmt.Fprintf(&b, "| %s | %.2f | %d |\n", row.Node, row.AvgCPUPercent, row.Samples)
		}
		b.WriteString("\n")
	}

	if len(meta.Notes) > 0 {
		b.WriteString("## Notes\n\n")
		for _, note := range meta.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
	}
	return b.String()
}

func writeLatencyCSV(path string, load *LoadResult) error {
	rows := [][]string{{
		"operation", "count", "attempts", "errors", "p50_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms", "ops_per_sec", "bytes_per_sec", "error_rate",
	}}
	for _, op := range []OpStats{load.Put, load.Get} {
		rows = append(rows, []string{
			op.Operation,
			strconv.FormatInt(op.Count, 10),
			strconv.FormatInt(op.Attempts, 10),
			strconv.FormatInt(op.Errors, 10),
			FormatDuration(op.P50),
			FormatDuration(op.P95),
			FormatDuration(op.P99),
			FormatDuration(op.Max),
			FormatDuration(op.Mean),
			fmt.Sprintf("%.3f", op.OpsPerSecond),
			fmt.Sprintf("%.3f", op.BytesPerSecond),
			fmt.Sprintf("%.6f", op.ErrorRate()),
		})
	}
	return writeCSV(path, rows)
}

func writeThroughputCSV(path string, load *LoadResult) error {
	rows := [][]string{{"operation", "ops_per_sec", "bytes_per_sec", "error_rate", "retries", "degraded_writes"}}
	for _, op := range []OpStats{load.Put, load.Get} {
		rows = append(rows, []string{
			op.Operation,
			fmt.Sprintf("%.3f", op.OpsPerSecond),
			fmt.Sprintf("%.3f", op.BytesPerSecond),
			fmt.Sprintf("%.6f", op.ErrorRate()),
			strconv.FormatInt(load.Retries, 10),
			strconv.FormatInt(load.Degraded, 10),
		})
	}
	return writeCSV(path, rows)
}

func writeStorageCSV(path string, results []StorageResult) error {
	rows := [][]string{{
		"mode", "keys", "value_size", "logical_bytes", "fragment_bytes", "disk_bytes", "payload_ratio", "disk_ratio",
	}}
	for _, row := range results {
		rows = append(rows, []string{
			row.Mode,
			strconv.Itoa(row.Keys),
			strconv.Itoa(row.ValueSize),
			strconv.FormatInt(row.LogicalBytes, 10),
			strconv.FormatInt(row.FragmentBytes, 10),
			strconv.FormatInt(row.DiskBytes, 10),
			fmt.Sprintf("%.6f", row.PayloadRatio),
			fmt.Sprintf("%.6f", row.DiskRatio),
		})
	}
	return writeCSV(path, rows)
}

func writeRecoveryCSV(path string, results []RecoveryResult) error {
	rows := [][]string{{
		"scenario", "start_time", "degraded_time", "restart_time", "repaired_time",
		"time_to_degraded_seconds", "repair_seconds", "total_seconds",
		"healthy_nodes", "total_nodes", "degraded_stripes", "repair_queue_depth", "error",
	}}
	for _, row := range results {
		rows = append(rows, []string{
			row.Scenario,
			formatTime(row.StartTime),
			formatTime(row.DegradedAt),
			formatTime(row.RestartTime),
			formatTime(row.RepairedAt),
			fmt.Sprintf("%.3f", row.TimeToDegraded.Seconds()),
			fmt.Sprintf("%.3f", row.RepairDuration.Seconds()),
			fmt.Sprintf("%.3f", row.TotalDuration.Seconds()),
			strconv.FormatUint(uint64(row.HealthyNodes), 10),
			strconv.FormatUint(uint64(row.TotalNodes), 10),
			strconv.FormatUint(row.DegradedStripes, 10),
			strconv.FormatUint(row.RepairQueueDepth, 10),
			row.Error,
		})
	}
	return writeCSV(path, rows)
}

func writeCPUCSV(path string, results []CPUStat) error {
	rows := [][]string{{"node", "avg_cpu_percent", "samples"}}
	for _, row := range results {
		rows = append(rows, []string{row.Node, fmt.Sprintf("%.2f", row.AvgCPUPercent), strconv.Itoa(row.Samples)})
	}
	return writeCSV(path, rows)
}

func writeCSV(path string, rows [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if err := writer.WriteAll(rows); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush %s: %w", path, err)
	}
	return nil
}

type latencyRow struct {
	Operation string
	Count     int64
	Attempts  int64
	Errors    int64
	P50, P95  time.Duration
	P99, Max  time.Duration
	Mean      time.Duration
	OpsPerSec float64
	BytesSec  float64
	ErrorRate float64
}

type throughputRow struct {
	Operation string
	OpsPerSec float64
	BytesSec  float64
	ErrorRate float64
}

func readLoadCSVs(dir string) ([]latencyRow, []throughputRow, error) {
	latency, err := readLatencyCSV(filepath.Join(dir, "latency.csv"))
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	throughput, err := readThroughputCSV(filepath.Join(dir, "throughput.csv"))
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	return latency, throughput, nil
}

func readLatencyCSV(path string) ([]latencyRow, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var out []latencyRow
	for _, row := range rows {
		if len(row) < 12 {
			continue
		}
		p50, _ := parseDurationMS(row[4])
		p95, _ := parseDurationMS(row[5])
		p99, _ := parseDurationMS(row[6])
		maxVal, _ := parseDurationMS(row[7])
		mean, _ := parseDurationMS(row[8])
		out = append(out, latencyRow{
			Operation: row[0],
			Count:     parseInt64(row[1]),
			Attempts:  parseInt64(row[2]),
			Errors:    parseInt64(row[3]),
			P50:       p50, P95: p95, P99: p99, Max: maxVal, Mean: mean,
			OpsPerSec: parseFloat(row[9]),
			BytesSec:  parseFloat(row[10]),
			ErrorRate: parseFloat(row[11]),
		})
	}
	return out, nil
}

func readThroughputCSV(path string) ([]throughputRow, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var out []throughputRow
	for _, row := range rows {
		if len(row) < 4 {
			continue
		}
		out = append(out, throughputRow{
			Operation: row[0],
			OpsPerSec: parseFloat(row[1]),
			BytesSec:  parseFloat(row[2]),
			ErrorRate: parseFloat(row[3]),
		})
	}
	return out, nil
}

func applyLatencyRows(load *LoadResult, rows []latencyRow) {
	for _, row := range rows {
		op := &load.Put
		if row.Operation == "get" {
			op = &load.Get
		}
		op.Operation = row.Operation
		op.Count = row.Count
		op.Attempts = row.Attempts
		op.Errors = row.Errors
		op.P50, op.P95, op.P99, op.Max, op.Mean = row.P50, row.P95, row.P99, row.Max, row.Mean
		op.OpsPerSecond = row.OpsPerSec
		op.BytesPerSecond = row.BytesSec
		load.TotalErrors += row.Errors
		load.TotalAttempt += row.Attempts
	}
	if load.TotalAttempt > 0 {
		load.ErrorRate = float64(load.TotalErrors) / float64(load.TotalAttempt)
	}
}

func applyThroughputRows(load *LoadResult, rows []throughputRow) {
	for _, row := range rows {
		op := &load.Put
		if row.Operation == "get" {
			op = &load.Get
		}
		op.Operation = row.Operation
		op.OpsPerSecond = row.OpsPerSec
		op.BytesPerSecond = row.BytesSec
	}
}

func readStorageCSV(path string) ([]StorageResult, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var out []StorageResult
	for _, row := range rows {
		if len(row) < 8 {
			continue
		}
		out = append(out, StorageResult{
			Mode:          row[0],
			Keys:          int(parseInt64(row[1])),
			ValueSize:     int(parseInt64(row[2])),
			LogicalBytes:  parseInt64(row[3]),
			FragmentBytes: parseInt64(row[4]),
			DiskBytes:     parseInt64(row[5]),
			PayloadRatio:  parseFloat(row[6]),
			DiskRatio:     parseFloat(row[7]),
		})
	}
	return out, nil
}

func readRecoveryCSV(path string) ([]RecoveryResult, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var out []RecoveryResult
	for _, row := range rows {
		if len(row) < 13 {
			continue
		}
		out = append(out, RecoveryResult{
			Scenario:         row[0],
			StartTime:        parseTime(row[1]),
			DegradedAt:       parseTime(row[2]),
			RestartTime:      parseTime(row[3]),
			RepairedAt:       parseTime(row[4]),
			TimeToDegraded:   secondsDuration(row[5]),
			RepairDuration:   secondsDuration(row[6]),
			TotalDuration:    secondsDuration(row[7]),
			HealthyNodes:     uint32(parseInt64(row[8])),
			TotalNodes:       uint32(parseInt64(row[9])),
			DegradedStripes:  uint64(parseInt64(row[10])),
			RepairQueueDepth: uint64(parseInt64(row[11])),
			Error:            row[12],
		})
	}
	return out, nil
}

func readCPUCSV(path string) ([]CPUStat, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var out []CPUStat
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		out = append(out, CPUStat{
			Node:          row[0],
			AvgCPUPercent: parseFloat(row[1]),
			Samples:       int(parseInt64(row[2])),
		})
	}
	return out, nil
}

func readCSV(path string) ([][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(records) <= 1 {
		return nil, nil
	}
	return records[1:], nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func writeFile(path, content string) error {
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func removeIfExists(path string) error {
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func formatSeconds(value time.Duration) string {
	return fmt.Sprintf("%.3f", value.Seconds())
}

func parseTime(raw string) time.Time {
	if strings.TrimSpace(raw) == "" {
		return time.Time{}
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return value
}

func secondsDuration(raw string) time.Duration {
	seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func parseDurationMS(raw string) (time.Duration, error) {
	ms, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(ms * float64(time.Millisecond)), nil
}

func parseInt64(raw string) int64 {
	value, _ := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	return value
}

func parseFloat(raw string) float64 {
	value, _ := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	return value
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// gitState records the checkout revision without making git a hard requirement.
// A source-less container run simply reports no commit.
func gitState() (string, bool) {
	commitCmd := exec.Command("git", "rev-parse", "HEAD")
	commitOut, err := commitCmd.Output()
	if err != nil {
		return "", false
	}
	commit := strings.TrimSpace(string(commitOut))
	statusCmd := exec.Command("git", "status", "--porcelain")
	statusOut, statusErr := statusCmd.Output()
	return commit, statusErr == nil && strings.TrimSpace(string(statusOut)) != ""
}
