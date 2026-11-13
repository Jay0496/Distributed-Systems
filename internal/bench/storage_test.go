package bench

import (
	"context"
	"testing"

	pb "distributed-kv-store/proto"
)

func TestMeasureStorageRatioAndForcedCompaction(t *testing.T) {
	api := &fakeAPI{stats: pb.StatsResponse{
		Keys: 1, Stripes: 1, Fragments: 1, FragmentBytes: 100, StorageBytes: 120,
	}}
	nodes := fakeNodes(5, api)

	result, err := MeasureStorage(context.Background(), nodes, DatasetOptions{
		Mode:      "rs32",
		Values:    1,
		ValueSize: 100,
		Prefix:    "ratio",
		Seed:      1,
	})
	if err != nil {
		t.Fatalf("MeasureStorage: %v", err)
	}
	if result.LogicalBytes != 100 {
		t.Fatalf("logical bytes = %d, want 100", result.LogicalBytes)
	}
	if result.FragmentBytes != 500 {
		t.Fatalf("fragment bytes = %d, want 500", result.FragmentBytes)
	}
	if result.DiskBytes != 600 {
		t.Fatalf("disk bytes = %d, want 600", result.DiskBytes)
	}
	if result.PayloadRatio != 5 {
		t.Fatalf("payload ratio = %f, want 5", result.PayloadRatio)
	}
	if api.snapshots != 5 || api.checks != 5 {
		t.Fatalf("snapshots=%d checkpoints=%d, want 5 each", api.snapshots, api.checks)
	}
}
