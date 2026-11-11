package redundancy_test

import (
	"math"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

func totalShardBytes(shards []redundancy.Shard) int64 {
	var total int64
	for _, shard := range shards {
		total += int64(len(shard.Data))
	}
	return total
}

// TestRS32StorageMath verifies the RS(3,2) footprint: five shards, each
// ceil(size/3) bytes, for a non-empty value. That is ~1.67x the original size,
// about 67% extra storage, not "67% of the original".
func TestRS32StorageMath(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	sizes := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 31, 100, 1024, 4096, 65536, 1 << 20}

	for _, size := range sizes {
		value := testValue(size)
		shards, err := engine.Encode(value, redundancy.ModeRS32)
		if err != nil {
			t.Fatalf("Encode(%d bytes): %v", size, err)
		}
		shardSize := (size + redundancy.RSDataShards - 1) / redundancy.RSDataShards
		want := int64(redundancy.TotalShards * shardSize)
		if got := totalShardBytes(shards); got != want {
			t.Errorf("RS total for %d bytes = %d, want 5*ceil(size/3) = %d", size, got, want)
		}
	}
}

func TestRS32LargeValueOverhead(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	const size = 1 << 20 // 1 MiB
	shards, err := engine.Encode(testValue(size), redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	total := totalShardBytes(shards)
	ratio := float64(total) / float64(size)
	overhead := float64(total-size) / float64(size)

	if math.Abs(ratio-(5.0/3.0)) > 0.001 {
		t.Errorf("RS total ratio = %.4fx, want ~1.67x", ratio)
	}
	if math.Abs(overhead-(2.0/3.0)) > 0.001 {
		t.Errorf("RS overhead = %.1f%%, want ~67%% extra", overhead*100)
	}
}

// TestReplicationStorageMath verifies the five-copy footprint: 5x the original
// size, which is 400% extra storage.
func TestReplicationStorageMath(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	sizes := []int{0, 1, 2, 3, 100, 1024, 65536, 1 << 20}

	for _, size := range sizes {
		shards, err := engine.Encode(testValue(size), redundancy.ModeReplication)
		if err != nil {
			t.Fatalf("Encode(%d bytes): %v", size, err)
		}
		want := int64(redundancy.TotalShards * size)
		if got := totalShardBytes(shards); got != want {
			t.Errorf("replication total for %d bytes = %d, want 5*size = %d", size, got, want)
		}
	}
}

func TestReplicationLargeValueOverhead(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	const size = 1 << 20
	shards, err := engine.Encode(testValue(size), redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	total := totalShardBytes(shards)
	ratio := float64(total) / float64(size)
	overhead := float64(total-size) / float64(size)

	if ratio != 5.0 {
		t.Errorf("replication total ratio = %.4fx, want 5x", ratio)
	}
	if overhead != 4.0 {
		t.Errorf("replication overhead = %.1f%%, want 400%% extra", overhead*100)
	}
}

func TestEmptyValueStorageException(t *testing.T) {
	rsShards, err := redundancy.NewRS32Engine().Encode(nil, redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("RS Encode(empty): %v", err)
	}
	if got := totalShardBytes(rsShards); got != redundancy.TotalShards {
		t.Errorf("RS empty-value storage = %d bytes, want five 1-byte shards = %d", got, redundancy.TotalShards)
	}

	repShards, err := redundancy.NewReplicationEngine().Encode(nil, redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("replication Encode(empty): %v", err)
	}
	if got := totalShardBytes(repShards); got != 0 {
		t.Errorf("replication empty-value storage = %d bytes, want 0", got)
	}
}

func TestRS32StoresLessThanReplicationForNonTinyValues(t *testing.T) {
	rsEngine := redundancy.NewRS32Engine()
	repEngine := redundancy.NewReplicationEngine()

	for _, size := range []int{2, 3, 4, 10, 100, 1024, 1 << 20} {
		value := testValue(size)
		rsShards, err := rsEngine.Encode(value, redundancy.ModeRS32)
		if err != nil {
			t.Fatalf("RS Encode(%d bytes): %v", size, err)
		}
		repShards, err := repEngine.Encode(value, redundancy.ModeReplication)
		if err != nil {
			t.Fatalf("replication Encode(%d bytes): %v", size, err)
		}
		if rs := totalShardBytes(rsShards); rs >= int64(redundancy.TotalShards*size) {
			t.Errorf("RS storage for %d bytes = %d, want less than replication total %d", size, rs, redundancy.TotalShards*size)
		}
		if rep := totalShardBytes(repShards); rep != int64(redundancy.TotalShards*size) {
			t.Errorf("replication storage for %d bytes = %d, want %d", size, rep, redundancy.TotalShards*size)
		}
	}
}
