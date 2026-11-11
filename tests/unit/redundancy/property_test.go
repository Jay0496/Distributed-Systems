package redundancy_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

// randomValue returns deterministic pseudo-random bytes for a fixed seed.
func randomValue(rng *rand.Rand, size int) []byte {
	value := make([]byte, size)
	if _, err := rng.Read(value); err != nil {
		panic(err)
	}
	return value
}

func cloneShards(shards []redundancy.Shard) []redundancy.Shard {
	out := make([]redundancy.Shard, len(shards))
	for i, shard := range shards {
		out[i] = shard
		out[i].Data = append([]byte(nil), shard.Data...)
	}
	return out
}

func TestRS32PropertyRoundTripAndEveryTwoLostShards(t *testing.T) {
	engine := redundancy.NewRS32Engine()
	rng := rand.New(rand.NewSource(20261110))

	sizes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 31, 32, 33, 100, 255, 256, 257, 1023, 1024, 1025, 4096}
	for i := 0; i < 50; i++ {
		sizes = append(sizes, rng.Intn(8192))
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			value := randomValue(rng, size)
			shards, err := engine.Encode(value, redundancy.ModeRS32)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(shards) != redundancy.TotalShards {
				t.Fatalf("Encode returned %d shards, want %d", len(shards), redundancy.TotalShards)
			}

			decoded, err := engine.Decode(shards, redundancy.ModeRS32, int64(size))
			if err != nil {
				t.Fatalf("Decode(all): %v", err)
			}
			if !bytes.Equal(decoded, value) {
				t.Fatalf("Decode(all) changed the value")
			}

			// RS(3,2) must tolerate every pair of lost shards, leaving three.
			for lost1 := 0; lost1 < len(shards); lost1++ {
				for lost2 := lost1 + 1; lost2 < len(shards); lost2++ {
					survivors := make([]redundancy.Shard, 0, redundancy.RSDataShards)
					for index, shard := range shards {
						if index != lost1 && index != lost2 {
							survivors = append(survivors, shard)
						}
					}
					decoded, err := engine.Decode(survivors, redundancy.ModeRS32, int64(size))
					if err != nil {
						t.Fatalf("Decode without shards %d,%d: %v", lost1, lost2, err)
					}
					if !bytes.Equal(decoded, value) {
						t.Fatalf("Decode without shards %d,%d changed the value", lost1, lost2)
					}
				}
			}
		})
	}
}

func TestReplicationPropertyRoundTripAndEveryLostCopy(t *testing.T) {
	engine := redundancy.NewReplicationEngine()
	rng := rand.New(rand.NewSource(20261111))

	sizes := []int{0, 1, 2, 3, 31, 1024, 65536}
	for i := 0; i < 25; i++ {
		sizes = append(sizes, rng.Intn(8192))
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			value := randomValue(rng, size)
			shards, err := engine.Encode(value, redundancy.ModeReplication)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(shards) != redundancy.TotalShards {
				t.Fatalf("Encode returned %d copies, want %d", len(shards), redundancy.TotalShards)
			}

			decoded, err := engine.Decode(shards, redundancy.ModeReplication, int64(size))
			if err != nil {
				t.Fatalf("Decode(all): %v", err)
			}
			if !bytes.Equal(decoded, value) {
				t.Fatalf("Decode(all) changed the value")
			}

			for index, shard := range shards {
				decoded, err := engine.Decode([]redundancy.Shard{shard}, redundancy.ModeReplication, int64(size))
				if err != nil {
					t.Fatalf("Decode(copy %d): %v", index, err)
				}
				if !bytes.Equal(decoded, value) {
					t.Fatalf("Decode(copy %d) changed the value", index)
				}
			}
		})
	}
}

func TestCorruptedShardsNeverReturnPayload(t *testing.T) {
	rsEngine := redundancy.NewRS32Engine()
	repEngine := redundancy.NewReplicationEngine()
	value := []byte("corruption must be rejected, never returned")
	totalSize := int64(len(value))

	rsShards, err := rsEngine.Encode(value, redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("RS Encode: %v", err)
	}
	for index := range rsShards {
		corrupt := cloneShards(rsShards)
		corrupt[index].Data[0] ^= 0xFF

		t.Run(fmt.Sprintf("rs32 shard %d", index), func(t *testing.T) {
			decoded, err := rsEngine.Decode(corrupt, redundancy.ModeRS32, totalSize)
			if err == nil {
				t.Fatalf("Decode(corrupt shard %d) = %q, want error", index, decoded)
			}
			if decoded != nil {
				t.Fatalf("Decode(corrupt shard %d) returned payload on error", index)
			}
		})
	}

	repShards, err := repEngine.Encode(value, redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("replication Encode: %v", err)
	}
	for index := range repShards {
		corrupt := cloneShards(repShards)
		corrupt[index].Data[0] ^= 0xFF

		t.Run(fmt.Sprintf("replication copy %d", index), func(t *testing.T) {
			decoded, err := repEngine.Decode([]redundancy.Shard{corrupt[index]}, redundancy.ModeReplication, totalSize)
			if err == nil {
				t.Fatalf("Decode(corrupt copy %d) = %q, want error", index, decoded)
			}
			if decoded != nil {
				t.Fatalf("Decode(corrupt copy %d) returned payload on error", index)
			}
		})
	}
}

func TestDuplicateIndicesAreRejectedForEveryMode(t *testing.T) {
	rsEngine := redundancy.NewRS32Engine()
	repEngine := redundancy.NewReplicationEngine()
	value := []byte("duplicates are not three distinct shards")
	totalSize := int64(len(value))

	rsShards, err := rsEngine.Encode(value, redundancy.ModeRS32)
	if err != nil {
		t.Fatalf("RS Encode: %v", err)
	}
	rsInput := []redundancy.Shard{rsShards[0], rsShards[0], rsShards[1], rsShards[2]}
	if decoded, err := rsEngine.Decode(rsInput, redundancy.ModeRS32, totalSize); err == nil {
		t.Fatalf("RS Decode(duplicate indices) = %q, want error", decoded)
	}

	repShards, err := repEngine.Encode(value, redundancy.ModeReplication)
	if err != nil {
		t.Fatalf("replication Encode: %v", err)
	}
	repInput := []redundancy.Shard{repShards[0], repShards[0]}
	if decoded, err := repEngine.Decode(repInput, redundancy.ModeReplication, totalSize); err == nil {
		t.Fatalf("replication Decode(duplicate indices) = %q, want error", decoded)
	}
}
