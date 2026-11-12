package cluster

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

type repairTestStore struct {
	storage.Store
	metas map[string]storage.KeyMeta
}

func (s *repairTestStore) GetKey(key string) (storage.KeyMeta, error) {
	meta, ok := s.metas[key]
	if !ok {
		return storage.KeyMeta{}, storage.ErrNotFound
	}
	return meta, nil
}

func (s *repairTestStore) ListKeys() ([]storage.KeyMeta, error) {
	out := make([]storage.KeyMeta, 0, len(s.metas))
	for _, meta := range s.metas {
		out = append(out, meta)
	}
	return out, nil
}

type repairTestReader struct {
	value []byte
	err   error
	calls int
}

func (r *repairTestReader) ReadVerified(context.Context, storage.KeyMeta) ([]byte, error) {
	r.calls++
	return r.value, r.err
}

type repairTestUpload struct {
	nodeID   string
	stripeID string
	shard    redundancy.Shard
	replace  bool
}

type repairTestUploader struct {
	uploads []repairTestUpload
	err     error
}

func (u *repairTestUploader) UploadShard(_ context.Context, nodeID, stripeID string, shard redundancy.Shard, replace bool) error {
	if u.err != nil {
		return u.err
	}
	u.uploads = append(u.uploads, repairTestUpload{nodeID: nodeID, stripeID: stripeID, shard: shard, replace: replace})
	return nil
}

type statKey struct {
	nodeID     string
	stripeID   string
	shardIndex int
}

type repairTestStatter struct {
	present map[statKey]bool
	checks  map[statKey]string
}

func newRepairTestStatter() *repairTestStatter {
	return &repairTestStatter{present: map[statKey]bool{}, checks: map[statKey]string{}}
}

func (s *repairTestStatter) StatShard(_ context.Context, nodeID, stripeID string, shardIndex int) (bool, string, error) {
	key := statKey{nodeID: nodeID, stripeID: stripeID, shardIndex: shardIndex}
	return s.present[key], s.checks[key], nil
}

type repairTestMetaStatus struct {
	calls []bool
}

func (m *repairTestMetaStatus) SetDegraded(_ context.Context, _ string, _ string, _ int64, degraded bool) error {
	m.calls = append(m.calls, degraded)
	return nil
}

func repairTestMeta(t *testing.T, key, mode string, value []byte, degraded bool) (storage.KeyMeta, []redundancy.Shard) {
	t.Helper()
	parsed, err := redundancy.ParseMode(mode)
	if err != nil {
		t.Fatalf("parse mode: %v", err)
	}
	shards, err := encodeValue(mode, value)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	placements := make([]storage.ShardPlacement, redundancy.TotalShards)
	for i := range placements {
		placements[i] = storage.ShardPlacement{
			ShardIndex: i,
			NodeID:     []string{"node1", "node2", "node3", "node4", "node5"}[i],
			IsParity:   parsed == redundancy.ModeRS32 && i >= redundancy.RSDataShards,
		}
	}
	return storage.KeyMeta{
		Key:       key,
		StripeID:  "stripe-" + key,
		Mode:      mode,
		Size:      int64(len(value)),
		Checksum:  redundancy.Checksum(value),
		Version:   3,
		Degraded:  degraded,
		Placement: placements,
	}, shards
}

func repairTestRepairer(meta storage.KeyMeta, value []byte, statter *repairTestStatter) (*Repairer, *repairTestReader, *repairTestUploader, *repairTestMetaStatus) {
	reader := &repairTestReader{value: value}
	uploader := &repairTestUploader{}
	status := &repairTestMetaStatus{}
	repairer := NewRepairer(RepairerOptions{
		Store:      &repairTestStore{metas: map[string]storage.KeyMeta{meta.Key: meta}},
		Reader:     reader,
		Uploader:   uploader,
		Statter:    statter,
		MetaStatus: status,
		Nodes:      []string{"node1", "node2", "node3", "node4", "node5"},
		SelfNode:   "node1",
		RateLimit:  10000,
		Logf:       func(string, ...any) {},
	})
	return repairer, reader, uploader, status
}

func TestRepairerRestoresMissingRSShards(t *testing.T) {
	for _, missing := range [][]int{{0}, {0, 1}} {
		t.Run("missing", func(t *testing.T) {
			value := bytes.Repeat([]byte("repairer-rs-"), 83)
			meta, shards := repairTestMeta(t, "rs", "rs32", value, false)
			statter := newRepairTestStatter()
			for _, shard := range shards {
				present := true
				for _, index := range missing {
					if shard.Index == index {
						present = false
					}
				}
				key := statKey{nodeID: meta.Placement[shard.Index].NodeID, stripeID: meta.StripeID, shardIndex: shard.Index}
				statter.present[key] = present
				statter.checks[key] = shard.Checksum
			}

			repairer, reader, uploader, _ := repairTestRepairer(meta, value, statter)
			for _, index := range missing {
				err := repairer.Process(context.Background(), Task{
					Key:        meta.Key,
					StripeID:   meta.StripeID,
					Version:    meta.Version,
					ShardIndex: index,
					Priority:   PriorityDegraded,
				})
				if err != nil {
					t.Fatalf("repair shard %d: %v", index, err)
				}
			}

			if reader.calls != len(missing) {
				t.Fatalf("reader calls = %d, want %d", reader.calls, len(missing))
			}
			if len(uploader.uploads) != len(missing) {
				t.Fatalf("uploads = %d, want %d", len(uploader.uploads), len(missing))
			}
			for i, index := range missing {
				upload := uploader.uploads[i]
				if upload.shard.Index != index || upload.replace {
					t.Fatalf("upload = %+v, want shard %d replace=false", upload, index)
				}
				if upload.shard.Checksum != shards[index].Checksum {
					t.Fatalf("uploaded checksum = %q, want %q", upload.shard.Checksum, shards[index].Checksum)
				}
			}
		})
	}
}

func TestRepairerRestoresReplicationCopy(t *testing.T) {
	value := bytes.Repeat([]byte("repairer-copy-"), 41)
	meta, shards := repairTestMeta(t, "copy", "replication", value, false)
	statter := newRepairTestStatter()
	for _, shard := range shards {
		if shard.Index == 2 {
			continue
		}
		key := statKey{nodeID: meta.Placement[shard.Index].NodeID, stripeID: meta.StripeID, shardIndex: shard.Index}
		statter.present[key] = true
		statter.checks[key] = shard.Checksum
	}

	repairer, _, uploader, _ := repairTestRepairer(meta, value, statter)
	if err := repairer.Process(context.Background(), Task{
		Key:        meta.Key,
		StripeID:   meta.StripeID,
		Version:    meta.Version,
		ShardIndex: 2,
	}); err != nil {
		t.Fatalf("repair copy: %v", err)
	}
	if len(uploader.uploads) != 1 || uploader.uploads[0].shard.Index != 2 {
		t.Fatalf("uploads = %+v, want copy 2", uploader.uploads)
	}
	if !bytes.Equal(uploader.uploads[0].shard.Data, value) {
		t.Fatal("repair copy payload does not match the verified value")
	}
}

func TestRepairerDropsStaleStripeTask(t *testing.T) {
	value := []byte("stale")
	meta, _ := repairTestMeta(t, "stale", "rs32", value, false)
	repairer, reader, uploader, _ := repairTestRepairer(meta, value, newRepairTestStatter())

	err := repairer.Process(context.Background(), Task{
		Key:        meta.Key,
		StripeID:   "older-stripe",
		Version:    meta.Version,
		ShardIndex: 0,
	})
	if err != nil {
		t.Fatalf("stale task: %v", err)
	}
	if reader.calls != 0 || len(uploader.uploads) != 0 {
		t.Fatalf("stale task performed work: reads=%d uploads=%d", reader.calls, len(uploader.uploads))
	}
}

func TestRepairerClearsDegradedAfterAllShardsPresent(t *testing.T) {
	value := bytes.Repeat([]byte("complete-"), 29)
	meta, shards := repairTestMeta(t, "complete", "rs32", value, true)
	statter := newRepairTestStatter()
	for _, shard := range shards {
		key := statKey{nodeID: meta.Placement[shard.Index].NodeID, stripeID: meta.StripeID, shardIndex: shard.Index}
		statter.present[key] = true
		statter.checks[key] = shard.Checksum
	}

	repairer, _, uploader, status := repairTestRepairer(meta, value, statter)
	if err := repairer.Process(context.Background(), Task{
		Key:        meta.Key,
		StripeID:   meta.StripeID,
		Version:    meta.Version,
		ShardIndex: 4,
	}); err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(uploader.uploads) != 0 {
		t.Fatalf("present shard was re-uploaded: %+v", uploader.uploads)
	}
	if len(status.calls) != 1 || status.calls[0] != false {
		t.Fatalf("degraded calls = %v, want one false", status.calls)
	}
}

func TestRepairerForceReplacesPresentShard(t *testing.T) {
	value := bytes.Repeat([]byte("force-"), 31)
	meta, shards := repairTestMeta(t, "force", "rs32", value, false)
	statter := newRepairTestStatter()
	key := statKey{nodeID: meta.Placement[0].NodeID, stripeID: meta.StripeID, shardIndex: 0}
	statter.present[key] = true
	statter.checks[key] = shards[0].Checksum

	repairer, _, uploader, _ := repairTestRepairer(meta, value, statter)
	if err := repairer.Process(context.Background(), Task{
		Key:        meta.Key,
		StripeID:   meta.StripeID,
		Version:    meta.Version,
		ShardIndex: 0,
		Replace:    true,
	}); err != nil {
		t.Fatalf("force repair: %v", err)
	}
	if len(uploader.uploads) != 1 || !uploader.uploads[0].replace {
		t.Fatalf("uploads = %+v, want one replace upload", uploader.uploads)
	}
}

func TestRepairerNeverRepairsFromSubQuorum(t *testing.T) {
	value := []byte("unrecoverable")
	meta, _ := repairTestMeta(t, "lost", "rs32", value, true)
	reader := &repairTestReader{err: errors.New("insufficient valid shards")}
	uploader := &repairTestUploader{}
	repairer := NewRepairer(RepairerOptions{
		Store:      &repairTestStore{metas: map[string]storage.KeyMeta{meta.Key: meta}},
		Reader:     reader,
		Uploader:   uploader,
		Statter:    newRepairTestStatter(),
		MetaStatus: &repairTestMetaStatus{},
		Nodes:      []string{"node1"},
		SelfNode:   "node1",
		RateLimit:  10000,
		Logf:       func(string, ...any) {},
	})

	err := repairer.Process(context.Background(), Task{
		Key:        meta.Key,
		StripeID:   meta.StripeID,
		Version:    meta.Version,
		ShardIndex: 0,
	})
	if err == nil {
		t.Fatal("sub-quorum repair returned success")
	}
	if len(uploader.uploads) != 0 {
		t.Fatalf("sub-quorum repair uploaded %d shards", len(uploader.uploads))
	}
}
