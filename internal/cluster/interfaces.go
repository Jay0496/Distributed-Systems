package cluster

import (
	"context"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"
)

// ValueReader reconstructs and verifies a committed value from its metadata.
// *server.Server implements it with the task 07 read path, without enqueuing
// another read repair.
type ValueReader interface {
	ReadVerified(ctx context.Context, meta storage.KeyMeta) ([]byte, error)
}

// ShardUploader uploads one shard or copy to its intended owner. The production
// implementation is the task 06 chunked fragment push, including checksum
// verification and replace handling; the repairer must not duplicate that
// transport.
type ShardUploader interface {
	UploadShard(ctx context.Context, nodeID, stripeID string, shard redundancy.Shard, replace bool) error
}

// ShardStatter reports whether an owner already stores a fragment and its
// stored checksum, without transferring the payload. A missing fragment is
// present=false with a nil error.
type ShardStatter interface {
	StatShard(ctx context.Context, nodeID, stripeID string, shardIndex int) (present bool, checksum string, err error)
}

// MetaStatusWriter clears a stripe's degraded flag through the Raft
// set_meta_status command. It never changes the key version or placement.
type MetaStatusWriter interface {
	SetDegraded(ctx context.Context, key, stripeID string, version int64, degraded bool) error
}
