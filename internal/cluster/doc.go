// Package cluster owns membership-aware runtime behaviour: failure detection,
// degraded-mode reporting, repair scheduling, reconstruction, and orphan
// cleanup.
//
// # Architecture
//
// A leader-gated Detector probes every peer with the standard gRPC health
// service plus NodeStatus and maintains a health map with hysteresis. It emits
// node-up, node-down, leader-gained, and leader-lost events. Followers do not
// probe; ClusterHealth forwards to the leader and reads the cached map.
//
// The Repairer consumes a shared, bounded Queue. Read repair, degraded stripes,
// returning nodes, explicit Repair RPCs, and the periodic metadata sweep all
// enqueue into the same structure. Tasks are deduplicated by
// (stripe_id, shard_index), ordered by priority, retried with jittered
// exponential backoff, and executed by a small worker pool. Reconstruction
// re-reads metadata, drops stale stripe versions, reads a verified value from
// live peers, re-encodes it, and uploads only the missing or corrupt shard to
// its metadata-named owner. It never invents data from a sub-quorum and never
// changes placement, stripe ID, or version. The degraded flag clears only after
// all five placements verify.
//
// The orphan sweep is the one non-leader background duty. Because fragments are
// node-local, every node lists its local fragments and deletes those that are
// older than a grace period and either unreferenced by metadata or placed on a
// different node. Membership is fixed at five nodes: "rebalancing" means
// refilling returning nodes and removing orphans, not migrating data to a
// changed node set.
//
// # Package boundaries
//
// internal/cluster must not import internal/server. The Repairer depends on the
// narrow ValueReader, ShardUploader, ShardStatter, and MetaStatusWriter
// interfaces, which *server.Server implements. Command wiring passes the same
// server into them. Cluster-local RPC code is avoided; production uploads and
// stats use the server seams so there is one chunked transport implementation.
package cluster
