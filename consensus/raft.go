package consensus

import (
	"fmt"

	"distributed-kv-store/storage"

	"github.com/hashicorp/raft"
)

// Node bundles the Raft instance with its FSM so callers can access both.
type Node struct {
	Raft *raft.Raft
	FSM  *FSM
}

// StartRaft initializes a Raft node using the provided transport and store.
//
// The transport is injected so that callers can choose:
//   - raft.NewTCPTransport(...) for a real multi-container deployment
//   - raft.NewInmemTransport(...) for fast in-process tests
//
// If bootstrapConfig contains at least one server, the cluster is bootstrapped.
func StartRaft(nodeID string, store storage.KVStore, transport raft.Transport, bootstrapConfig raft.Configuration) (*Node, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// In-memory Raft log/stable/snapshot stores. These can be swapped for
	// BoltDB later if Raft state needs to survive node restarts.
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	fsm := NewFSM(store)

	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	if len(bootstrapConfig.Servers) > 0 {
		if err := r.BootstrapCluster(bootstrapConfig).Error(); err != nil {
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return &Node{Raft: r, FSM: fsm}, nil
}
