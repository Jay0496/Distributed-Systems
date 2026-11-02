package consensus

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"distributed-kv-store/storage"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Node bundles the Raft instance with its FSM and the BoltDB store that backs
// the log. Close must be called on shutdown so the database file lock is
// released; otherwise a restarted process (or a test) blocks opening the same
// file again.
type Node struct {
	Raft *raft.Raft
	FSM  *FSM

	bolt *raftboltdb.BoltStore

	closeOnce sync.Once
	closeErr  error
}

// Close shuts down Raft and releases the BoltDB file handle. It is idempotent.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		if n.Raft != nil {
			if err := n.Raft.Shutdown().Error(); err != nil {
				n.closeErr = err
			}
		}
		if n.bolt != nil {
			if err := n.bolt.Close(); err != nil && n.closeErr == nil {
				n.closeErr = err
			}
		}
	})
	return n.closeErr
}

// StartRaft initializes a Raft node using the provided transport and store.
//
// The transport is injected so that callers can choose:
//   - raft.NewTCPTransport(...) for a real multi-container deployment
//   - raft.NewInmemTransport(...) for fast in-process tests
//
// Raft's log and stable state are persisted in a BoltDB file under dataDir so
// that a restarted container can replay its log and rejoin the cluster.
// If bootstrapConfig contains at least one server and no state exists yet,
// the cluster is bootstrapped.
func StartRaft(nodeID string, store storage.KVStore, transport raft.Transport, dataDir string, bootstrapConfig raft.Configuration) (*Node, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	fsm := NewFSM(store)

	r, err := raft.NewRaft(config, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	if len(bootstrapConfig.Servers) > 0 {
		hasState, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
		if err != nil {
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return nil, fmt.Errorf("check existing state: %w", err)
		}
		if !hasState {
			if err := r.BootstrapCluster(bootstrapConfig).Error(); err != nil {
				_ = r.Shutdown().Error()
				_ = boltStore.Close()
				return nil, fmt.Errorf("bootstrap cluster: %w", err)
			}
		}
	}

	return &Node{Raft: r, FSM: fsm, bolt: boltStore}, nil
}

// AddVoter adds a new voting member to the cluster. It must be called on the
// leader; otherwise it returns an error naming the current leader.
func (n *Node) AddVoter(id, addr string) error {
	if n.Raft.State() != raft.Leader {
		_, leaderID := n.Raft.LeaderWithID()
		return fmt.Errorf("not the leader (current leader: %q)", leaderID)
	}
	return n.Raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0).Error()
}
