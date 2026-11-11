package consensus

import (
	"encoding/json"
	"errors"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	"github.com/hashicorp/raft"
)

// FSM is the Finite State Machine that Raft uses to apply committed log entries.
// It is the bridge between the Raft log and the actual key-value store.
type FSM struct {
	store storage.Store
}

// NewFSM creates a new FSM backed by the given store.
func NewFSM(store storage.Store) *FSM {
	return &FSM{store: store}
}

// Apply is called by Raft on every node once a log entry has been committed
// by a quorum. It must apply the command to the local store.
//
// Expected failures are returned as typed Result values. Apply never returns a
// Go error for a bad command because Raft treats an apply panic as fatal.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return &Result{Err: ResultErrInvalid}
	}

	if IsLegacy(cmd.Op) {
		return f.applyLegacy(cmd)
	}
	if !IsMetadata(cmd.Op) {
		return &Result{Err: ResultErrInvalid}
	}

	if err := validateMetadataSize(cmd, log.Data); err != nil {
		return &Result{Err: ResultErrInvalid}
	}
	if err := cmd.Validate(); err != nil {
		return &Result{Err: ResultErrInvalid}
	}

	// Replaying an entry at or before the last applied index is a no-op. This
	// matters when SQLite is already ahead of the Raft snapshot on restart.
	if isMetadataMutation(cmd.Op) && log.Index != 0 {
		applied, err := f.store.LastAppliedIndex()
		if err != nil {
			return &Result{Err: ResultErrInternal}
		}
		if log.Index <= applied {
			return f.currentResult(cmd)
		}
	}

	switch cmd.Op {
	case OpPutMeta:
		return f.applyPutMeta(log, cmd)
	case OpSetMetaStatus:
		return f.applySetMetaStatus(log, cmd)
	case OpGetMeta:
		return f.applyGetMeta(cmd)
	case OpDeleteMeta:
		return f.applyDeleteMeta(log, cmd)
	case OpSetConfig:
		return f.applySetConfig(log, cmd)
	case OpGetConfig:
		return f.applyGetConfig()
	default:
		return &Result{Err: ResultErrInvalid}
	}
}

// applyLegacy applies the deprecated full-value put/get commands. It is removed
// with the legacy path in task 15.
func (f *FSM) applyLegacy(cmd Command) interface{} {
	switch cmd.Op {
	case OpPut:
		if cmd.Legacy == nil {
			return &Result{Err: ResultErrInvalid}
		}
		if err := f.store.Put(cmd.Legacy.Key, cmd.Legacy.Value, false); err != nil {
			return resultFromError(err)
		}
		return nil
	case OpGet:
		key := cmd.Key
		if cmd.Legacy != nil {
			key = cmd.Legacy.Key
		}
		value, err := f.store.Get(key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return &GetResult{Found: false}
			}
			return &GetResult{Found: false, Err: ResultErrInternal}
		}
		return &GetResult{Value: value, Found: true}
	default:
		return &Result{Err: ResultErrInvalid}
	}
}

func (f *FSM) applyPutMeta(log *raft.Log, cmd Command) *Result {
	meta := *cmd.Meta

	old, err := f.store.ApplyPutMeta(log.Index, meta)
	if err != nil {
		return resultFromError(err)
	}

	result := &Result{Meta: &meta, Found: true}
	// Only a genuine overwrite with a different stripe may be garbage
	// collected. An idempotent retry of the same stripe must not delete the
	// stripe the caller just wrote.
	if old.Key != "" && old.StripeID != meta.StripeID {
		result.OldMeta = &old
	}
	return result
}

func (f *FSM) applySetMetaStatus(log *raft.Log, cmd Command) *Result {
	updated, err := f.store.ApplyMetaStatus(log.Index, cmd.Key, cmd.StripeID, cmd.Version, cmd.Degraded)
	if err != nil {
		return resultFromError(err)
	}

	if updated.Key != "" {
		return &Result{Meta: &updated, Found: true}
	}

	// A replay returns an empty record from the store. Return the current
	// metadata instead of an empty result.
	return f.currentMeta(cmd.Key)
}

func (f *FSM) applyGetMeta(cmd Command) *Result {
	return f.currentMeta(cmd.Key)
}

func (f *FSM) applyDeleteMeta(log *raft.Log, cmd Command) *Result {
	old, err := f.store.ApplyDeleteMeta(log.Index, cmd.Key, cmd.Version)
	if err != nil {
		return resultFromError(err)
	}
	if old.Key == "" {
		return &Result{Found: false}
	}
	return &Result{OldMeta: &old, Found: true}
}

func (f *FSM) applySetConfig(log *raft.Log, cmd Command) *Result {
	if err := f.store.ApplySetConfig(log.Index, *cmd.Config); err != nil {
		return resultFromError(err)
	}
	cfg := *cmd.Config
	return &Result{Config: &cfg, Found: true}
}

func (f *FSM) applyGetConfig() *Result {
	cfg, err := f.store.GetConfig()
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			def := defaultClusterConfig()
			return &Result{Config: &def, Found: false}
		}
		return &Result{Err: ResultErrInternal}
	}
	return &Result{Config: &cfg, Found: true}
}

func (f *FSM) currentMeta(key string) *Result {
	meta, err := f.store.GetKey(key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &Result{Found: false}
		}
		return &Result{Err: ResultErrInternal}
	}
	return &Result{Meta: &meta, Found: true}
}

func (f *FSM) currentResult(cmd Command) *Result {
	switch cmd.Op {
	case OpPutMeta:
		if cmd.Meta != nil {
			return f.currentMeta(cmd.Meta.Key)
		}
	case OpSetMetaStatus, OpDeleteMeta:
		return f.currentMeta(cmd.Key)
	case OpSetConfig:
		return f.applyGetConfig()
	}
	return &Result{Found: false}
}

func defaultClusterConfig() storage.ClusterConfig {
	return storage.ClusterConfig{DefaultMode: string(redundancy.ModeRS32)}
}
