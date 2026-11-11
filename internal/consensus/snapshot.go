package consensus

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"distributed-kv-store/internal/storage"

	"github.com/hashicorp/raft"
)

// fsmSnapshotSchemaVersion is the envelope version owned by the FSM. Version 2
// carries a metadata section and, during the legacy transition, a legacy KV
// section. Version 1 snapshots written before this task remain readable.
const fsmSnapshotSchemaVersion = 2

// fsmSnapshotEnvelope is the versioned snapshot format. Each section is a raw
// payload so the metadata and legacy schemas stay independent.
type fsmSnapshotEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Legacy        json.RawMessage `json:"legacy,omitempty"`
}

type metadataSnapshotter interface {
	SnapshotMetadataPayload() ([]byte, error)
	RestoreMetadataPayload([]byte) error
}

type legacySnapshotter interface {
	SnapshotLegacyPayload() ([]byte, error)
	RestoreLegacyPayload([]byte) error
}

type fullSnapshotRestorer interface {
	RestoreFullSnapshot(metadataPayload, legacyPayload []byte) error
}

type metadataEnvelopeRestorer interface {
	RestoreMetadata([]byte) error
}

// Snapshot returns the versioned FSM snapshot. Metadata is always captured from
// stores that support metadata snapshots; the legacy section is captured while
// the transitional full-value commands still exist.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	metadata, legacy, err := f.snapshotPayloads()
	if err != nil {
		return nil, err
	}

	envelope := fsmSnapshotEnvelope{SchemaVersion: fsmSnapshotSchemaVersion}
	if len(metadata) > 0 {
		envelope.Metadata = metadata
	}
	if len(legacy) > 0 {
		envelope.Legacy = legacy
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal FSM snapshot: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

func (f *FSM) snapshotPayloads() (metadata, legacy []byte, err error) {
	if snapshotter, ok := f.store.(metadataSnapshotter); ok {
		metadata, err = snapshotter.SnapshotMetadataPayload()
		if err != nil {
			return nil, nil, err
		}
	}

	if snapshotter, ok := f.store.(legacySnapshotter); ok {
		legacy, err = snapshotter.SnapshotLegacyPayload()
		if err != nil {
			return nil, nil, err
		}
	} else if snapshotter, ok := f.store.(storage.Snapshotter); ok {
		legacy, err = snapshotter.Snapshot()
		if err != nil {
			return nil, nil, err
		}
	}

	return metadata, legacy, nil
}

// Restore restores the FSM state from a snapshot. Version 2 restores the
// metadata and legacy sections atomically when the store supports it. Version 1
// (the previous single-section format) and the pre-SQLite raw map remain
// readable for upgrade safety.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	return f.restoreSnapshot(data)
}

func (f *FSM) restoreSnapshot(data []byte) error {
	var peek struct {
		SchemaVersion int    `json:"schema_version"`
		Kind          string `json:"kind"`
	}
	if err := json.Unmarshal(data, &peek); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	switch peek.SchemaVersion {
	case fsmSnapshotSchemaVersion:
		if peek.Kind != "" {
			return fmt.Errorf("unsupported snapshot kind %q for schema version %d", peek.Kind, peek.SchemaVersion)
		}
		return f.restoreV2(data)
	case 1:
		return f.restoreV1(data, peek.Kind)
	case 0:
		if peek.Kind == "" {
			// Pre-SQLite snapshots were a bare map[string][]byte.
			snapshotter, ok := f.store.(storage.Snapshotter)
			if !ok {
				return nil
			}
			return snapshotter.Restore(data)
		}
		return fmt.Errorf("snapshot has schema version 0 and kind %q", peek.Kind)
	default:
		return fmt.Errorf("unsupported snapshot schema version %d", peek.SchemaVersion)
	}
}

func (f *FSM) restoreV2(data []byte) error {
	var envelope fsmSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode version 2 snapshot: %w", err)
	}
	if len(envelope.Metadata) == 0 && len(envelope.Legacy) == 0 {
		return nil
	}

	if restorer, ok := f.store.(fullSnapshotRestorer); ok {
		return restorer.RestoreFullSnapshot(envelope.Metadata, envelope.Legacy)
	}

	if len(envelope.Metadata) > 0 {
		snapshotter, ok := f.store.(metadataSnapshotter)
		if !ok {
			return errors.New("store does not support metadata snapshot restore")
		}
		if err := snapshotter.RestoreMetadataPayload(envelope.Metadata); err != nil {
			return err
		}
	}

	if len(envelope.Legacy) > 0 {
		if snapshotter, ok := f.store.(legacySnapshotter); ok {
			if err := snapshotter.RestoreLegacyPayload(envelope.Legacy); err != nil {
				return err
			}
		} else if snapshotter, ok := f.store.(storage.Snapshotter); ok {
			if err := snapshotter.Restore(envelope.Legacy); err != nil {
				return err
			}
		} else {
			return errors.New("store does not support legacy snapshot restore")
		}
	}

	return nil
}

func (f *FSM) restoreV1(data []byte, kind string) error {
	switch kind {
	case "", "legacy":
		snapshotter, ok := f.store.(storage.Snapshotter)
		if !ok {
			return nil
		}
		return snapshotter.Restore(data)
	case "metadata":
		snapshotter, ok := f.store.(metadataEnvelopeRestorer)
		if !ok {
			return errors.New("store does not support metadata snapshot restore")
		}
		return snapshotter.RestoreMetadata(data)
	default:
		return fmt.Errorf("unsupported version 1 snapshot kind %q", kind)
	}
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
