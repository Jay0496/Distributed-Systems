package consensus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

// fsmSnapshotSchemaVersion is the envelope version owned by the FSM. Version 3
// is metadata-only. Versions 1 and 2 remain readable for upgrade safety, but a
// snapshot that still carries legacy values fails rather than dropping them.
const fsmSnapshotSchemaVersion = 3

// legacyValueExportInstruction is returned when an old snapshot still holds
// full-value rows. Those rows cannot be converted into stripes without the
// original payloads, so the operator must export and rewrite them first.
const legacyValueExportInstruction = "legacy snapshot values cannot be converted to stripes; " +
	"export the keys with the last release that supports the legacy path, rewrite each key through the current CLI, " +
	"and take a new snapshot before upgrading"

// fsmSnapshotEnvelope is the versioned snapshot format. Metadata is the only
// section written at version 3; the legacy field exists solely so a version 2
// snapshot can be inspected and rejected without silently dropping values.
type fsmSnapshotEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Legacy        json.RawMessage `json:"legacy,omitempty"`
}

type metadataSnapshotter interface {
	SnapshotMetadataPayload() ([]byte, error)
	RestoreMetadataPayload([]byte) error
}

type metadataEnvelopeRestorer interface {
	RestoreMetadata([]byte) error
}

// Snapshot returns the versioned metadata-only FSM snapshot.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	envelope := fsmSnapshotEnvelope{SchemaVersion: fsmSnapshotSchemaVersion}
	if snapshotter, ok := f.store.(metadataSnapshotter); ok {
		metadata, err := snapshotter.SnapshotMetadataPayload()
		if err != nil {
			return nil, err
		}
		if len(metadata) > 0 {
			envelope.Metadata = metadata
		}
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal FSM snapshot: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore restores the FSM state from a snapshot. Version 3 is metadata-only.
// Version 2 restores its metadata after proving the transitional legacy section
// is empty. Version 1 restores metadata envelopes and permits an empty legacy
// payload; a non-empty legacy payload fails with an export instruction.
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
		return f.restoreV3(data)
	case 2:
		if peek.Kind != "" {
			return fmt.Errorf("unsupported snapshot kind %q for schema version 2", peek.Kind)
		}
		return f.restoreV2(data)
	case 1:
		return f.restoreV1(data, peek.Kind)
	case 0:
		if peek.Kind != "" {
			return fmt.Errorf("snapshot has schema version 0 and kind %q", peek.Kind)
		}
		// Pre-SQLite snapshots were a bare map[string][]byte of legacy values.
		return f.restoreLegacyPayload(data)
	default:
		return fmt.Errorf("unsupported snapshot schema version %d", peek.SchemaVersion)
	}
}

func (f *FSM) restoreV3(data []byte) error {
	var envelope fsmSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode version 3 snapshot: %w", err)
	}
	if len(envelope.Legacy) > 0 {
		return errors.New(legacyValueExportInstruction)
	}
	return f.restoreMetadataPayload(envelope.Metadata)
}

func (f *FSM) restoreV2(data []byte) error {
	var envelope fsmSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode version 2 snapshot: %w", err)
	}
	empty, err := legacySectionEmpty(envelope.Legacy)
	if err != nil {
		return fmt.Errorf("decode version 2 legacy section: %w", err)
	}
	if !empty {
		return errors.New(legacyValueExportInstruction)
	}
	return f.restoreMetadataPayload(envelope.Metadata)
}

func (f *FSM) restoreV1(data []byte, kind string) error {
	switch kind {
	case "metadata":
		snapshotter, ok := f.store.(metadataEnvelopeRestorer)
		if !ok {
			return errors.New("store does not support metadata snapshot restore")
		}
		return snapshotter.RestoreMetadata(data)
	case "", "legacy":
		return f.restoreLegacyPayload(data)
	default:
		return fmt.Errorf("unsupported version 1 snapshot kind %q", kind)
	}
}

// restoreLegacyPayload accepts a bare map or a version 1 legacy envelope and
// fails only when it actually holds values.
func (f *FSM) restoreLegacyPayload(data []byte) error {
	payload := data
	var envelope struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && envelope.Kind != "" {
		if envelope.Kind != "legacy" {
			return fmt.Errorf("unsupported legacy snapshot kind %q", envelope.Kind)
		}
		payload = envelope.Payload
	}

	empty, err := legacySectionEmpty(payload)
	if err != nil {
		return fmt.Errorf("decode legacy snapshot: %w", err)
	}
	if !empty {
		return errors.New(legacyValueExportInstruction)
	}

	// An empty legacy snapshot carries no metadata. Clear any existing metadata
	// so the restored state matches the snapshot rather than the local store.
	return f.restoreMetadataPayload(nil)
}

// restoreMetadataPayload replaces metadata state. An empty payload clears it.
func (f *FSM) restoreMetadataPayload(payload []byte) error {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	snapshotter, ok := f.store.(metadataSnapshotter)
	if !ok {
		return nil
	}
	return snapshotter.RestoreMetadataPayload(payload)
}

// legacySectionEmpty reports whether a legacy payload contains no values. It
// accepts the JSON array written by the SQLite store and the object written by
// the pre-SQLite in-memory store.
func legacySectionEmpty(raw []byte) (bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return true, nil
	}

	switch trimmed[0] {
	case '[':
		var rows []json.RawMessage
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return false, err
		}
		return len(rows) == 0, nil
	case '{':
		var rows map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &rows); err != nil {
			return false, err
		}
		return len(rows) == 0, nil
	default:
		return false, fmt.Errorf("unsupported legacy payload %q", string(trimmed))
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
