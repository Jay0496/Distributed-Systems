package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	snapshotSchemaVersion = 1
	snapshotKindMetadata  = "metadata"
)

// snapshotEnvelope is the versioned wrapper used by the storage-level metadata
// snapshot helpers. The consensus FSM uses the raw payload so its own envelope
// is not nested.
type snapshotEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Kind          string          `json:"kind"`
	Payload       json.RawMessage `json:"payload"`
}

type stripeRecord struct {
	StripeID string `json:"stripe_id"`
	Key      string `json:"key"`
	Mode     string `json:"mode"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

type metadataSnapshotPayload struct {
	Stripes          []stripeRecord `json:"stripes"`
	Keys             []KeyMeta      `json:"keys"`
	Config           *ClusterConfig `json:"config,omitempty"`
	LastAppliedIndex uint64         `json:"last_applied_index"`
}

func encodeSnapshot(kind string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %s snapshot payload: %w", kind, err)
	}
	envelope := snapshotEnvelope{
		SchemaVersion: snapshotSchemaVersion,
		Kind:          kind,
		Payload:       raw,
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal %s snapshot envelope: %w", kind, err)
	}
	return data, nil
}

func decodeSnapshot(data []byte, wantKind string) (json.RawMessage, error) {
	var envelope snapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode snapshot envelope: %w", err)
	}
	if envelope.SchemaVersion == 0 && envelope.Kind == "" {
		return nil, errors.New("snapshot has no envelope")
	}
	if envelope.SchemaVersion != snapshotSchemaVersion {
		return nil, fmt.Errorf("unsupported snapshot schema version %d", envelope.SchemaVersion)
	}
	if envelope.Kind != wantKind {
		return nil, fmt.Errorf("snapshot kind %q, want %q", envelope.Kind, wantKind)
	}
	return envelope.Payload, nil
}

// SnapshotMetadataPayload serializes metadata only without the storage
// envelope: stripes, keys, placements, the singleton config, and the applied
// index. Fragment payloads are never included; they are durable on their owners
// and repaired from peers.
func (s *SQLiteStore) SnapshotMetadataPayload() ([]byte, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}

	payload := metadataSnapshotPayload{}

	rows, err := s.db.QueryContext(context.Background(),
		`SELECT stripe_id, key, mode, size, checksum FROM stripes ORDER BY stripe_id`)
	if err != nil {
		return nil, fmt.Errorf("read stripes for metadata snapshot: %w", err)
	}
	for rows.Next() {
		var record stripeRecord
		if err := rows.Scan(&record.StripeID, &record.Key, &record.Mode, &record.Size, &record.Checksum); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan stripe for metadata snapshot: %w", err)
		}
		payload.Stripes = append(payload.Stripes, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate stripes for metadata snapshot: %w", err)
	}
	rows.Close()

	keys, err := s.ListKeys()
	if err != nil {
		return nil, err
	}
	payload.Keys = keys

	if cfg, err := s.GetConfig(); err == nil {
		payload.Config = &cfg
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	idx, err := s.LastAppliedIndex()
	if err != nil {
		return nil, err
	}
	payload.LastAppliedIndex = idx

	return json.Marshal(payload)
}

// SnapshotMetadata serializes metadata inside the storage-level envelope. The
// FSM uses SnapshotMetadataPayload instead so its own envelope is not nested.
func (s *SQLiteStore) SnapshotMetadata() ([]byte, error) {
	payload, err := s.SnapshotMetadataPayload()
	if err != nil {
		return nil, err
	}
	return encodeSnapshot(snapshotKindMetadata, json.RawMessage(payload))
}

// RestoreMetadata atomically replaces metadata state from a metadata snapshot.
func (s *SQLiteStore) RestoreMetadata(data []byte) error {
	raw, err := decodeSnapshot(data, snapshotKindMetadata)
	if err != nil {
		return err
	}
	return s.RestoreMetadataPayload(raw)
}

// RestoreMetadataPayload atomically replaces metadata state from the raw
// metadata payload used by the FSM envelope. Fragment rows are intentionally
// left untouched.
func (s *SQLiteStore) RestoreMetadataPayload(data []byte) error {
	if err := s.ready(); err != nil {
		return err
	}

	var payload metadataSnapshotPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("decode metadata snapshot payload: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin metadata restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := restoreMetadataRowsTx(context.Background(), tx, payload); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata restore: %w", err)
	}
	return nil
}

func restoreMetadataRowsTx(ctx context.Context, tx *sql.Tx, payload metadataSnapshotPayload) error {
	for _, statement := range []string{
		`DELETE FROM keys`,
		`DELETE FROM placements`,
		`DELETE FROM stripes`,
		`DELETE FROM cluster_config`,
		`DELETE FROM raft_state`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("clear metadata during restore: %w", err)
		}
	}

	for _, record := range payload.Stripes {
		if strings.TrimSpace(record.StripeID) == "" {
			return errors.New("metadata snapshot contains an empty stripe ID")
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO stripes (stripe_id, key, mode, size, checksum)
			VALUES (?, ?, ?, ?, ?)`,
			record.StripeID, record.Key, record.Mode, record.Size, record.Checksum,
		); err != nil {
			return fmt.Errorf("restore stripe %q: %w", record.StripeID, err)
		}
	}

	for _, meta := range payload.Keys {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO keys (key, stripe_id, mode, size, checksum, version, degraded)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			meta.Key, meta.StripeID, meta.Mode, meta.Size, meta.Checksum, meta.Version, meta.Degraded,
		); err != nil {
			return fmt.Errorf("restore key %q: %w", meta.Key, err)
		}
		for _, placement := range meta.Placement {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO placements (stripe_id, shard_index, node_id, is_parity)
				VALUES (?, ?, ?, ?)`,
				meta.StripeID, placement.ShardIndex, placement.NodeID, placement.IsParity,
			); err != nil {
				return fmt.Errorf("restore placement for stripe %q: %w", meta.StripeID, err)
			}
		}
	}

	if payload.Config != nil {
		if err := putConfigTx(tx, *payload.Config); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO raft_state (id, last_applied_index) VALUES (1, ?)`,
		int64(payload.LastAppliedIndex),
	); err != nil {
		return fmt.Errorf("restore applied index: %w", err)
	}

	return nil
}
