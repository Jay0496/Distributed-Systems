package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	snapshotSchemaVersion = 1
	snapshotKindLegacy    = "legacy"
	snapshotKindMetadata  = "metadata"
)

// snapshotEnvelope is the versioned wrapper stored by the FSM. The payload is
// deliberately raw so the legacy and metadata snapshots can share an envelope
// without sharing a schema.
type snapshotEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Kind          string          `json:"kind"`
	Payload       json.RawMessage `json:"payload"`
}

type legacyKVRow struct {
	Key      string `json:"key"`
	Value    []byte `json:"value"`
	IsParity bool   `json:"is_parity"`
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

var _ Snapshotter = (*SQLiteStore)(nil)

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

// Snapshot serializes the transitional legacy KV table as a versioned JSON
// envelope. It is used by the FSM until task 06 switches to metadata-only
// snapshots.
func (s *SQLiteStore) Snapshot() ([]byte, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(context.Background(),
		`SELECT key, value, is_parity FROM store ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("read legacy snapshot rows: %w", err)
	}
	defer rows.Close()

	records := make([]legacyKVRow, 0)
	for rows.Next() {
		var (
			record   legacyKVRow
			isParity int
		)
		if err := rows.Scan(&record.Key, &record.Value, &isParity); err != nil {
			return nil, fmt.Errorf("scan legacy snapshot row: %w", err)
		}
		record.IsParity = isParity == 1
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy snapshot rows: %w", err)
	}

	return encodeSnapshot(snapshotKindLegacy, records)
}

// Restore replaces the legacy KV table from a snapshot. It also accepts the
// raw JSON map format written by the previous in-memory store so an existing
// snapshot directory survives the transition to SQLite.
func (s *SQLiteStore) Restore(data []byte) error {
	if err := s.ready(); err != nil {
		return err
	}

	payload, err := decodeSnapshot(data, snapshotKindLegacy)
	if err != nil {
		var legacy map[string][]byte
		if jsonErr := json.Unmarshal(data, &legacy); jsonErr != nil {
			return err
		}
		records := make([]legacyKVRow, 0, len(legacy))
		for key, value := range legacy {
			records = append(records, legacyKVRow{Key: key, Value: value})
		}
		return s.restoreLegacyRows(records)
	}

	var records []legacyKVRow
	if err := json.Unmarshal(payload, &records); err != nil {
		return fmt.Errorf("decode legacy snapshot payload: %w", err)
	}
	return s.restoreLegacyRows(records)
}

func (s *SQLiteStore) restoreLegacyRows(records []legacyKVRow) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin legacy restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ctx := context.Background()
	if _, err := tx.ExecContext(ctx, `DELETE FROM store`); err != nil {
		return fmt.Errorf("clear legacy table: %w", err)
	}

	for _, record := range records {
		if strings.TrimSpace(record.Key) == "" {
			return errors.New("snapshot contains an empty key")
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO store (key, value, is_parity)
			VALUES (?, ?, ?)
			ON CONFLICT(key) DO UPDATE SET
				value = excluded.value,
				is_parity = excluded.is_parity,
				updated_at = CURRENT_TIMESTAMP`,
			record.Key, record.Value, record.IsParity,
		); err != nil {
			return fmt.Errorf("restore legacy row %q: %w", record.Key, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit legacy restore: %w", err)
	}
	return nil
}

// SnapshotMetadata serializes metadata only: stripes, keys, placements, the
// singleton config, and the applied index. Fragment payloads are never
// included; they are durable on their owners and repaired from peers.
func (s *SQLiteStore) SnapshotMetadata() ([]byte, error) {
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

	return encodeSnapshot(snapshotKindMetadata, payload)
}

// RestoreMetadata atomically replaces metadata state from a metadata snapshot.
// Fragment rows are intentionally left untouched.
func (s *SQLiteStore) RestoreMetadata(data []byte) error {
	if err := s.ready(); err != nil {
		return err
	}

	raw, err := decodeSnapshot(data, snapshotKindMetadata)
	if err != nil {
		return err
	}

	var payload metadataSnapshotPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("decode metadata snapshot payload: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin metadata restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ctx := context.Background()
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metadata restore: %w", err)
	}
	return nil
}
