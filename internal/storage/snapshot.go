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
	snapshotKindLegacy    = "legacy"
	snapshotKindMetadata  = "metadata"
)

// snapshotEnvelope is the versioned wrapper stored by the storage layer. The
// payload is deliberately raw so the legacy and metadata snapshots can share an
// envelope without sharing a schema.
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

// SnapshotLegacyPayload serializes the transitional legacy KV table without the
// storage envelope. The consensus FSM wraps this payload in its own versioned
// snapshot envelope.
func (s *SQLiteStore) SnapshotLegacyPayload() ([]byte, error) {
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

	return json.Marshal(records)
}

// Snapshot serializes the transitional legacy KV table as a versioned JSON
// envelope. It is used by stores that do not implement the FSM's snapshot
// payload interfaces.
func (s *SQLiteStore) Snapshot() ([]byte, error) {
	payload, err := s.SnapshotLegacyPayload()
	if err != nil {
		return nil, err
	}
	return encodeSnapshot(snapshotKindLegacy, json.RawMessage(payload))
}

// RestoreLegacyPayload replaces the legacy KV table from the raw payload used
// by the FSM envelope.
func (s *SQLiteStore) RestoreLegacyPayload(data []byte) error {
	if err := s.ready(); err != nil {
		return err
	}

	var records []legacyKVRow
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("decode legacy snapshot payload: %w", err)
	}
	return s.restoreLegacyRows(records)
}

// Restore replaces the legacy KV table from a snapshot. It also accepts the
// raw JSON map format written by the previous in-memory store so an existing
// snapshot directory survives the transition to SQLite.
func (s *SQLiteStore) Restore(data []byte) error {
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
	return s.RestoreLegacyPayload(payload)
}

func (s *SQLiteStore) restoreLegacyRows(records []legacyKVRow) error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin legacy restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ctx := context.Background()
	if err := clearLegacyRowsTx(ctx, tx); err != nil {
		return err
	}
	if err := insertLegacyRowsTx(ctx, tx, records); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit legacy restore: %w", err)
	}
	return nil
}

func clearLegacyRowsTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM store`); err != nil {
		return fmt.Errorf("clear legacy table: %w", err)
	}
	return nil
}

func insertLegacyRowsTx(ctx context.Context, tx *sql.Tx, records []legacyKVRow) error {
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
	return nil
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

// RestoreFullSnapshot restores the metadata and legacy sections in one SQLite
// transaction. A nil section is left untouched; fragment rows are never
// restored. The consensus FSM prefers this method so a version 2 snapshot is
// applied atomically during the legacy transition.
func (s *SQLiteStore) RestoreFullSnapshot(metadataPayload, legacyPayload []byte) error {
	if err := s.ready(); err != nil {
		return err
	}

	var metadata metadataSnapshotPayload
	if len(metadataPayload) > 0 {
		if err := json.Unmarshal(metadataPayload, &metadata); err != nil {
			return fmt.Errorf("decode metadata snapshot payload: %w", err)
		}
	}

	var legacy []legacyKVRow
	if len(legacyPayload) > 0 {
		if err := json.Unmarshal(legacyPayload, &legacy); err != nil {
			return fmt.Errorf("decode legacy snapshot payload: %w", err)
		}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin snapshot restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ctx := context.Background()
	if len(legacyPayload) > 0 {
		if err := clearLegacyRowsTx(ctx, tx); err != nil {
			return err
		}
		if err := insertLegacyRowsTx(ctx, tx, legacy); err != nil {
			return err
		}
	}
	if len(metadataPayload) > 0 {
		if err := restoreMetadataRowsTx(ctx, tx, metadata); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot restore: %w", err)
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
