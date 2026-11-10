package storage

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	modeRS32        = "rs32"
	modeReplication = "replication"
)

// Compile-time check that SQLiteStore satisfies the production Store surface.
var _ Store = (*SQLiteStore)(nil)

// querier is satisfied by both *sql.DB and *sql.Tx.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func validMode(mode string) bool {
	return mode == modeRS32 || mode == modeReplication
}

func validateChecksum(checksum string) error {
	if len(checksum) != 64 {
		return fmt.Errorf("checksum must be 64 hex characters")
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return fmt.Errorf("checksum is not valid hex: %w", err)
	}
	return nil
}

// validateKeyMeta enforces the metadata invariants that storage can check
// without importing internal/redundancy.
func validateKeyMeta(meta KeyMeta) error {
	if strings.TrimSpace(meta.Key) == "" {
		return errors.New("key metadata must have a non-empty key")
	}
	if strings.TrimSpace(meta.StripeID) == "" {
		return errors.New("key metadata must have a non-empty stripe ID")
	}
	if !validMode(meta.Mode) {
		return fmt.Errorf("invalid metadata mode %q", meta.Mode)
	}
	if meta.Size < 0 {
		return fmt.Errorf("metadata size must be non-negative, got %d", meta.Size)
	}
	if err := validateChecksum(meta.Checksum); err != nil {
		return err
	}
	if meta.Version < 1 {
		return fmt.Errorf("metadata version must be at least 1, got %d", meta.Version)
	}
	if len(meta.Placement) != 5 {
		return fmt.Errorf("metadata must describe exactly five placements, got %d", len(meta.Placement))
	}

	seenIndex := make(map[int]struct{}, len(meta.Placement))
	seenNode := make(map[string]struct{}, len(meta.Placement))
	for _, placement := range meta.Placement {
		if placement.ShardIndex < 0 || placement.ShardIndex > 4 {
			return fmt.Errorf("invalid shard index %d", placement.ShardIndex)
		}
		if _, exists := seenIndex[placement.ShardIndex]; exists {
			return fmt.Errorf("duplicate shard index %d", placement.ShardIndex)
		}
		seenIndex[placement.ShardIndex] = struct{}{}

		if strings.TrimSpace(placement.NodeID) == "" {
			return errors.New("placement must have a non-empty node ID")
		}
		if _, exists := seenNode[placement.NodeID]; exists {
			return fmt.Errorf("duplicate placement node %q", placement.NodeID)
		}
		seenNode[placement.NodeID] = struct{}{}

		wantParity := meta.Mode == modeRS32 && placement.ShardIndex >= 3
		if placement.IsParity != wantParity {
			return fmt.Errorf("shard %d parity=%v does not match mode %q", placement.ShardIndex, placement.IsParity, meta.Mode)
		}
	}

	return nil
}

func validateStorageKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("key must not be empty")
	}
	return nil
}

func validateClusterConfig(cfg ClusterConfig) error {
	if !validMode(cfg.DefaultMode) {
		return fmt.Errorf("invalid default mode %q", cfg.DefaultMode)
	}
	return nil
}

func readAppliedIndex(q querier) (uint64, error) {
	var index int64
	err := q.QueryRowContext(context.Background(),
		`SELECT last_applied_index FROM raft_state WHERE id = 1`,
	).Scan(&index)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("read last applied index: %w", err)
	}
	if index < 0 {
		return 0, fmt.Errorf("last applied index is negative: %d", index)
	}
	return uint64(index), nil
}

func writeAppliedIndex(tx *sql.Tx, index uint64) error {
	_, err := tx.ExecContext(context.Background(), `
		INSERT INTO raft_state (id, last_applied_index) VALUES (1, ?)
		ON CONFLICT(id) DO UPDATE SET last_applied_index = excluded.last_applied_index`,
		int64(index),
	)
	if err != nil {
		return fmt.Errorf("update last applied index: %w", err)
	}
	return nil
}

// alreadyApplied reports whether the command was applied previously. Replaying
// an entry at or before last_applied_index is a no-op.
func alreadyApplied(tx *sql.Tx, raftIndex uint64) (bool, error) {
	applied, err := readAppliedIndex(tx)
	if err != nil {
		return false, err
	}
	return raftIndex <= applied, nil
}

func readKeyMeta(q querier, key string) (KeyMeta, error) {
	var (
		meta     KeyMeta
		degraded int
	)

	err := q.QueryRowContext(context.Background(), `
		SELECT stripe_id, mode, size, checksum, version, degraded
		FROM keys WHERE key = ?`, key,
	).Scan(&meta.StripeID, &meta.Mode, &meta.Size, &meta.Checksum, &meta.Version, &degraded)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return KeyMeta{}, fmt.Errorf("%w: key %q", ErrNotFound, key)
		}
		return KeyMeta{}, fmt.Errorf("read key %q: %w", key, err)
	}

	meta.Key = key
	meta.Degraded = degraded == 1

	rows, err := q.QueryContext(context.Background(), `
		SELECT shard_index, node_id, is_parity
		FROM placements WHERE stripe_id = ? ORDER BY shard_index`, meta.StripeID)
	if err != nil {
		return KeyMeta{}, fmt.Errorf("read placements for stripe %q: %w", meta.StripeID, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			placement ShardPlacement
			isParity  int
		)
		if err := rows.Scan(&placement.ShardIndex, &placement.NodeID, &isParity); err != nil {
			return KeyMeta{}, fmt.Errorf("scan placement for stripe %q: %w", meta.StripeID, err)
		}
		placement.IsParity = isParity == 1
		meta.Placement = append(meta.Placement, placement)
	}
	if err := rows.Err(); err != nil {
		return KeyMeta{}, fmt.Errorf("iterate placements for stripe %q: %w", meta.StripeID, err)
	}

	return meta, nil
}

// ApplyPutMeta installs new key metadata and advances the applied index in one
// transaction. It returns the previous metadata, or an empty KeyMeta when the
// key is new or the entry is a replay.
func (s *SQLiteStore) ApplyPutMeta(raftIndex uint64, meta KeyMeta) (KeyMeta, error) {
	if err := s.ready(); err != nil {
		return KeyMeta{}, err
	}
	if err := validateKeyMeta(meta); err != nil {
		return KeyMeta{}, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return KeyMeta{}, fmt.Errorf("begin put metadata transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	replay, err := alreadyApplied(tx, raftIndex)
	if err != nil {
		return KeyMeta{}, err
	}
	if replay {
		return KeyMeta{}, nil
	}

	old, err := readKeyMeta(tx, meta.Key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return KeyMeta{}, err
	}
	if errors.Is(err, ErrNotFound) {
		old = KeyMeta{}
	}

	if old.Key != "" {
		switch {
		case meta.Version < old.Version:
			return KeyMeta{}, fmt.Errorf("%w: key %q version %d is older than %d", ErrConflict, meta.Key, meta.Version, old.Version)
		case meta.Version == old.Version:
			if meta.StripeID != old.StripeID || meta.Checksum != old.Checksum || meta.Size != old.Size {
				return KeyMeta{}, fmt.Errorf("%w: key %q version %d already exists with different metadata", ErrConflict, meta.Key, meta.Version)
			}
			// Same version and same stripe: idempotent, but still advance the
			// applied index so a retried command does not replay forever.
			if err := writeAppliedIndex(tx, raftIndex); err != nil {
				return KeyMeta{}, err
			}
			if err := tx.Commit(); err != nil {
				return KeyMeta{}, fmt.Errorf("commit idempotent put metadata: %w", err)
			}
			return old, nil
		}
	}

	ctx := context.Background()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO stripes (stripe_id, key, mode, size, checksum)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(stripe_id) DO NOTHING`,
		meta.StripeID, meta.Key, meta.Mode, meta.Size, meta.Checksum,
	)
	if err != nil {
		return KeyMeta{}, fmt.Errorf("insert stripe %q: %w", meta.StripeID, err)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM placements WHERE stripe_id = ?`, meta.StripeID,
	); err != nil {
		return KeyMeta{}, fmt.Errorf("clear placements for stripe %q: %w", meta.StripeID, err)
	}

	for _, placement := range meta.Placement {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO placements (stripe_id, shard_index, node_id, is_parity)
			VALUES (?, ?, ?, ?)`,
			meta.StripeID, placement.ShardIndex, placement.NodeID, placement.IsParity,
		); err != nil {
			return KeyMeta{}, fmt.Errorf("insert placement for stripe %q: %w", meta.StripeID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO keys (key, stripe_id, mode, size, checksum, version, degraded)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			stripe_id = excluded.stripe_id,
			mode = excluded.mode,
			size = excluded.size,
			checksum = excluded.checksum,
			version = excluded.version,
			degraded = excluded.degraded,
			updated_at = CURRENT_TIMESTAMP`,
		meta.Key, meta.StripeID, meta.Mode, meta.Size, meta.Checksum, meta.Version, meta.Degraded,
	); err != nil {
		return KeyMeta{}, fmt.Errorf("upsert key %q: %w", meta.Key, err)
	}

	// A genuine overwrite supersedes the previous stripe. Delete its rows in
	// the same transaction so every node converges on the same metadata.
	if old.StripeID != "" && old.StripeID != meta.StripeID {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM stripes WHERE stripe_id = ?`, old.StripeID,
		); err != nil {
			return KeyMeta{}, fmt.Errorf("delete superseded stripe %q: %w", old.StripeID, err)
		}
	}

	if err := writeAppliedIndex(tx, raftIndex); err != nil {
		return KeyMeta{}, err
	}
	if err := tx.Commit(); err != nil {
		return KeyMeta{}, fmt.Errorf("commit put metadata: %w", err)
	}

	return old, nil
}

// ApplyMetaStatus updates only the degraded flag for an existing stripe and
// version. It never changes the user-visible version.
func (s *SQLiteStore) ApplyMetaStatus(raftIndex uint64, key, stripeID string, version int64, degraded bool) (KeyMeta, error) {
	if err := s.ready(); err != nil {
		return KeyMeta{}, err
	}
	if err := validateStorageKey(key); err != nil {
		return KeyMeta{}, err
	}
	if strings.TrimSpace(stripeID) == "" {
		return KeyMeta{}, errors.New("stripe ID must not be empty")
	}
	if version < 1 {
		return KeyMeta{}, fmt.Errorf("version must be at least 1, got %d", version)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return KeyMeta{}, fmt.Errorf("begin metadata status transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	replay, err := alreadyApplied(tx, raftIndex)
	if err != nil {
		return KeyMeta{}, err
	}
	if replay {
		return KeyMeta{}, nil
	}

	current, err := readKeyMeta(tx, key)
	if err != nil {
		return KeyMeta{}, err
	}
	if current.StripeID != stripeID || current.Version != version {
		return KeyMeta{}, fmt.Errorf("%w: key %q stripe/version mismatch", ErrConflict, key)
	}

	if _, err := tx.ExecContext(context.Background(), `
		UPDATE keys SET degraded = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?`,
		degraded, key,
	); err != nil {
		return KeyMeta{}, fmt.Errorf("update metadata status for %q: %w", key, err)
	}
	if err := writeAppliedIndex(tx, raftIndex); err != nil {
		return KeyMeta{}, err
	}
	if err := tx.Commit(); err != nil {
		return KeyMeta{}, fmt.Errorf("commit metadata status: %w", err)
	}

	current.Degraded = degraded
	return current, nil
}

// ApplyDeleteMeta removes a key and its local stripe rows and returns the old
// metadata so the caller can delete fragments on their remote owners.
func (s *SQLiteStore) ApplyDeleteMeta(raftIndex uint64, key string, expectedVersion int64) (KeyMeta, error) {
	if err := s.ready(); err != nil {
		return KeyMeta{}, err
	}
	if err := validateStorageKey(key); err != nil {
		return KeyMeta{}, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return KeyMeta{}, fmt.Errorf("begin delete metadata transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	replay, err := alreadyApplied(tx, raftIndex)
	if err != nil {
		return KeyMeta{}, err
	}
	if replay {
		return KeyMeta{}, nil
	}

	old, err := readKeyMeta(tx, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return KeyMeta{}, err
	}
	if errors.Is(err, ErrNotFound) {
		// Idempotent delete: the key may already have been removed.
		if err := writeAppliedIndex(tx, raftIndex); err != nil {
			return KeyMeta{}, err
		}
		if err := tx.Commit(); err != nil {
			return KeyMeta{}, fmt.Errorf("commit idempotent delete metadata: %w", err)
		}
		return KeyMeta{}, nil
	}

	if expectedVersion != 0 && expectedVersion != old.Version {
		return KeyMeta{}, fmt.Errorf("%w: key %q version %d does not match expected %d", ErrConflict, key, old.Version, expectedVersion)
	}

	ctx := context.Background()
	if _, err := tx.ExecContext(ctx, `DELETE FROM keys WHERE key = ?`, key); err != nil {
		return KeyMeta{}, fmt.Errorf("delete key %q: %w", key, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM stripes WHERE stripe_id = ?`, old.StripeID); err != nil {
		return KeyMeta{}, fmt.Errorf("delete stripe %q: %w", old.StripeID, err)
	}
	if err := writeAppliedIndex(tx, raftIndex); err != nil {
		return KeyMeta{}, err
	}
	if err := tx.Commit(); err != nil {
		return KeyMeta{}, fmt.Errorf("commit delete metadata: %w", err)
	}

	return old, nil
}

// ApplySetConfig upserts the singleton cluster configuration and advances the
// applied index.
func (s *SQLiteStore) ApplySetConfig(raftIndex uint64, cfg ClusterConfig) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := validateClusterConfig(cfg); err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin set config transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	replay, err := alreadyApplied(tx, raftIndex)
	if err != nil {
		return err
	}
	if replay {
		return nil
	}

	if err := putConfigTx(tx, cfg); err != nil {
		return err
	}
	if err := writeAppliedIndex(tx, raftIndex); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit set config: %w", err)
	}
	return nil
}

// GetKey returns the committed metadata for a key.
func (s *SQLiteStore) GetKey(key string) (KeyMeta, error) {
	if err := s.ready(); err != nil {
		return KeyMeta{}, err
	}
	if err := validateStorageKey(key); err != nil {
		return KeyMeta{}, err
	}
	return readKeyMeta(s.db, key)
}

// ListKeys returns all committed key metadata ordered by key. Placements are
// included because repair and GC need the owning node for each shard.
func (s *SQLiteStore) ListKeys() ([]KeyMeta, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(context.Background(), `SELECT key FROM keys ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer rows.Close()

	var keyNames []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan key name: %w", err)
		}
		keyNames = append(keyNames, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate keys: %w", err)
	}

	keys := make([]KeyMeta, 0, len(keyNames))
	for _, key := range keyNames {
		meta, err := readKeyMeta(s.db, key)
		if err != nil {
			return nil, err
		}
		keys = append(keys, meta)
	}
	return keys, nil
}

// LastAppliedIndex returns the last Raft index applied to metadata.
func (s *SQLiteStore) LastAppliedIndex() (uint64, error) {
	if err := s.ready(); err != nil {
		return 0, err
	}
	return readAppliedIndex(s.db)
}

// PutConfig writes the singleton cluster configuration without touching the
// applied index. It is for seeding and tests; the FSM uses ApplySetConfig.
func (s *SQLiteStore) PutConfig(cfg ClusterConfig) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := validateClusterConfig(cfg); err != nil {
		return err
	}
	return putConfigTx(s.db, cfg)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func putConfigTx(q execer, cfg ClusterConfig) error {
	if _, err := q.ExecContext(context.Background(), `
		INSERT INTO cluster_config (id, default_mode) VALUES (1, ?)
		ON CONFLICT(id) DO UPDATE SET
			default_mode = excluded.default_mode,
			updated_at = CURRENT_TIMESTAMP`, cfg.DefaultMode,
	); err != nil {
		return fmt.Errorf("write cluster config: %w", err)
	}
	return nil
}

// GetConfig returns the singleton cluster configuration.
func (s *SQLiteStore) GetConfig() (ClusterConfig, error) {
	if err := s.ready(); err != nil {
		return ClusterConfig{}, err
	}

	var cfg ClusterConfig
	err := s.db.QueryRowContext(context.Background(),
		`SELECT default_mode FROM cluster_config WHERE id = 1`,
	).Scan(&cfg.DefaultMode)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ClusterConfig{}, fmt.Errorf("%w: cluster config", ErrNotFound)
		}
		return ClusterConfig{}, fmt.Errorf("read cluster config: %w", err)
	}
	return cfg, nil
}
