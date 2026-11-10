package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	sqliteTimeLayout = "2006-01-02 15:04:05"
)

func checksumBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, sqliteTimeLayout} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised timestamp %q", raw)
}

func validateFragmentKey(stripeID string, shardIndex int) error {
	if strings.TrimSpace(stripeID) == "" {
		return errors.New("fragment stripe ID must not be empty")
	}
	if shardIndex < 0 || shardIndex > 4 {
		return fmt.Errorf("invalid fragment shard index %d (expected 0..4)", shardIndex)
	}
	return nil
}

// PutFragment upserts a local fragment. The checksum is verified before the
// write; an empty checksum is computed from the data. created_at is preserved
// when an existing fragment is updated.
func (s *SQLiteStore) PutFragment(frag Fragment) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := validateFragmentKey(frag.StripeID, frag.ShardIndex); err != nil {
		return err
	}

	computed := checksumBytes(frag.Data)
	if frag.Checksum == "" {
		frag.Checksum = computed
	} else {
		if err := validateChecksum(frag.Checksum); err != nil {
			return err
		}
		if !strings.EqualFold(frag.Checksum, computed) {
			return fmt.Errorf("fragment checksum mismatch for stripe %q shard %d", frag.StripeID, frag.ShardIndex)
		}
		frag.Checksum = computed
	}

	createdAt := frag.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	_, err := s.db.ExecContext(context.Background(), `
		INSERT INTO fragments (stripe_id, shard_index, is_parity, data, checksum, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(stripe_id, shard_index) DO UPDATE SET
			is_parity = excluded.is_parity,
			data = excluded.data,
			checksum = excluded.checksum`,
		frag.StripeID, frag.ShardIndex, frag.IsParity, frag.Data, frag.Checksum, formatTime(createdAt),
	)
	if err != nil {
		return fmt.Errorf("put fragment %q/%d: %w", frag.StripeID, frag.ShardIndex, err)
	}
	return nil
}

// GetFragment returns one local fragment and verifies its checksum.
func (s *SQLiteStore) GetFragment(stripeID string, shardIndex int) (Fragment, error) {
	if err := s.ready(); err != nil {
		return Fragment{}, err
	}
	if err := validateFragmentKey(stripeID, shardIndex); err != nil {
		return Fragment{}, err
	}

	var (
		frag      Fragment
		isParity  int
		createdAt string
	)
	err := s.db.QueryRowContext(context.Background(), `
		SELECT is_parity, data, checksum, created_at
		FROM fragments WHERE stripe_id = ? AND shard_index = ?`,
		stripeID, shardIndex,
	).Scan(&isParity, &frag.Data, &frag.Checksum, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Fragment{}, fmt.Errorf("%w: fragment %q/%d", ErrNotFound, stripeID, shardIndex)
		}
		return Fragment{}, fmt.Errorf("get fragment %q/%d: %w", stripeID, shardIndex, err)
	}

	frag.StripeID = stripeID
	frag.ShardIndex = shardIndex
	frag.IsParity = isParity == 1
	frag.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return Fragment{}, fmt.Errorf("parse fragment timestamp: %w", err)
	}

	if !strings.EqualFold(frag.Checksum, checksumBytes(frag.Data)) {
		return Fragment{}, fmt.Errorf("stored fragment %q/%d failed checksum verification", stripeID, shardIndex)
	}
	return frag, nil
}

// DeleteFragment removes one local fragment. Deleting a missing fragment is a
// no-op so repair and garbage collection are idempotent.
func (s *SQLiteStore) DeleteFragment(stripeID string, shardIndex int) error {
	if err := s.ready(); err != nil {
		return err
	}
	if err := validateFragmentKey(stripeID, shardIndex); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(context.Background(),
		`DELETE FROM fragments WHERE stripe_id = ? AND shard_index = ?`,
		stripeID, shardIndex,
	); err != nil {
		return fmt.Errorf("delete fragment %q/%d: %w", stripeID, shardIndex, err)
	}
	return nil
}

// DeleteStripe removes a stripe's metadata, placements, and local fragments in
// one transaction. Deleting a missing stripe is a no-op.
func (s *SQLiteStore) DeleteStripe(stripeID string) error {
	if err := s.ready(); err != nil {
		return err
	}
	if strings.TrimSpace(stripeID) == "" {
		return errors.New("stripe ID must not be empty")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin delete stripe transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ctx := context.Background()
	for _, statement := range []string{
		`DELETE FROM placements WHERE stripe_id = ?`,
		`DELETE FROM fragments WHERE stripe_id = ?`,
		`DELETE FROM stripes WHERE stripe_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, statement, stripeID); err != nil {
			return fmt.Errorf("delete stripe %q: %w", stripeID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete stripe %q: %w", stripeID, err)
	}
	return nil
}

// ListFragments returns fragment metadata for a stripe without loading payload
// bytes. Results are ordered by shard index.
func (s *SQLiteStore) ListFragments(stripeID string) ([]FragmentRef, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(stripeID) == "" {
		return nil, errors.New("stripe ID must not be empty")
	}

	rows, err := s.db.QueryContext(context.Background(), `
		SELECT shard_index, is_parity, length(data), checksum, created_at
		FROM fragments WHERE stripe_id = ? ORDER BY shard_index`, stripeID)
	if err != nil {
		return nil, fmt.Errorf("list fragments for stripe %q: %w", stripeID, err)
	}
	defer rows.Close()

	var fragments []FragmentRef
	for rows.Next() {
		ref := FragmentRef{StripeID: stripeID}
		var (
			isParity  int
			createdAt string
		)
		if err := rows.Scan(&ref.ShardIndex, &isParity, &ref.Size, &ref.Checksum, &createdAt); err != nil {
			return nil, fmt.Errorf("scan fragment for stripe %q: %w", stripeID, err)
		}
		ref.IsParity = isParity == 1
		ref.CreatedAt, err = parseTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse fragment timestamp: %w", err)
		}
		fragments = append(fragments, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fragments for stripe %q: %w", stripeID, err)
	}
	return fragments, nil
}
