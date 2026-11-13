package storage

import (
	"database/sql"
	"fmt"
)

// schemaStatements is the additive SQLite schema. Every statement is safe to
// run against an existing database: tables use IF NOT EXISTS, indexes use
// IF NOT EXISTS, and singleton rows use INSERT OR IGNORE.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,

	`CREATE TABLE IF NOT EXISTS stripes (
		stripe_id TEXT PRIMARY KEY,
		key TEXT NOT NULL,
		mode TEXT NOT NULL,
		size INTEGER NOT NULL,
		checksum TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,

	`CREATE TABLE IF NOT EXISTS keys (
		key TEXT PRIMARY KEY,
		stripe_id TEXT NOT NULL REFERENCES stripes(stripe_id) ON DELETE CASCADE,
		mode TEXT NOT NULL,
		size INTEGER NOT NULL,
		checksum TEXT NOT NULL,
		version INTEGER NOT NULL,
		degraded INTEGER NOT NULL DEFAULT 0 CHECK (degraded IN (0,1)),
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,

	`CREATE TABLE IF NOT EXISTS placements (
		stripe_id TEXT NOT NULL REFERENCES stripes(stripe_id) ON DELETE CASCADE,
		shard_index INTEGER NOT NULL,
		node_id TEXT NOT NULL,
		is_parity INTEGER NOT NULL CHECK (is_parity IN (0,1)),
		PRIMARY KEY (stripe_id, shard_index)
	)`,

	// No foreign key from fragments to stripes: a fragment can exist briefly
	// before its metadata commits, or after a stripe is superseded. The orphan
	// sweep removes those rows.
	`CREATE TABLE IF NOT EXISTS fragments (
		stripe_id TEXT NOT NULL,
		shard_index INTEGER NOT NULL,
		is_parity INTEGER NOT NULL CHECK (is_parity IN (0,1)),
		data BLOB NOT NULL,
		checksum TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (stripe_id, shard_index)
	)`,

	`CREATE TABLE IF NOT EXISTS cluster_config (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		default_mode TEXT NOT NULL,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,

	`CREATE TABLE IF NOT EXISTS raft_state (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		last_applied_index INTEGER NOT NULL DEFAULT 0
	)`,

	`CREATE INDEX IF NOT EXISTS idx_keys_stripe_id ON keys(stripe_id)`,
	`CREATE INDEX IF NOT EXISTS idx_fragments_stripe_id ON fragments(stripe_id)`,
	`CREATE INDEX IF NOT EXISTS idx_fragments_created_at ON fragments(created_at)`,
	`CREATE INDEX IF NOT EXISTS idx_placements_node_id ON placements(node_id)`,
}

// legacyStoreTable is the transitional full-value table created by earlier
// releases. It is dropped only when empty; otherwise startup fails so values
// are never discarded silently.
const legacyStoreTable = "store"

// migrate creates the schema idempotently, seeds the singleton rows, and runs
// the guarded removal of the legacy full-value table.
func migrate(db *sql.DB) error {
	for _, statement := range schemaStatements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}

	if _, err := db.Exec(
		`INSERT OR IGNORE INTO raft_state (id, last_applied_index) VALUES (1, 0)`,
	); err != nil {
		return fmt.Errorf("seed raft_state: %w", err)
	}

	if err := dropEmptyLegacyStore(db); err != nil {
		return err
	}

	if _, err := db.Exec(
		`INSERT OR IGNORE INTO schema_migrations (version) VALUES (3)`,
	); err != nil {
		return fmt.Errorf("record schema migration: %w", err)
	}

	return nil
}

// dropEmptyLegacyStore refuses to drop the legacy table while it holds rows.
// The message tells the operator how to preserve the data first.
func dropEmptyLegacyStore(db *sql.DB) error {
	exists, err := tableExists(db, legacyStoreTable)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	var count int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM store`).Scan(&count); err != nil {
		return fmt.Errorf("count legacy store rows: %w", err)
	}
	if count > 0 {
		return fmt.Errorf(
			"legacy table %q still holds %d value(s) that cannot be converted to stripes; "+
				"export them with the last release that supports the legacy path, rewrite each key through the current CLI, "+
				"then delete the rows or start with a fresh data directory",
			legacyStoreTable, count)
	}

	if _, err := db.Exec(`DROP TABLE store`); err != nil {
		return fmt.Errorf("drop empty legacy table: %w", err)
	}
	return nil
}

func tableExists(db *sql.DB, name string) (bool, error) {
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("inspect SQLite schema: %w", err)
	}
	return count > 0, nil
}
