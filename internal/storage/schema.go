package storage

import (
	"database/sql"
	"fmt"
)

// schemaStatements is the additive SQLite schema. Every statement is safe to
// run against an existing database: tables use IF NOT EXISTS, indexes use
// IF NOT EXISTS, and singleton rows use INSERT OR IGNORE. The legacy `store`
// table keeps its original name so an existing /data/node.db upgrades in place.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS store (
		key TEXT PRIMARY KEY,
		value BLOB NOT NULL,
		is_parity INTEGER NOT NULL DEFAULT 0 CHECK (is_parity IN (0,1)),
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

// migrate creates the schema idempotently and seeds the singleton rows.
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

	return nil
}
