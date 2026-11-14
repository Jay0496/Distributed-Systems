package storage_test

import (
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/storage"
)

// TestSchemaCreatesTablesAndIndexes verifies the full additive schema is
// present after the first open.
func TestSchemaCreatesTablesAndIndexes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")

	db, err := storage.InitDB(path)
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	wantTables := []string{
		"schema_migrations",
		"stripes",
		"keys",
		"placements",
		"fragments",
		"cluster_config",
		"raft_state",
	}
	for _, table := range wantTables {
		var name string
		err := db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
		).Scan(&name)
		if err != nil {
			t.Errorf("table %q is missing: %v", table, err)
		}
	}

	wantIndexes := []string{
		"idx_keys_stripe_id",
		"idx_fragments_stripe_id",
		"idx_fragments_created_at",
		"idx_placements_node_id",
	}
	for _, index := range wantIndexes {
		var name string
		err := db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, index,
		).Scan(&name)
		if err != nil {
			t.Errorf("index %q is missing: %v", index, err)
		}
	}

	var applied int64
	if err := db.QueryRow(`SELECT last_applied_index FROM raft_state WHERE id = 1`).Scan(&applied); err != nil {
		t.Fatalf("raft_state singleton missing: %v", err)
	}
	if applied != 0 {
		t.Errorf("last_applied_index = %d, want 0", applied)
	}
}

// TestSchemaMigrationIsIdempotent reopens the same database twice.
func TestSchemaMigrationIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")

	first, err := storage.InitDB(path)
	if err != nil {
		t.Fatalf("first InitDB: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first database: %v", err)
	}

	second, err := storage.InitDB(path)
	if err != nil {
		t.Fatalf("second InitDB: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	var tables int
	if err := second.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('schema_migrations','stripes','keys','placements','fragments','cluster_config','raft_state')`,
	).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 7 {
		t.Fatalf("tables = %d, want 7", tables)
	}

	var legacy int
	if err := second.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'store'`,
	).Scan(&legacy); err != nil {
		t.Fatalf("count legacy table: %v", err)
	}
	if legacy != 0 {
		t.Fatalf("legacy store table still exists")
	}
}
