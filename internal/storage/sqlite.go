package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// InitDB opens or creates a SQLite database for a single storage node.
// Each Docker node should use its own database file, e.g. /data/node.db.
func InitDB(dbPath string) (*sql.DB, error) {
	if dbPath == "" {
		return nil, errors.New("database path cannot be empty")
	}

	// Create the parent directory if it does not already exist.
	if err := os.MkdirAll(filepath.Dir(dbPath), 0750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}

	// One connection avoids competing SQLite writers within this process.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Configure SQLite for durable writes and safe concurrent access.
	pragmas := []string{
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = FULL;",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("configure SQLite: %w", err)
		}
	}

	// BLOB preserves arbitrary binary fragments, including RS parity bytes.
	// INTEGER stores 0 for data and 1 for parity.
	schema := `
	CREATE TABLE IF NOT EXISTS store (
		key TEXT PRIMARY KEY NOT NULL,
		value BLOB NOT NULL,
		is_parity INTEGER NOT NULL
			CHECK (is_parity IN (0, 1)),
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize SQLite schema: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping SQLite database: %w", err)
	}

	return db, nil
}

// SaveChunk inserts or updates a binary fragment in the local database.
// isParity identifies Reed-Solomon parity fragments.
func SaveChunk(db *sql.DB, key string, value []byte, isParity bool) error {
	if db == nil {
		return errors.New("database connection is nil")
	}
	if key == "" {
		return errors.New("chunk key cannot be empty")
	}
	if value == nil {
		return errors.New("chunk value cannot be nil")
	}

	query := `
	INSERT INTO store (key, value, is_parity)
	VALUES (?, ?, ?)
	ON CONFLICT(key) DO UPDATE SET
		value = excluded.value,
		is_parity = excluded.is_parity,
		updated_at = CURRENT_TIMESTAMP;`

	if _, err := db.Exec(query, key, value, isParity); err != nil {
		return fmt.Errorf("save chunk %q: %w", key, err)
	}

	return nil
}

// GetChunk retrieves the original binary fragment for a given key.
// Missing keys return an error wrapping sql.ErrNoRows.
func GetChunk(db *sql.DB, key string) ([]byte, error) {
	if db == nil {
		return nil, errors.New("database connection is nil")
	}
	if key == "" {
		return nil, errors.New("chunk key cannot be empty")
	}

	var value []byte

	err := db.QueryRow(
		"SELECT value FROM store WHERE key = ?",
		key,
	).Scan(&value)

	if err != nil {
		return nil, fmt.Errorf("get chunk %q: %w", key, err)
	}

	return value, nil
}

// SQLiteStore implements the existing KVStore interface used by Raft.
type SQLiteStore struct {
	db *sql.DB
}

// Compile-time check that SQLiteStore satisfies KVStore.
var _ KVStore = (*SQLiteStore)(nil)

// NewSQLiteStore initializes a persistent SQLite-backed KVStore.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := InitDB(dbPath)
	if err != nil {
		return nil, err
	}

	return &SQLiteStore{db: db}, nil
}

// Put implements KVStore.Put.
func (s *SQLiteStore) Put(key string, value []byte, isParity bool) error {
	if s == nil {
		return errors.New("SQLiteStore is nil")
	}
	return SaveChunk(s.db, key, value, isParity)
}

// Get implements KVStore.Get.
func (s *SQLiteStore) Get(key string) ([]byte, error) {
	if s == nil {
		return nil, errors.New("SQLiteStore is nil")
	}
	return GetChunk(s.db, key)
}

// Close releases the database connection.
func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
