package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// defaultBusyTimeout is used by InitDB and NewSQLiteStore.
const defaultBusyTimeout = 5 * time.Second

// InitDB opens or creates a SQLite database for a single storage node and
// applies the full schema. Each Docker node uses its own database file, for
// example /data/node.db.
func InitDB(dbPath string) (*sql.DB, error) {
	return openDB(dbPath, defaultBusyTimeout)
}

// openDB opens a SQLite database with the production pragmas and applies the
// additive schema. A single connection avoids competing in-process writers.
func openDB(dbPath string, busyTimeout time.Duration) (*sql.DB, error) {
	if dbPath == "" {
		return nil, errors.New("database path cannot be empty")
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o750); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if busyTimeout <= 0 {
		busyTimeout = defaultBusyTimeout
	}

	pragmas := []string{
		fmt.Sprintf("PRAGMA busy_timeout = %d;", busyTimeout.Milliseconds()),
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = FULL;",
		"PRAGMA foreign_keys = ON;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure SQLite: %w", err)
		}
	}

	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite database: %w", err)
	}

	return db, nil
}

// SaveChunk inserts or updates a legacy full-value chunk in the local
// database. isParity identifies Reed-Solomon parity fragments.
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

// GetChunk retrieves the legacy full-value chunk for a key. Missing keys return
// an error wrapping ErrNotFound.
func GetChunk(db *sql.DB, key string) ([]byte, error) {
	if db == nil {
		return nil, errors.New("database connection is nil")
	}
	if key == "" {
		return nil, errors.New("chunk key cannot be empty")
	}

	var value []byte
	err := db.QueryRow("SELECT value FROM store WHERE key = ?", key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: chunk %q", ErrNotFound, key)
		}
		return nil, fmt.Errorf("get chunk %q: %w", key, err)
	}

	return value, nil
}

// SQLiteStore is the production persistent Store implementation.
type SQLiteStore struct {
	db     *sql.DB
	path   string
	closed atomic.Bool
}

// NewSQLiteStore initializes a persistent SQLite-backed Store at dbPath.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	return newSQLiteStore(dbPath, defaultBusyTimeout)
}

func newSQLiteStore(dbPath string, busyTimeout time.Duration) (*SQLiteStore, error) {
	db, err := openDB(dbPath, busyTimeout)
	if err != nil {
		return nil, err
	}

	return &SQLiteStore{db: db, path: dbPath}, nil
}

// ready reports ErrClosed when the store is nil or has been closed.
func (s *SQLiteStore) ready() error {
	if s == nil || s.db == nil || s.closed.Load() {
		return ErrClosed
	}
	return nil
}

// Put implements the legacy KVStore.Put method.
func (s *SQLiteStore) Put(key string, value []byte, isParity bool) error {
	if err := s.ready(); err != nil {
		return err
	}
	return SaveChunk(s.db, key, value, isParity)
}

// Get implements the legacy KVStore.Get method.
func (s *SQLiteStore) Get(key string) ([]byte, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	return GetChunk(s.db, key)
}

// Checkpoint truncates the write-ahead log. It is useful before measuring
// storage usage or before shutting down.
func (s *SQLiteStore) Checkpoint() error {
	if err := s.ready(); err != nil {
		return err
	}
	if _, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);"); err != nil {
		return fmt.Errorf("checkpoint SQLite WAL: %w", err)
	}
	return nil
}

// Close checkpoints the WAL, releases the database connection, and marks the
// store closed. It is idempotent.
func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil || s.closed.Load() {
		return nil
	}
	if s.closed.Swap(true) {
		return nil
	}
	_, _ = s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	return s.db.Close()
}
