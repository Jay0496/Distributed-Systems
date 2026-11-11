package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// StoreConfig configures Open.
type StoreConfig struct {
	// Path is the SQLite database file. When empty it defaults to
	// filepath.Join(DataDir, "node.db").
	Path string
	// DataDir is used to derive Path when Path is empty.
	DataDir string
	// BusyTimeout bounds SQLite lock waits. Zero uses 5 seconds.
	BusyTimeout time.Duration
}

// DefaultStorePath returns the production database path for a data directory.
func DefaultStorePath(dataDir string) string {
	return filepath.Join(dataDir, "node.db")
}

// Open creates or opens the production SQLite store. It fails fast when the
// path cannot be resolved or the database cannot be opened.
func Open(cfg StoreConfig) (Store, error) {
	path := cfg.Path
	if path == "" {
		if cfg.DataDir == "" {
			return nil, errors.New("storage: store path or data directory is required")
		}
		path = DefaultStorePath(cfg.DataDir)
	}

	store, err := newSQLiteStore(path, cfg.BusyTimeout)
	if err != nil {
		return nil, fmt.Errorf("open store at %s: %w", path, err)
	}
	return store, nil
}
