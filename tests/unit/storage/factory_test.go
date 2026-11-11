package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"distributed-kv-store/internal/storage"
)

func TestOpenRequiresAPath(t *testing.T) {
	if _, err := storage.Open(storage.StoreConfig{}); err == nil {
		t.Fatal("Open accepted an empty configuration")
	}
}

func TestOpenDefaultsToDataDirNodeDB(t *testing.T) {
	dataDir := t.TempDir()

	store, err := storage.Open(storage.StoreConfig{DataDir: dataDir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dataDir, "node.db")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("default database %q was not created: %v", path, err)
	}
}

func TestOpenUsesExplicitPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "custom.db")

	store, err := storage.Open(storage.StoreConfig{Path: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("explicit database %q was not created: %v", path, err)
	}
}
