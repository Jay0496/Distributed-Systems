package storage

import (
	"testing"
	"os"
)

func TestInitDB(t *testing.T) {
	dbPath := "test_node.db"
	defer os.Remove(dbPath)

	db, err := InitDB(dbPath)
	if err != nil {
		t.Fatalf("Failed to initialize SQLite DB: %v", err)
	}
	defer db.Close()
}