package storage

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestSQLiteReopenPersistence verifies that data and parity fragments remain
// available after the database is closed and reopened.
func TestSQLiteReopenPersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("initialize SQLite: %v", err)
	}

	data := []byte("distributed database fragment")
	parity := []byte{0x00, 0xFF, 0x01, 0x80, 0x42}

	if err := store.Put("stripe1:data0", data, false); err != nil {
		t.Fatalf("save data fragment: %v", err)
	}

	if err := store.Put("stripe1:parity0", parity, true); err != nil {
		t.Fatalf("save parity fragment: %v", err)
	}

	// Verify parity metadata was actually written.
	var parityFlag int
	err = store.db.QueryRow(
		"SELECT is_parity FROM store WHERE key = ?",
		"stripe1:parity0",
	).Scan(&parityFlag)

	if err != nil || parityFlag != 1 {
		t.Fatalf("expected parity flag 1, got %d, err=%v",
			parityFlag, err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	// Reopen the same physical database file.
	reopened, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen SQLite: %v", err)
	}
	defer reopened.Close()

	gotData, err := reopened.Get("stripe1:data0")
	if err != nil {
		t.Fatalf("retrieve data after reopen: %v", err)
	}

	if !bytes.Equal(gotData, data) {
		t.Fatalf("data mismatch: got %v, want %v", gotData, data)
	}

	gotParity, err := reopened.Get("stripe1:parity0")
	if err != nil {
		t.Fatalf("retrieve parity after reopen: %v", err)
	}

	if !bytes.Equal(gotParity, parity) {
		t.Fatalf("parity mismatch: got %v, want %v", gotParity, parity)
	}
}

// TestUpsertAndMissingKey checks updating an existing chunk and
// correctly reporting a key that does not exist.
func TestUpsertAndMissingKey(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")

	db, err := InitDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := SaveChunk(db, "key1", []byte("original"), false); err != nil {
		t.Fatal(err)
	}

	if err := SaveChunk(db, "key1", []byte("updated"), true); err != nil {
		t.Fatal(err)
	}

	value, err := GetChunk(db, "key1")
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "updated" {
		t.Fatalf("expected updated, got %q", string(value))
	}

	var parityFlag int
	if err := db.QueryRow(
		"SELECT is_parity FROM store WHERE key = ?",
		"key1",
	).Scan(&parityFlag); err != nil {
		t.Fatal(err)
	}

	if parityFlag != 1 {
		t.Fatalf("expected updated parity flag 1, got %d", parityFlag)
	}

	_, err = GetChunk(db, "missing-key")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestNodeIsolation verifies that different node database files
// do not automatically share stored fragments.
func TestNodeIsolation(t *testing.T) {
	baseDir := t.TempDir()

	node1, err := NewSQLiteStore(
		filepath.Join(baseDir, "node1", "node.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Close()

	node2, err := NewSQLiteStore(
		filepath.Join(baseDir, "node2", "node.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Close()

	if err := node1.Put("fragment-A", []byte("node1-data"), false); err != nil {
		t.Fatal(err)
	}

	if err := node2.Put("fragment-B", []byte("node2-data"), true); err != nil {
		t.Fatal(err)
	}

	_, err = node1.Get("fragment-B")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("node1 should not contain node2's fragment: %v", err)
	}

	_, err = node2.Get("fragment-A")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("node2 should not contain node1's fragment: %v", err)
	}
}

// TestConcurrentWrites verifies that multiple goroutines can
// safely save fragments using the same SQLiteStore.
func TestConcurrentWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const count = 50

	var wg sync.WaitGroup
	errCh := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("fragment-%d", i)
			value := []byte(fmt.Sprintf("data-%d", i))

			if err := store.Put(key, value, i%2 == 0); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent write failed: %v", err)
	}

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("fragment-%d", i)
		expected := fmt.Sprintf("data-%d", i)

		value, err := store.Get(key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}

		if string(value) != expected {
			t.Fatalf("key %s: got %q, want %q",
				key, string(value), expected)
		}
	}
}
