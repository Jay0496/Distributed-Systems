package storage

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestSQLiteReopenPersistence verifies that fragments remain available after
// the database is closed and reopened.
func TestSQLiteReopenPersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("initialize SQLite: %v", err)
	}

	data := []byte("distributed database fragment")
	parity := []byte{0x00, 0xFF, 0x01, 0x80, 0x42}

	if err := store.PutFragment(Fragment{StripeID: "stripe1", ShardIndex: 0, Data: data}); err != nil {
		t.Fatalf("save data fragment: %v", err)
	}

	if err := store.PutFragment(Fragment{StripeID: "stripe1", ShardIndex: 3, IsParity: true, Data: parity}); err != nil {
		t.Fatalf("save parity fragment: %v", err)
	}

	var parityFlag int
	err = store.db.QueryRow(
		"SELECT is_parity FROM fragments WHERE stripe_id = ? AND shard_index = ?",
		"stripe1", 3,
	).Scan(&parityFlag)

	if err != nil || parityFlag != 1 {
		t.Fatalf("expected parity flag 1, got %d, err=%v", parityFlag, err)
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

	gotData, err := reopened.GetFragment("stripe1", 0)
	if err != nil {
		t.Fatalf("retrieve data after reopen: %v", err)
	}
	if !bytes.Equal(gotData.Data, data) {
		t.Fatalf("data mismatch: got %v, want %v", gotData.Data, data)
	}

	gotParity, err := reopened.GetFragment("stripe1", 3)
	if err != nil {
		t.Fatalf("retrieve parity after reopen: %v", err)
	}
	if !bytes.Equal(gotParity.Data, parity) || !gotParity.IsParity {
		t.Fatalf("parity mismatch: got %+v", gotParity)
	}
}

// TestCheckpointTruncatesWALAndStatsFragmentBytes verifies the benchmark
// measurement helpers: the checkpoint empties the write-ahead log and Stats
// reports payload bytes separately from total file size.
func TestCheckpointTruncatesWALAndStatsFragmentBytes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	const shardSize = 1 << 20
	payload := bytes.Repeat([]byte{0x5a}, shardSize)
	for shard := 0; shard < 4; shard++ {
		if err := store.PutFragment(Fragment{
			StripeID:   "checkpoint-stripe",
			ShardIndex: shard,
			Data:       payload,
		}); err != nil {
			t.Fatalf("PutFragment(%d): %v", shard, err)
		}
	}

	before, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats before checkpoint: %v", err)
	}
	if before.FragmentBytes != shardSize*4 {
		t.Fatalf("fragment bytes = %d, want %d", before.FragmentBytes, shardSize*4)
	}

	if err := store.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	walSize, err := fileSize(dbPath + "-wal")
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	if walSize != 0 {
		t.Fatalf("WAL size after checkpoint = %d, want 0", walSize)
	}

	after, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats after checkpoint: %v", err)
	}
	if after.FragmentBytes != before.FragmentBytes {
		t.Fatalf("fragment bytes changed across checkpoint: before=%d after=%d", before.FragmentBytes, after.FragmentBytes)
	}
	if after.Bytes < after.FragmentBytes {
		t.Fatalf("disk bytes %d < fragment bytes %d", after.Bytes, after.FragmentBytes)
	}
}

// TestLegacyStoreTableDroppedWhenEmpty verifies the guarded upgrade path: an
// existing database with an empty transitional table opens and the table is
// removed.
func TestLegacyStoreTableDroppedWhenEmpty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	createLegacyStoreTable(t, dbPath)

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	exists, err := tableExists(store.db, legacyStoreTable)
	if err != nil {
		t.Fatalf("tableExists: %v", err)
	}
	if exists {
		t.Fatal("empty legacy store table was not dropped")
	}
}

// TestLegacyStoreTableWithDataFailsStartup verifies that values are never
// dropped silently: a non-empty transitional table fails with an export
// instruction.
func TestLegacyStoreTableWithDataFailsStartup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "node.db")
	createLegacyStoreTable(t, dbPath)
	insertLegacyValue(t, dbPath, "legacy-key", []byte("legacy-value"))

	store, err := NewSQLiteStore(dbPath)
	if err == nil {
		_ = store.Close()
		t.Fatal("startup accepted a non-empty legacy store table")
	}
	if !strings.Contains(err.Error(), "export") {
		t.Fatalf("error = %v, want an export instruction", err)
	}
}

func createLegacyStoreTable(t *testing.T, dbPath string) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE store (
		key TEXT PRIMARY KEY,
		value BLOB NOT NULL,
		is_parity INTEGER NOT NULL DEFAULT 0,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
}

func insertLegacyValue(t *testing.T, dbPath, key string, value []byte) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO store (key, value) VALUES (?, ?)`, key, value); err != nil {
		t.Fatalf("insert legacy value: %v", err)
	}
}

// TestNodeIsolation verifies that different node database files do not
// automatically share stored fragments.
func TestNodeIsolation(t *testing.T) {
	baseDir := t.TempDir()

	node1, err := NewSQLiteStore(filepath.Join(baseDir, "node1", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer node1.Close()

	node2, err := NewSQLiteStore(filepath.Join(baseDir, "node2", "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer node2.Close()

	if err := node1.PutFragment(Fragment{StripeID: "stripe-A", ShardIndex: 0, Data: []byte("node1-data")}); err != nil {
		t.Fatal(err)
	}
	if err := node2.PutFragment(Fragment{StripeID: "stripe-B", ShardIndex: 0, Data: []byte("node2-data")}); err != nil {
		t.Fatal(err)
	}

	if _, err := node1.GetFragment("stripe-B", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("node1 should not contain node2's fragment: %v", err)
	}
	if _, err := node2.GetFragment("stripe-A", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("node2 should not contain node1's fragment: %v", err)
	}
}

// TestConcurrentWrites verifies that multiple goroutines can safely write
// fragments through the same SQLiteStore.
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
			if err := store.PutFragment(Fragment{StripeID: key, ShardIndex: 0, Data: value}); err != nil {
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

		frag, err := store.GetFragment(key, 0)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if string(frag.Data) != expected {
			t.Fatalf("key %s: got %q, want %q", key, frag.Data, expected)
		}
	}
}
