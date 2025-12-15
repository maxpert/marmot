//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
)

func setupTestBatchCommitter(t *testing.T, maxBatchSize int, maxWaitTime time.Duration) (*SQLiteBatchCommitter, *sql.DB, func()) {
	// Use temp file since batch committer opens its own connection
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create database and table first
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	bc := NewSQLiteBatchCommitter(dbPath, maxBatchSize, maxWaitTime)
	if err := bc.Start(); err != nil {
		db.Close()
		t.Fatalf("failed to start batch committer: %v", err)
	}

	cleanup := func() {
		bc.Stop()
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return bc, db, cleanup
}

func makeIntentEntry(table string, op OpType, id int, name string, value int) *IntentEntry {
	entry := &IntentEntry{
		Table:     table,
		Operation: uint8(op),
		IntentKey: fmt.Sprintf("%d", id),
	}

	if op != OpTypeDelete {
		entry.NewValues = map[string][]byte{
			"id":    mustMarshal(id),
			"name":  mustMarshal(name),
			"value": mustMarshal(value),
		}
	}

	if op == OpTypeUpdate || op == OpTypeDelete {
		entry.OldValues = map[string][]byte{
			"id":    mustMarshal(id),
			"name":  mustMarshal(name),
			"value": mustMarshal(value - 1),
		}
	}

	return entry
}

func mustMarshal(v interface{}) []byte {
	b, err := encoding.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestBatchCommitter_SingleTransaction(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 1, 50*time.Millisecond)
	defer cleanup()

	entry := makeIntentEntry("test_table", OpTypeInsert, 1, "test", 100)
	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{entry}, nil)

	_, err := fut.Get()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestBatchCommitter_MultipleConcurrent(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			entry := makeIntentEntry("test_table", OpTypeInsert, id, fmt.Sprintf("test%d", id), id*100)
			fut := bc.Enqueue(uint64(id), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
			if _, err := fut.Get(); err != nil {
				t.Errorf("txn %d failed: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

func TestBatchCommitter_TimeoutFlush(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 100, 50*time.Millisecond)
	defer cleanup()

	entry := makeIntentEntry("test_table", OpTypeInsert, 1, "test", 100)
	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{entry}, nil)

	_, err := fut.Get()
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestBatchCommitter_BatchSizeFlush(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 3, 1*time.Second)
	defer cleanup()

	var futures []*struct {
		fut interface{ Get() (error, error) }
	}
	for i := 1; i <= 3; i++ {
		entry := makeIntentEntry("test_table", OpTypeInsert, i, fmt.Sprintf("test%d", i), i*100)
		fut := bc.Enqueue(uint64(i), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
		futures = append(futures, &struct {
			fut interface{ Get() (error, error) }
		}{fut})
	}

	for _, f := range futures {
		if _, err := f.fut.Get(); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestBatchCommitter_StopFlushes(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 100, 10*time.Second)
	defer cleanup()

	entry := makeIntentEntry("test_table", OpTypeInsert, 1, "test", 100)
	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{entry}, nil)

	bc.Stop()

	_, err := fut.Get()
	if err != nil {
		t.Fatalf("expected no error after stop, got: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after stop flush, got %d", count)
	}
}

func TestBatchCommitter_EmptyBatch(t *testing.T) {
	_, _, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	time.Sleep(100 * time.Millisecond)
}

func TestBatchCommitter_UpdateOperation(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	insertEntry := makeIntentEntry("test_table", OpTypeInsert, 1, "original", 100)
	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{insertEntry}, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	updateEntry := &IntentEntry{
		Table:     "test_table",
		Operation: uint8(OpTypeUpdate),
		IntentKey: "1",
		OldValues: map[string][]byte{
			"id":    mustMarshal(1),
			"name":  mustMarshal("original"),
			"value": mustMarshal(100),
		},
		NewValues: map[string][]byte{
			"id":    mustMarshal(1),
			"name":  mustMarshal("updated"),
			"value": mustMarshal(200),
		},
	}
	fut = bc.Enqueue(2, hlc.Timestamp{}, []*IntentEntry{updateEntry}, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	var name string
	var value int
	if err := db.QueryRow("SELECT name, value FROM test_table WHERE id = 1").Scan(&name, &value); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if name != "updated" || value != 200 {
		t.Errorf("expected updated/200, got %s/%d", name, value)
	}
}

func TestBatchCommitter_DeleteOperation(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	insertEntry := makeIntentEntry("test_table", OpTypeInsert, 1, "to_delete", 100)
	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{insertEntry}, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	deleteEntry := &IntentEntry{
		Table:     "test_table",
		Operation: uint8(OpTypeDelete),
		IntentKey: "1",
		OldValues: map[string][]byte{
			"id":    mustMarshal(1),
			"name":  mustMarshal("to_delete"),
			"value": mustMarshal(100),
		},
	}
	fut = bc.Enqueue(2, hlc.Timestamp{}, []*IntentEntry{deleteEntry}, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows after delete, got %d", count)
	}
}

func TestBatchCommitter_ReplaceOperation(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	insertEntry := makeIntentEntry("test_table", OpTypeInsert, 1, "original", 100)
	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{insertEntry}, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	replaceEntry := &IntentEntry{
		Table:     "test_table",
		Operation: uint8(OpTypeReplace),
		IntentKey: "1",
		NewValues: map[string][]byte{
			"id":    mustMarshal(1),
			"name":  mustMarshal("replaced"),
			"value": mustMarshal(999),
		},
	}
	fut = bc.Enqueue(2, hlc.Timestamp{}, []*IntentEntry{replaceEntry}, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("replace failed: %v", err)
	}

	var name string
	var value int
	if err := db.QueryRow("SELECT name, value FROM test_table WHERE id = 1").Scan(&name, &value); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if name != "replaced" || value != 999 {
		t.Errorf("expected replaced/999, got %s/%d", name, value)
	}
}

func TestBatchCommitter_MultipleEntriesInBatch(t *testing.T) {
	bc, db, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	entries := []*IntentEntry{
		makeIntentEntry("test_table", OpTypeInsert, 1, "first", 100),
		makeIntentEntry("test_table", OpTypeInsert, 2, "second", 200),
		makeIntentEntry("test_table", OpTypeInsert, 3, "third", 300),
	}

	fut := bc.Enqueue(1, hlc.Timestamp{}, entries, nil)
	if _, err := fut.Get(); err != nil {
		t.Fatalf("batch insert failed: %v", err)
	}

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestBatchCommitter_ErrorHandling(t *testing.T) {
	bc, _, cleanup := setupTestBatchCommitter(t, 10, 50*time.Millisecond)
	defer cleanup()

	badEntry := &IntentEntry{
		Table:     "nonexistent_table",
		Operation: uint8(OpTypeInsert),
		IntentKey: "1",
		NewValues: map[string][]byte{
			"id": mustMarshal(1),
		},
	}

	fut := bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{badEntry}, nil)
	_, err := fut.Get()
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}
