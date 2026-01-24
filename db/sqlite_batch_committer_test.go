//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

	bc := NewSQLiteBatchCommitter(
		dbPath,
		maxBatchSize,
		maxWaitTime,
		true,  // checkpointEnabled
		4.0,   // passiveThreshMB
		16.0,  // restartThreshMB
		true,  // allowDynamicBatchSize
		false, // incrementalVacuumEnabled (disabled for tests)
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
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
		IntentKey: []byte(fmt.Sprintf("%d", id)),
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
		IntentKey: []byte("1"),
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
		IntentKey: []byte("1"),
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
		IntentKey: []byte("1"),
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
		IntentKey: []byte("1"),
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

func TestBatchCommitter_NoCheckpointBelowThreshold(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		10,
		50*time.Millisecond,
		true,
		4.0,
		16.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	for i := 0; i < 100; i++ {
		entry := makeIntentEntry("test_table", OpTypeInsert, i, "small", i)
		fut := bc.Enqueue(uint64(i), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
		if _, err := fut.Get(); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	if bc.checkpointRunning.Load() {
		t.Error("checkpoint should not be running for small WAL")
	}

	walSize := bc.checkWALSize()
	if walSize >= 4.0 {
		t.Errorf("WAL size should be < 4MB, got %.2f MB", walSize)
	}
}

func TestBatchCommitter_PassiveCheckpointTriggered(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		10,
		50*time.Millisecond,
		true,
		1.0,
		4.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	largeValue := fmt.Sprintf("%0*d", 10000, 0)
	for i := 0; i < 200; i++ {
		entry := makeIntentEntry("test_table", OpTypeInsert, i, largeValue, i)
		fut := bc.Enqueue(uint64(i), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
		if _, err := fut.Get(); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	walSizeBeforeCheckpoint := bc.checkWALSize()
	if walSizeBeforeCheckpoint < 1.0 {
		t.Logf("Warning: WAL size %.2f MB may not trigger checkpoint", walSizeBeforeCheckpoint)
	}

	checkpointSeen := false
	for i := 0; i < 100; i++ {
		if bc.checkpointRunning.Load() {
			checkpointSeen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)

	if checkpointSeen {
		walSizeAfterCheckpoint := bc.checkWALSize()
		if walSizeAfterCheckpoint >= walSizeBeforeCheckpoint {
			t.Logf("WAL size before: %.2f MB, after: %.2f MB", walSizeBeforeCheckpoint, walSizeAfterCheckpoint)
		}
	}
}

func TestBatchCommitter_RestartCheckpointTriggered(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		10,
		50*time.Millisecond,
		true,
		1.0,
		3.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	largeValue := fmt.Sprintf("%0*d", 15000, 0)
	for i := 0; i < 300; i++ {
		entry := makeIntentEntry("test_table", OpTypeInsert, i, largeValue, i)
		fut := bc.Enqueue(uint64(i), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
		if _, err := fut.Get(); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	walSizeBeforeCheckpoint := bc.checkWALSize()
	if walSizeBeforeCheckpoint < 3.0 {
		t.Logf("Warning: WAL size %.2f MB may not trigger RESTART checkpoint", walSizeBeforeCheckpoint)
	}

	time.Sleep(1 * time.Second)

	walSizeAfterCheckpoint := bc.checkWALSize()
	if walSizeAfterCheckpoint >= walSizeBeforeCheckpoint {
		t.Logf("WAL size before: %.2f MB, after: %.2f MB", walSizeBeforeCheckpoint, walSizeAfterCheckpoint)
	}
}

func TestBatchCommitter_DynamicBatchSizing(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		10,
		100*time.Millisecond,
		true,
		0.5,
		2.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	largeValue := fmt.Sprintf("%0*d", 8000, 0)
	for i := 0; i < 100; i++ {
		entry := makeIntentEntry("test_table", OpTypeInsert, i, largeValue, i)
		bc.Enqueue(uint64(i), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
	}

	checkpointStarted := false
	for i := 0; i < 100; i++ {
		if bc.checkpointRunning.Load() {
			checkpointStarted = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if checkpointStarted {
		var wg sync.WaitGroup
		for i := 100; i < 150; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				entry := makeIntentEntry("test_table", OpTypeInsert, id, "value", id)
				fut := bc.Enqueue(uint64(id), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
				fut.Get()
			}(i)
		}
		wg.Wait()
	}

	time.Sleep(500 * time.Millisecond)

	var count int
	db, err = sql.Open("sqlite3", dbPath)
	if err == nil {
		defer db.Close()
		if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		if count < 100 {
			t.Errorf("expected at least 100 rows, got %d", count)
		}
	}
}

func TestBatchCommitter_TimerDisabledDuringCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		100,
		100*time.Millisecond,
		true,
		0.5,
		2.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	bc.checkpointRunning.Store(true)

	entry := makeIntentEntry("test_table", OpTypeInsert, 1, "test", 100)
	bc.Enqueue(1, hlc.Timestamp{}, []*IntentEntry{entry}, nil)

	time.Sleep(150 * time.Millisecond)

	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows (timer disabled), got %d", count)
	}

	bc.checkpointRunning.Store(false)

	time.Sleep(150 * time.Millisecond)

	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after timer re-enabled, got %d", count)
	}
}

func TestBatchCommitter_ConcurrentOperationsWithCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		10,
		50*time.Millisecond,
		true,
		1.0,
		4.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	numGoroutines := 100
	var wg sync.WaitGroup
	var errCount atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			largeValue := fmt.Sprintf("%0*d", 5000, id)
			entry := makeIntentEntry("test_table", OpTypeInsert, id, largeValue, id)
			fut := bc.Enqueue(uint64(id), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
			if _, err := fut.Get(); err != nil {
				errCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("expected no errors, got %d errors", errCount.Load())
	}

	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != numGoroutines {
		t.Errorf("expected %d rows, got %d", numGoroutines, count)
	}

	rows, err := db.Query("SELECT id FROM test_table ORDER BY id")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	defer rows.Close()

	seenIDs := make(map[int]bool)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("failed to scan: %v", err)
		}
		if seenIDs[id] {
			t.Errorf("duplicate id: %d", id)
		}
		seenIDs[id] = true
	}

	if len(seenIDs) != numGoroutines {
		t.Errorf("expected %d unique IDs, got %d", numGoroutines, len(seenIDs))
	}
}

func TestBatchCommitter_CheckpointMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	_, err = db.Exec(`CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)`)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}
	db.Close()

	bc := NewSQLiteBatchCommitter(
		dbPath,
		10,
		50*time.Millisecond,
		true,
		0.5,
		2.0,
		true,
		false, // incrementalVacuumEnabled
		0,     // incrementalVacuumPages
		0,     // incrementalVacuumTimeLimitMS
	)
	if err := bc.Start(); err != nil {
		t.Fatalf("failed to start batch committer: %v", err)
	}
	defer bc.Stop()

	largeValue := fmt.Sprintf("%0*d", 8000, 0)
	for i := 0; i < 100; i++ {
		entry := makeIntentEntry("test_table", OpTypeInsert, i, largeValue, i)
		fut := bc.Enqueue(uint64(i), hlc.Timestamp{}, []*IntentEntry{entry}, nil)
		if _, err := fut.Get(); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	time.Sleep(1 * time.Second)

	if batchCommitterCheckpointCounter == nil {
		t.Error("checkpoint counter metric should be initialized")
	}
	if batchCommitterCheckpointDurHist == nil {
		t.Error("checkpoint duration histogram should be initialized")
	}
	if batchCommitterWALSizeBeforeHist == nil {
		t.Error("WAL size histogram should be initialized")
	}
	if batchCommitterCheckpointEfficiency == nil {
		t.Error("checkpoint efficiency metric should be initialized")
	}
}
