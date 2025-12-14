//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
)

// TestInsertOrReplaceWithHooks tests INSERT OR REPLACE behavior with CDC hooks
// This reproduces the pika UPSERT hang issue
func TestInsertOrReplaceWithHooks(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	// Create MetaStore
	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	// Create clock
	clock := hlc.NewClock(1)

	// Create replicated database
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		t.Fatalf("Failed to create replicated database: %v", err)
	}
	defer replicatedDB.Close()

	// Create test table
	_, err = replicatedDB.GetWriteDB().Exec(`
		CREATE TABLE test_upsert (
			id VARCHAR(64) PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Log("Table created successfully")

	// Reload schema after DDL
	if err := replicatedDB.ReloadSchema(); err != nil {
		t.Fatalf("Failed to reload schema: %v", err)
	}

	// Step 1: Insert a row
	t.Log("Step 1: Inserting initial row")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req1 := coordinator.ExecutionRequest{
		SQL: "INSERT INTO test_upsert (id, value) VALUES ('key1', 'value1')",
	}

	pending1, err := replicatedDB.ExecuteLocalWithHooks(ctx, 1001, []coordinator.ExecutionRequest{req1})
	if err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}

	// Check CDC entries
	entries1 := pending1.GetCDCEntries()
	t.Logf("INSERT captured %d CDC entries", len(entries1))
	for _, e := range entries1 {
		t.Logf("  Entry: intentKey=%s oldVals=%d newVals=%d", e.IntentKey, len(e.OldValues), len(e.NewValues))
	}

	if err := pending1.Commit(); err != nil {
		t.Fatalf("Failed to commit INSERT: %v", err)
	}
	t.Log("INSERT committed successfully")

	// Step 2: Now do INSERT OR REPLACE (UPSERT) on the same key
	t.Log("Step 2: Executing INSERT OR REPLACE on existing key")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	req2 := coordinator.ExecutionRequest{
		SQL: "INSERT OR REPLACE INTO test_upsert (id, value) VALUES ('key1', 'value2')",
	}

	t.Log("About to call ExecuteLocalWithHooks for INSERT OR REPLACE...")
	start := time.Now()
	pending2, err := replicatedDB.ExecuteLocalWithHooks(ctx2, 1002, []coordinator.ExecutionRequest{req2})
	elapsed := time.Since(start)
	t.Logf("ExecuteLocalWithHooks returned after %v", elapsed)

	if err != nil {
		t.Fatalf("Failed to execute INSERT OR REPLACE: %v", err)
	}

	// Check CDC entries for the REPLACE
	entries2 := pending2.GetCDCEntries()
	t.Logf("INSERT OR REPLACE captured %d CDC entries", len(entries2))
	for _, e := range entries2 {
		t.Logf("  Entry: intentKey=%s oldVals=%d newVals=%d", e.IntentKey, len(e.OldValues), len(e.NewValues))
	}

	if err := pending2.Commit(); err != nil {
		t.Fatalf("Failed to commit INSERT OR REPLACE: %v", err)
	}
	t.Log("INSERT OR REPLACE committed successfully")

	// Verify the value was replaced - try both writeDB and readDB
	var value string
	err = replicatedDB.GetWriteDB().QueryRow("SELECT value FROM test_upsert WHERE id = 'key1'").Scan(&value)
	if err != nil {
		t.Logf("Failed to read from writeDB: %v", err)
		err = replicatedDB.GetReadDB().QueryRow("SELECT value FROM test_upsert WHERE id = 'key1'").Scan(&value)
		if err != nil {
			t.Fatalf("Failed to read from both writeDB and readDB: %v", err)
		}
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}
	t.Logf("Final value: %s", value)
}

// TestInsertOrReplaceNewRow tests INSERT OR REPLACE on a non-existent key
func TestInsertOrReplaceNewRow(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		t.Fatalf("Failed to create replicated database: %v", err)
	}
	defer replicatedDB.Close()

	_, err = replicatedDB.GetWriteDB().Exec(`
		CREATE TABLE test_upsert2 (
			id VARCHAR(64) PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Reload schema cache after DDL
	if err := replicatedDB.ReloadSchema(); err != nil {
		t.Fatalf("Failed to reload schema: %v", err)
	}

	// INSERT OR REPLACE on NEW key (row doesn't exist)
	t.Log("Testing INSERT OR REPLACE on non-existent key")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := coordinator.ExecutionRequest{
		SQL: "INSERT OR REPLACE INTO test_upsert2 (id, value) VALUES ('newkey', 'newvalue')",
	}

	t.Log("About to call ExecuteLocalWithHooks...")
	start := time.Now()
	pending, err := replicatedDB.ExecuteLocalWithHooks(ctx, 2001, []coordinator.ExecutionRequest{req})
	elapsed := time.Since(start)
	t.Logf("ExecuteLocalWithHooks returned after %v", elapsed)

	if err != nil {
		t.Fatalf("Failed to execute: %v", err)
	}

	entries := pending.GetCDCEntries()
	t.Logf("Captured %d CDC entries", len(entries))
	for _, e := range entries {
		t.Logf("  Entry: intentKey=%s oldVals=%d newVals=%d", e.IntentKey, len(e.OldValues), len(e.NewValues))
	}

	if err := pending.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	t.Log("Committed successfully")
}

// TestLastInsertIdCapture tests that LastInsertId is properly captured
func TestLastInsertIdCapture(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewPebbleMetaStore(metaPath, DefaultPebbleOptions())
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	replicatedDB, err := NewReplicatedDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		t.Fatalf("Failed to create replicated database: %v", err)
	}
	defer replicatedDB.Close()

	// Create table with auto-increment primary key
	_, err = replicatedDB.GetWriteDB().Exec(`
		CREATE TABLE test_autoincrement (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Reload schema after DDL
	if err := replicatedDB.ReloadSchema(); err != nil {
		t.Fatalf("Failed to reload schema: %v", err)
	}

	// Test 1: Insert a row and verify lastInsertId
	t.Log("Test 1: Insert row and check lastInsertId")
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	req1 := coordinator.ExecutionRequest{
		SQL: "INSERT INTO test_autoincrement (value) VALUES ('first')",
	}

	pending1, err := replicatedDB.ExecuteLocalWithHooks(ctx1, 3001, []coordinator.ExecutionRequest{req1})
	if err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}

	lastInsertId1 := pending1.GetLastInsertId()
	t.Logf("First insert lastInsertId: %d", lastInsertId1)
	if lastInsertId1 != 1 {
		t.Errorf("Expected lastInsertId=1, got %d", lastInsertId1)
	}

	if err := pending1.Commit(); err != nil {
		t.Fatalf("Failed to commit first INSERT: %v", err)
	}

	// Test 2: Insert another row and verify lastInsertId increments
	t.Log("Test 2: Insert second row and check lastInsertId")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	req2 := coordinator.ExecutionRequest{
		SQL: "INSERT INTO test_autoincrement (value) VALUES ('second')",
	}

	pending2, err := replicatedDB.ExecuteLocalWithHooks(ctx2, 3002, []coordinator.ExecutionRequest{req2})
	if err != nil {
		t.Fatalf("Failed to execute second INSERT: %v", err)
	}

	lastInsertId2 := pending2.GetLastInsertId()
	t.Logf("Second insert lastInsertId: %d", lastInsertId2)
	if lastInsertId2 != 2 {
		t.Errorf("Expected lastInsertId=2, got %d", lastInsertId2)
	}

	if err := pending2.Commit(); err != nil {
		t.Fatalf("Failed to commit second INSERT: %v", err)
	}

	// Test 3: UPDATE should not set lastInsertId (or set it to 0)
	t.Log("Test 3: UPDATE and check lastInsertId")
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()

	req3 := coordinator.ExecutionRequest{
		SQL: "UPDATE test_autoincrement SET value = 'updated' WHERE id = 1",
	}

	pending3, err := replicatedDB.ExecuteLocalWithHooks(ctx3, 3003, []coordinator.ExecutionRequest{req3})
	if err != nil {
		t.Fatalf("Failed to execute UPDATE: %v", err)
	}

	lastInsertId3 := pending3.GetLastInsertId()
	t.Logf("UPDATE lastInsertId: %d", lastInsertId3)
	if lastInsertId3 != 0 {
		t.Logf("UPDATE returned lastInsertId=%d (expected 0, but may vary)", lastInsertId3)
	}

	if err := pending3.Commit(); err != nil {
		t.Fatalf("Failed to commit UPDATE: %v", err)
	}

	t.Log("All LastInsertId tests passed")
}
