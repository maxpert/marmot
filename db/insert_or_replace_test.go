//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// TestInsertOrReplaceWithHooks tests INSERT OR REPLACE behavior with CDC hooks
// This reproduces the pika UPSERT hang issue
func TestInsertOrReplaceWithHooks(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	metaPath := filepath.Join(tmpDir, "meta")

	// Create MetaStore
	os.MkdirAll(metaPath, 0755)
	metaStore, err := NewBadgerMetaStore(metaPath, DefaultBadgerOptions())
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	// Create clock
	clock := hlc.NewClock(1)

	// Create MVCC database
	mvccDB, err := NewMVCCDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mvccDB.Close()

	// Create test table
	_, err = mvccDB.GetWriteDB().Exec(`
		CREATE TABLE test_upsert (
			id VARCHAR(64) PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Log("Table created successfully")

	// Step 1: Insert a row
	t.Log("Step 1: Inserting initial row")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stmt1 := protocol.Statement{
		SQL:       "INSERT INTO test_upsert (id, value) VALUES ('key1', 'value1')",
		Type:      protocol.StatementInsert,
		TableName: "test_upsert",
	}

	pending1, err := mvccDB.ExecuteLocalWithHooks(ctx, 1001, []protocol.Statement{stmt1})
	if err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}

	// Check CDC entries
	entries1 := pending1.GetCDCEntries()
	t.Logf("INSERT captured %d CDC entries", len(entries1))
	for _, e := range entries1 {
		t.Logf("  Entry: rowKey=%s oldVals=%d newVals=%d", e.RowKey, len(e.OldValues), len(e.NewValues))
	}

	if err := pending1.Commit(); err != nil {
		t.Fatalf("Failed to commit INSERT: %v", err)
	}
	t.Log("INSERT committed successfully")

	// Step 2: Now do INSERT OR REPLACE (UPSERT) on the same key
	t.Log("Step 2: Executing INSERT OR REPLACE on existing key")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	stmt2 := protocol.Statement{
		SQL:       "INSERT OR REPLACE INTO test_upsert (id, value) VALUES ('key1', 'value2')",
		Type:      protocol.StatementReplace,
		TableName: "test_upsert",
	}

	t.Log("About to call ExecuteLocalWithHooks for INSERT OR REPLACE...")
	start := time.Now()
	pending2, err := mvccDB.ExecuteLocalWithHooks(ctx2, 1002, []protocol.Statement{stmt2})
	elapsed := time.Since(start)
	t.Logf("ExecuteLocalWithHooks returned after %v", elapsed)

	if err != nil {
		t.Fatalf("Failed to execute INSERT OR REPLACE: %v", err)
	}

	// Check CDC entries for the REPLACE
	entries2 := pending2.GetCDCEntries()
	t.Logf("INSERT OR REPLACE captured %d CDC entries", len(entries2))
	for _, e := range entries2 {
		t.Logf("  Entry: rowKey=%s oldVals=%d newVals=%d", e.RowKey, len(e.OldValues), len(e.NewValues))
	}

	if err := pending2.Commit(); err != nil {
		t.Fatalf("Failed to commit INSERT OR REPLACE: %v", err)
	}
	t.Log("INSERT OR REPLACE committed successfully")

	// Verify the value was replaced
	var value string
	err = mvccDB.GetReadDB().QueryRow("SELECT value FROM test_upsert WHERE id = 'key1'").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to read value: %v", err)
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
	metaStore, err := NewBadgerMetaStore(metaPath, DefaultBadgerOptions())
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	clock := hlc.NewClock(1)
	mvccDB, err := NewMVCCDatabase(dbPath, 1, clock, metaStore)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	defer mvccDB.Close()

	_, err = mvccDB.GetWriteDB().Exec(`
		CREATE TABLE test_upsert2 (
			id VARCHAR(64) PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// INSERT OR REPLACE on NEW key (row doesn't exist)
	t.Log("Testing INSERT OR REPLACE on non-existent key")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stmt := protocol.Statement{
		SQL:       "INSERT OR REPLACE INTO test_upsert2 (id, value) VALUES ('newkey', 'newvalue')",
		Type:      protocol.StatementReplace,
		TableName: "test_upsert2",
	}

	t.Log("About to call ExecuteLocalWithHooks...")
	start := time.Now()
	pending, err := mvccDB.ExecuteLocalWithHooks(ctx, 2001, []protocol.Statement{stmt})
	elapsed := time.Since(start)
	t.Logf("ExecuteLocalWithHooks returned after %v", elapsed)

	if err != nil {
		t.Fatalf("Failed to execute: %v", err)
	}

	entries := pending.GetCDCEntries()
	t.Logf("Captured %d CDC entries", len(entries))
	for _, e := range entries {
		t.Logf("  Entry: rowKey=%s oldVals=%d newVals=%d", e.RowKey, len(e.OldValues), len(e.NewValues))
	}

	if err := pending.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	t.Log("Committed successfully")
}
