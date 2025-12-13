package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

func setupTestLocalReplicator(t *testing.T) (*LocalReplicator, *DatabaseManager, func()) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dm, err := NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}

	replicator := NewLocalReplicator(1, dm, clock)

	cleanup := func() {
		dm.Close()
	}

	return replicator, dm, cleanup
}

func TestLocalReplicator_PrepareWithDDL(t *testing.T) {
	replicator, dm, cleanup := setupTestLocalReplicator(t)
	defer cleanup()

	// Create a test database first
	err := dm.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 1000, Logical: 1}

	req := &coordinator.ReplicationRequest{
		TxnID:    1,
		NodeID:   1,
		Database: "testdb",
		Phase:    coordinator.PhasePrep,
		StartTS:  startTS,
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				Database:  "testdb",
				TableName: "test_table",
				SQL:       "CREATE TABLE test_table (id INT PRIMARY KEY, value TEXT)",
			},
		},
	}

	resp, err := replicator.ReplicateTransaction(ctx, 1, req)
	if err != nil {
		t.Fatalf("PrepareWithDDL failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("PrepareWithDDL returned failure: %s", resp.Error)
	}

	// Verify the transaction exists in transaction manager
	testDB, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to get test database: %v", err)
	}

	txnMgr := testDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(1)
	if txn == nil {
		t.Fatal("Transaction should exist after prepare")
	}
}

func TestLocalReplicator_PrepareWithCDC(t *testing.T) {
	replicator, dm, cleanup := setupTestLocalReplicator(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	testDB, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to get test database: %v", err)
	}

	_, err = testDB.GetDB().Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 1000, Logical: 1}

	req := &coordinator.ReplicationRequest{
		TxnID:    2,
		NodeID:   1,
		Database: "testdb",
		Phase:    coordinator.PhasePrep,
		StartTS:  startTS,
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				Database:  "testdb",
				TableName: "users",
				IntentKey: "users:1",
				SQL:       "INSERT INTO users (id, name) VALUES (1, 'test')",
				NewValues: map[string][]byte{
					"id":   []byte("1"),
					"name": []byte("test"),
				},
			},
		},
	}

	resp, err := replicator.ReplicateTransaction(ctx, 1, req)
	if err != nil {
		t.Fatalf("PrepareWithCDC failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("PrepareWithCDC returned failure: %s", resp.Error)
	}

	// Verify transaction exists
	txnMgr := testDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(2)
	if txn == nil {
		t.Fatal("Transaction should exist after prepare")
	}

	// Verify intent entry was written via MetaStore
	metaStore := testDB.GetMetaStore()
	entries, err := metaStore.GetIntentEntries(2)
	if err != nil {
		t.Fatalf("Failed to get intent entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Expected 1 intent entry, got %d", len(entries))
	}
	if entries[0].Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", entries[0].Table)
	}
	if entries[0].IntentKey != "users:1" {
		t.Errorf("Expected intent key 'users:1', got '%s'", entries[0].IntentKey)
	}
}

func TestLocalReplicator_PrepareWithDatabaseOps(t *testing.T) {
	replicator, dm, cleanup := setupTestLocalReplicator(t)
	defer cleanup()

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 1000, Logical: 1}

	// Test CREATE DATABASE prepare
	req := &coordinator.ReplicationRequest{
		TxnID:    3,
		NodeID:   1,
		Database: "",
		Phase:    coordinator.PhasePrep,
		StartTS:  startTS,
		Statements: []protocol.Statement{
			{
				Type:     protocol.StatementCreateDatabase,
				Database: "newdb",
			},
		},
	}

	resp, err := replicator.ReplicateTransaction(ctx, 1, req)
	if err != nil {
		t.Fatalf("PrepareWithDatabaseOps failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("PrepareWithDatabaseOps returned failure: %s", resp.Error)
	}

	// Verify the transaction was created in system database
	systemDB, err := dm.GetDatabase(SystemDatabaseName)
	if err != nil {
		t.Fatalf("Failed to get system database: %v", err)
	}

	txnMgr := systemDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(3)
	if txn == nil {
		t.Fatal("Transaction should exist in system database")
	}
}

// Note: TestLocalReplicator_CommitDatabaseOps is covered by integration tests
// The full CREATE DATABASE 2PC flow requires the coordinator to manage
// transaction state which is tested in test/ddl_replication_test.go

func TestLocalReplicator_AbortCleanup(t *testing.T) {
	replicator, dm, cleanup := setupTestLocalReplicator(t)
	defer cleanup()

	// Create test database
	err := dm.CreateDatabase("abortdb")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	testDB, err := dm.GetDatabase("abortdb")
	if err != nil {
		t.Fatalf("Failed to get test database: %v", err)
	}

	_, err = testDB.GetDB().Exec("CREATE TABLE items (id INT PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 1000, Logical: 1}

	// First prepare
	prepReq := &coordinator.ReplicationRequest{
		TxnID:    5,
		NodeID:   1,
		Database: "abortdb",
		Phase:    coordinator.PhasePrep,
		StartTS:  startTS,
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				Database:  "abortdb",
				TableName: "items",
				IntentKey: "items:1",
				SQL:       "INSERT INTO items (id, val) VALUES (1, 'test')",
				NewValues: map[string][]byte{
					"id":  []byte("1"),
					"val": []byte("test"),
				},
			},
		},
	}

	resp, err := replicator.ReplicateTransaction(ctx, 1, prepReq)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Prepare returned failure: %s", resp.Error)
	}

	// Verify transaction exists
	txnMgr := testDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(5)
	if txn == nil {
		t.Fatal("Transaction should exist after prepare")
	}

	// Now abort
	abortReq := &coordinator.ReplicationRequest{
		TxnID:    5,
		NodeID:   1,
		Database: "abortdb",
		Phase:    coordinator.PhaseAbort,
		StartTS:  startTS,
	}

	resp, err = replicator.ReplicateTransaction(ctx, 1, abortReq)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Abort returned failure: %s", resp.Error)
	}

	// Verify transaction is cleaned up
	txn = txnMgr.GetTransaction(5)
	if txn != nil {
		t.Error("Transaction should be cleaned up after abort")
	}
}

func TestLocalReplicator_ConflictDetection(t *testing.T) {
	replicator, dm, cleanup := setupTestLocalReplicator(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("conflictdb")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	testDB, err := dm.GetDatabase("conflictdb")
	if err != nil {
		t.Fatalf("Failed to get test database: %v", err)
	}

	_, err = testDB.GetDB().Exec("CREATE TABLE data (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	ctx := context.Background()
	startTS1 := hlc.Timestamp{WallTime: 1000, Logical: 1}
	startTS2 := hlc.Timestamp{WallTime: 1001, Logical: 1}

	// First transaction prepares on row 1
	req1 := &coordinator.ReplicationRequest{
		TxnID:    10,
		NodeID:   1,
		Database: "conflictdb",
		Phase:    coordinator.PhasePrep,
		StartTS:  startTS1,
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementUpdate,
				Database:  "conflictdb",
				TableName: "data",
				IntentKey: "data:1",
				SQL:       "UPDATE data SET value = 'a' WHERE id = 1",
				OldValues: map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				NewValues: map[string][]byte{"id": []byte("1"), "value": []byte("a")},
			},
		},
	}

	resp1, err := replicator.ReplicateTransaction(ctx, 1, req1)
	if err != nil {
		t.Fatalf("First prepare failed: %v", err)
	}
	if !resp1.Success {
		t.Fatalf("First prepare returned failure: %s", resp1.Error)
	}

	// Second transaction tries to prepare on same row - should conflict
	req2 := &coordinator.ReplicationRequest{
		TxnID:    11,
		NodeID:   1,
		Database: "conflictdb",
		Phase:    coordinator.PhasePrep,
		StartTS:  startTS2,
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementUpdate,
				Database:  "conflictdb",
				TableName: "data",
				IntentKey: "data:1",
				SQL:       "UPDATE data SET value = 'b' WHERE id = 1",
				OldValues: map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				NewValues: map[string][]byte{"id": []byte("1"), "value": []byte("b")},
			},
		},
	}

	resp2, err := replicator.ReplicateTransaction(ctx, 1, req2)
	if err != nil {
		t.Fatalf("Second prepare failed with error: %v", err)
	}

	// Should fail with conflict
	if resp2.Success {
		t.Error("Second prepare should have failed due to conflict")
	}
	if !resp2.ConflictDetected {
		t.Error("ConflictDetected should be true")
	}
}
