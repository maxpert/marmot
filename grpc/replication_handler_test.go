package grpc

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
)

func setupTestReplicationHandler(t *testing.T, nodeID uint64) (*ReplicationHandler, *db.MVCCDatabase) {
	dbPath := "/tmp/test_replication_" + string(rune(nodeID)) + ".db"
	os.Remove(dbPath)
	t.Cleanup(func() { os.Remove(dbPath) })

	clock := hlc.NewClock(nodeID)
	mdb, err := db.NewMVCCDatabase(dbPath, nodeID, clock)
	if err != nil {
		t.Fatalf("Failed to create MVCC database: %v", err)
	}
	t.Cleanup(func() { mdb.Close() })

	// Create test table
	_, err = mdb.GetDB().Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			balance INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	handler := NewReplicationHandler(nodeID, mdb.GetDB(), mdb.GetTransactionManager(), clock)
	return handler, mdb
}

func TestReplicationHandler_PreparePhase(t *testing.T) {
	handler, _ := setupTestReplicationHandler(t, 1)

	clock := hlc.NewClock(100)
	ts := clock.Now()

	req := &TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 100,
		Statements: []*Statement{
			{
				Sql:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
				Type:      StatementType_INSERT,
				TableName: "users",
			},
		},
		Timestamp: &HLC{
			WallTime: ts.WallTime,
			Logical:  ts.Logical,
			NodeId:   100,
		},
		Phase: TransactionPhase_PREPARE,
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("Prepare phase failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Expected success, got error: %s", resp.ErrorMessage)
	}

	t.Logf("✓ Prepare phase successful: write intent created for txn %d", req.TxnId)

	// Verify write intent exists
	var count int
	err = handler.db.QueryRow("SELECT COUNT(*) FROM __marmot__write_intents WHERE txn_id = ?", req.TxnId).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query write intents: %v", err)
	}

	if count != 1 {
		t.Fatalf("Expected 1 write intent, got %d", count)
	}

	t.Log("✓ Write intent persisted in __marmot__write_intents")
}

func TestReplicationHandler_CommitPhase(t *testing.T) {
	handler, _ := setupTestReplicationHandler(t, 1)

	clock := hlc.NewClock(100)
	ts := clock.Now()

	// Phase 1: Prepare
	prepareReq := &TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 100,
		Statements: []*Statement{
			{
				Sql:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
				Type:      StatementType_INSERT,
				TableName: "users",
			},
		},
		Timestamp: &HLC{
			WallTime: ts.WallTime,
			Logical:  ts.Logical,
			NodeId:   100,
		},
		Phase: TransactionPhase_PREPARE,
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), prepareReq)
	if err != nil {
		t.Fatalf("Prepare phase failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Prepare failed: %s", resp.ErrorMessage)
	}

	t.Log("✓ Prepare phase completed")

	// Phase 2: Commit
	commitReq := &TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 100,
		Statements:   prepareReq.Statements,
		Timestamp:    prepareReq.Timestamp,
		Phase:        TransactionPhase_COMMIT,
	}

	resp, err = handler.HandleReplicateTransaction(context.Background(), commitReq)
	if err != nil {
		t.Fatalf("Commit phase failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Commit failed: %s", resp.ErrorMessage)
	}

	t.Logf("✓ Commit phase successful for txn %d", commitReq.TxnId)

	// Wait a bit for async finalization
	time.Sleep(50 * time.Millisecond)

	// Verify transaction is committed
	var status string
	err = handler.db.QueryRow("SELECT status FROM __marmot__txn_records WHERE txn_id = ?", commitReq.TxnId).Scan(&status)
	if err != nil {
		t.Fatalf("Failed to query txn status: %v", err)
	}

	if status != db.TxnStatusCommitted {
		t.Fatalf("Expected status %s, got %s", db.TxnStatusCommitted, status)
	}

	t.Logf("✓ Transaction status: %s", status)
}

func TestReplicationHandler_AbortPhase(t *testing.T) {
	handler, _ := setupTestReplicationHandler(t, 1)

	clock := hlc.NewClock(100)
	ts := clock.Now()

	// Phase 1: Prepare
	prepareReq := &TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 100,
		Statements: []*Statement{
			{
				Sql:       "INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)",
				Type:      StatementType_INSERT,
				TableName: "users",
			},
		},
		Timestamp: &HLC{
			WallTime: ts.WallTime,
			Logical:  ts.Logical,
			NodeId:   100,
		},
		Phase: TransactionPhase_PREPARE,
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), prepareReq)
	if err != nil {
		t.Fatalf("Prepare phase failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Prepare failed: %s", resp.ErrorMessage)
	}

	t.Log("✓ Prepare phase completed")

	// Abort instead of commit
	abortReq := &TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 100,
		Statements:   prepareReq.Statements,
		Timestamp:    prepareReq.Timestamp,
		Phase:        TransactionPhase_ABORT,
	}

	resp, err = handler.HandleReplicateTransaction(context.Background(), abortReq)
	if err != nil {
		t.Fatalf("Abort phase failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Abort failed: %s", resp.ErrorMessage)
	}

	t.Logf("✓ Abort phase successful for txn %d", abortReq.TxnId)

	// Wait a bit for async cleanup
	time.Sleep(50 * time.Millisecond)

	// Verify transaction is aborted
	var status string
	err = handler.db.QueryRow("SELECT status FROM __marmot__txn_records WHERE txn_id = ?", abortReq.TxnId).Scan(&status)
	if err != nil {
		t.Fatalf("Failed to query txn status: %v", err)
	}

	if status != db.TxnStatusAborted {
		t.Fatalf("Expected status %s, got %s", db.TxnStatusAborted, status)
	}

	t.Logf("✓ Transaction status: %s", status)

	// Verify write intents are cleaned up
	var count int
	err = handler.db.QueryRow("SELECT COUNT(*) FROM __marmot__write_intents WHERE txn_id = ?", abortReq.TxnId).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query write intents: %v", err)
	}

	if count != 0 {
		t.Fatalf("Expected 0 write intents after abort, got %d", count)
	}

	t.Log("✓ Write intents cleaned up")
}

func TestReplicationHandler_ConflictDetection(t *testing.T) {
	handler, _ := setupTestReplicationHandler(t, 1)

	clock := hlc.NewClock(100)
	ts := clock.Now()

	// Transaction 1: Prepare
	txn1Req := &TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 100,
		Statements: []*Statement{
			{
				Sql:       "UPDATE users SET balance = 100 WHERE id = 1",
				Type:      StatementType_UPDATE,
				TableName: "users",
			},
		},
		Timestamp: &HLC{
			WallTime: ts.WallTime,
			Logical:  ts.Logical,
			NodeId:   100,
		},
		Phase: TransactionPhase_PREPARE,
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), txn1Req)
	if err != nil {
		t.Fatalf("Txn1 prepare failed: %v", err)
	}

	if !resp.Success {
		t.Fatalf("Txn1 prepare failed: %s", resp.ErrorMessage)
	}

	t.Log("✓ Transaction 1 prepared successfully")

	// Transaction 2: Try to prepare conflicting write
	txn2Req := &TransactionRequest{
		TxnId:        12346,
		SourceNodeId: 100,
		Statements: []*Statement{
			{
				Sql:       "UPDATE users SET balance = 200 WHERE id = 1",
				Type:      StatementType_UPDATE,
				TableName: "users",
			},
		},
		Timestamp: &HLC{
			WallTime: ts.WallTime + 1,
			Logical:  ts.Logical,
			NodeId:   100,
		},
		Phase: TransactionPhase_PREPARE,
	}

	resp, err = handler.HandleReplicateTransaction(context.Background(), txn2Req)
	if err != nil {
		t.Fatalf("Txn2 prepare error: %v", err)
	}

	if resp.Success {
		t.Fatal("Expected conflict, but txn2 prepare succeeded")
	}

	if !resp.ConflictDetected {
		t.Fatal("ConflictDetected flag should be true")
	}

	t.Logf("✓ Write-write conflict detected: %s", resp.ConflictDetails)
}

func TestReplicationHandler_Read(t *testing.T) {
	handler, mdb := setupTestReplicationHandler(t, 1)

	// Insert test data
	_, err := mdb.GetDB().Exec("INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	clock := hlc.NewClock(100)
	ts := clock.Now()

	// Execute read
	readReq := &ReadRequest{
		Query:        "SELECT id, name, balance FROM users WHERE id = 1",
		SourceNodeId: 100,
		SnapshotTs: &HLC{
			WallTime: ts.WallTime,
			Logical:  ts.Logical,
			NodeId:   100,
		},
		TableName: "users",
	}

	resp, err := handler.HandleRead(context.Background(), readReq)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(resp.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(resp.Rows))
	}

	row := resp.Rows[0]
	if string(row.Columns["name"]) != "Alice" {
		t.Fatalf("Expected name=Alice, got %s", string(row.Columns["name"]))
	}

	t.Logf("✓ Read successful: name=%s, balance=%s", string(row.Columns["name"]), string(row.Columns["balance"]))
}
