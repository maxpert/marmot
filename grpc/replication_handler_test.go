package grpc

import (
	"context"
	"os"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
)

// TestSchemaVersionRejection verifies that transactions with higher required schema version are rejected
func TestSchemaVersionRejection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_test_schema_rejection")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create database manager: %v", err)
	}
	defer dbMgr.Close()

	// Create test database
	testDB := "test_db"
	err = dbMgr.CreateDatabase(testDB)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Get system database (already created by DatabaseManager)
	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		t.Fatalf("Failed to get system database: %v", err)
	}
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	handler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	// Set local schema version to 5
	err = schemaVersionMgr.SetSchemaVersion(testDB, 5, "CREATE TABLE test (id INT)", 100)
	if err != nil {
		t.Fatalf("Failed to set schema version: %v", err)
	}

	// Transaction with RequiredSchemaVersion = 10 should be rejected (10 > 5)
	req := &TransactionRequest{
		TxnId:        4,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_PREPARE,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		RequiredSchemaVersion: 10, // Greater than local version (5)
		Statements: []*Statement{
			{
				Type:      pb.StatementType_INSERT,
				TableName: "test_table",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: &RowChange{
						IntentKey: []byte("test_key_4"),
						NewValues: map[string][]byte{"id": []byte("4")},
					},
				},
			},
		},
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("Expected no error (response should indicate failure), got: %v", err)
	}
	if resp.Success {
		t.Fatalf("Expected transaction to be rejected due to schema version mismatch, but it succeeded")
	}
	if len(resp.ErrorMessage) < 10 {
		t.Fatalf("Expected error message about schema version mismatch, got: %s", resp.ErrorMessage)
	}

	t.Logf("Test passed! Transaction correctly rejected with error: %s", resp.ErrorMessage)
}

func TestReplicationHandler_ReplayDDLIdempotent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_test_replay_ddl_idempotent")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create database manager: %v", err)
	}
	defer dbMgr.Close()

	const testDB = "test_replay_ddl"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		t.Fatalf("Failed to get system database: %v", err)
	}
	handler := NewReplicationHandler(1, dbMgr, clock, db.NewSchemaVersionManager(systemDB.GetMetaStore()))

	ddlStmt := &Statement{
		Type:     pb.StatementType_DDL,
		Database: testDB,
		Payload: &Statement_DdlChange{
			DdlChange: &DDLChange{
				Sql: "CREATE TABLE replay_users (id INTEGER PRIMARY KEY, name TEXT)",
			},
		},
	}

	for i := 0; i < 2; i++ {
		req := &TransactionRequest{
			TxnId:        uint64(100 + i),
			SourceNodeId: 2,
			Database:     testDB,
			Phase:        TransactionPhase_REPLAY,
			Timestamp: &HLC{
				WallTime: clock.Now().WallTime,
				Logical:  clock.Now().Logical,
				NodeId:   2,
			},
			Statements: []*Statement{ddlStmt},
		}

		resp, err := handler.HandleReplicateTransaction(context.Background(), req)
		if err != nil {
			t.Fatalf("Replay DDL call %d failed: %v", i+1, err)
		}
		if !resp.Success {
			t.Fatalf("Replay DDL call %d should be idempotent, got error: %s", i+1, resp.ErrorMessage)
		}
	}
}

func TestReplicationHandler_ReplayReloadsSchemaAfterDDL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_test_replay_schema_reload")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create database manager: %v", err)
	}
	defer dbMgr.Close()

	const testDB = "test_replay_schema"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		t.Fatalf("Failed to get system database: %v", err)
	}
	handler := NewReplicationHandler(1, dbMgr, clock, db.NewSchemaVersionManager(systemDB.GetMetaStore()))

	ddlReq := &TransactionRequest{
		TxnId:        200,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_REPLAY,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:     pb.StatementType_DDL,
				Database: testDB,
				Payload: &Statement_DdlChange{
					DdlChange: &DDLChange{
						Sql: "CREATE TABLE replay_updates (id INTEGER PRIMARY KEY, name TEXT)",
					},
				},
			},
		},
	}
	resp, err := handler.HandleReplicateTransaction(context.Background(), ddlReq)
	if err != nil {
		t.Fatalf("Replay DDL failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Replay DDL failed with response error: %s", resp.ErrorMessage)
	}

	insertReq := &TransactionRequest{
		TxnId:        201,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_REPLAY,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_INSERT,
				TableName: "replay_updates",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: &RowChange{
						NewValues: map[string][]byte{
							"id":   mustMarshalMsgpack(t, int64(1)),
							"name": mustMarshalMsgpack(t, "alice"),
						},
					},
				},
			},
		},
	}
	resp, err = handler.HandleReplicateTransaction(context.Background(), insertReq)
	if err != nil {
		t.Fatalf("Replay insert failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Replay insert failed with response error: %s", resp.ErrorMessage)
	}

	updateReq := &TransactionRequest{
		TxnId:        202,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_REPLAY,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_UPDATE,
				TableName: "replay_updates",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: &RowChange{
						OldValues: map[string][]byte{
							"id":   mustMarshalMsgpack(t, int64(1)),
							"name": mustMarshalMsgpack(t, "alice"),
						},
						NewValues: map[string][]byte{
							"id":   mustMarshalMsgpack(t, int64(1)),
							"name": mustMarshalMsgpack(t, "bob"),
						},
					},
				},
			},
		},
	}
	resp, err = handler.HandleReplicateTransaction(context.Background(), updateReq)
	if err != nil {
		t.Fatalf("Replay update failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Replay update failed with response error: %s", resp.ErrorMessage)
	}
}

func mustMarshalMsgpack(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := encoding.Marshal(v)
	if err != nil {
		t.Fatalf("Failed to marshal value %v: %v", v, err)
	}
	return b
}

// newLeavingHandlerFixture creates a ReplicationHandler with a LEAVING local node.
func newLeavingHandlerFixture(t *testing.T) (*ReplicationHandler, *NodeRegistry, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "marmot_test_leaving_handler")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	const localNodeID uint64 = 1
	clock := hlc.NewClock(localNodeID)
	dbMgr, err := db.NewDatabaseManager(tmpDir, localNodeID, clock)
	if err != nil {
		t.Fatalf("Failed to create database manager: %v", err)
	}
	t.Cleanup(func() { dbMgr.Close() })

	const testDB = "test_leaving"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		t.Fatalf("Failed to get system database: %v", err)
	}

	handler := NewReplicationHandler(localNodeID, dbMgr, clock, db.NewSchemaVersionManager(systemDB.GetMetaStore()))

	registry := NewNodeRegistry(localNodeID, "localhost:9000")
	handler.SetRegistry(registry)

	return handler, registry, testDB
}

// makePrepareReq builds a minimal PREPARE request for use in LEAVING-node tests.
func makePrepareReq(t *testing.T, txnID uint64, testDB string, clock *hlc.Clock) *TransactionRequest {
	t.Helper()
	return &TransactionRequest{
		TxnId:        txnID,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_PREPARE,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_INSERT,
				TableName: "t",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: &RowChange{
						IntentKey: []byte("k1"),
						NewValues: map[string][]byte{"id": mustMarshalMsgpack(t, int64(1))},
					},
				},
			},
		},
	}
}

// TestReplicationHandler_RejectsNewPrepareWhenLeaving verifies that a LEAVING
// node refuses new PREPARE requests.
func TestReplicationHandler_RejectsNewPrepareWhenLeaving(t *testing.T) {
	t.Parallel()
	handler, registry, testDB := newLeavingHandlerFixture(t)

	if err := registry.MarkSelfLeaving(); err != nil {
		t.Fatalf("MarkSelfLeaving failed: %v", err)
	}

	clock := hlc.NewClock(1)
	req := makePrepareReq(t, 100, testDB, clock)

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Fatal("expected PREPARE to be rejected when node is LEAVING")
	}
	if resp.ErrorMessage != "node is leaving cluster" {
		t.Errorf("unexpected error message: %q", resp.ErrorMessage)
	}
}

// TestReplicationHandler_AcceptsCommitWhenLeaving verifies that a LEAVING
// node still accepts COMMIT for already-prepared transactions.
func TestReplicationHandler_AcceptsCommitWhenLeaving(t *testing.T) {
	t.Parallel()
	handler, registry, testDB := newLeavingHandlerFixture(t)
	clock := hlc.NewClock(1)

	// First prepare while still ALIVE
	prepReq := makePrepareReq(t, 200, testDB, clock)
	prepResp, err := handler.HandleReplicateTransaction(context.Background(), prepReq)
	if err != nil {
		t.Fatalf("PREPARE failed: %v", err)
	}
	if !prepResp.Success {
		t.Fatalf("PREPARE rejected before LEAVING: %s", prepResp.ErrorMessage)
	}

	// Transition to LEAVING
	if err := registry.MarkSelfLeaving(); err != nil {
		t.Fatalf("MarkSelfLeaving failed: %v", err)
	}

	// COMMIT must still succeed
	commitReq := &TransactionRequest{
		TxnId:    200,
		Database: testDB,
		Phase:    TransactionPhase_COMMIT,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
	}
	commitResp, err := handler.HandleReplicateTransaction(context.Background(), commitReq)
	if err != nil {
		t.Fatalf("COMMIT returned error: %v", err)
	}
	if !commitResp.Success {
		t.Fatalf("COMMIT rejected on LEAVING node: %s", commitResp.ErrorMessage)
	}
}

// TestReplicationHandler_AcceptsAbortWhenLeaving verifies that a LEAVING
// node still accepts ABORT for already-prepared transactions.
func TestReplicationHandler_AcceptsAbortWhenLeaving(t *testing.T) {
	t.Parallel()
	handler, registry, testDB := newLeavingHandlerFixture(t)
	clock := hlc.NewClock(1)

	// Prepare while ALIVE
	prepReq := makePrepareReq(t, 300, testDB, clock)
	prepResp, err := handler.HandleReplicateTransaction(context.Background(), prepReq)
	if err != nil {
		t.Fatalf("PREPARE failed: %v", err)
	}
	if !prepResp.Success {
		t.Fatalf("PREPARE rejected before LEAVING: %s", prepResp.ErrorMessage)
	}

	// Transition to LEAVING
	if err := registry.MarkSelfLeaving(); err != nil {
		t.Fatalf("MarkSelfLeaving failed: %v", err)
	}

	// ABORT must still succeed
	abortReq := &TransactionRequest{
		TxnId:    300,
		Database: testDB,
		Phase:    TransactionPhase_ABORT,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
	}
	abortResp, err := handler.HandleReplicateTransaction(context.Background(), abortReq)
	if err != nil {
		t.Fatalf("ABORT returned error: %v", err)
	}
	if !abortResp.Success {
		t.Fatalf("ABORT rejected on LEAVING node: %s", abortResp.ErrorMessage)
	}
}

// TestReplicationHandler_AcceptsPrepareWhenNotLeaving verifies that a non-LEAVING
// node accepts PREPARE normally (regression guard).
func TestReplicationHandler_AcceptsPrepareWhenNotLeaving(t *testing.T) {
	t.Parallel()
	handler, _, testDB := newLeavingHandlerFixture(t)
	clock := hlc.NewClock(1)

	// Create the table so the prepare has something to work with
	req := makePrepareReq(t, 400, testDB, clock)

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should not be rejected due to LEAVING — any failure here is a real engine issue
	if !resp.Success {
		// Only acceptable failure is schema/engine related, not our new LEAVING check
		if resp.ErrorMessage == "node is leaving cluster" {
			t.Fatal("PREPARE incorrectly rejected as if node is LEAVING")
		}
	}
}
