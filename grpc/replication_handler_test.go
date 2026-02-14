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
