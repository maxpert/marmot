package grpc

import (
	"context"
	"os"
	"testing"

	"github.com/maxpert/marmot/db"
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
