package grpc

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// setupTestEnvironment creates a test database manager and schema version manager
func setupTestEnvironment(t *testing.T, testName string) (string, *db.DatabaseManager, *db.SchemaVersionManager) {
	tmpDir := filepath.Join("/tmp/marmot", testName)
	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create database manager: %v", err)
	}

	if _, err := dbMgr.GetDatabase(db.SystemDatabaseName); err != nil {
		dbMgr.Close()
		t.Fatalf("Failed to get system database: %v", err)
	}

	schemaVersionMgr := db.NewSchemaVersionManager(dbMgr)

	return tmpDir, dbMgr, schemaVersionMgr
}

// bumpSchemaVersionForTest commits n trivial CREATE TABLE DDL transactions
// against database, through its real TransactionManager, so its
// __marmot_schema_version ends at exactly n. Schema versions are
// derived from real committed DDL now (SchemaVersionManager has no setter
// any more), so this stands in for the tests' former direct
// SchemaVersionManager.SetSchemaVersion(database, n, ...) calls.
func bumpSchemaVersionForTest(t *testing.T, dbMgr *db.DatabaseManager, database string, n int) {
	t.Helper()
	mdb, err := dbMgr.GetDatabase(database)
	if err != nil {
		t.Fatalf("bumpSchemaVersionForTest: database %s unavailable: %v", database, err)
	}
	txnMgr := mdb.GetTransactionManager()

	for i := 0; i < n; i++ {
		txn, err := txnMgr.BeginTransaction(1)
		if err != nil {
			t.Fatalf("bumpSchemaVersionForTest: begin: %v", err)
		}
		table := fmt.Sprintf("__test_schema_bump_%d_%d", time.Now().UnixNano(), i)
		stmt := protocol.Statement{
			Type:      protocol.StatementDDL,
			SQL:       fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY)", table),
			TableName: table,
			Database:  database,
		}
		snapshot, err := db.SerializeData(db.DDLSnapshot{
			Type:      int(stmt.Type),
			Timestamp: time.Now().UnixNano(),
			SQL:       stmt.SQL,
			TableName: stmt.TableName,
		})
		if err != nil {
			t.Fatalf("bumpSchemaVersionForTest: serialize DDL: %v", err)
		}
		if err := txnMgr.WriteIntent(txn, db.IntentTypeDDL, table, "ddl:"+table, stmt, snapshot); err != nil {
			t.Fatalf("bumpSchemaVersionForTest: write intent: %v", err)
		}
		if err := txnMgr.CommitTransaction(txn); err != nil {
			t.Fatalf("bumpSchemaVersionForTest: commit: %v", err)
		}
	}
}
