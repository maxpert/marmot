package grpc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
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

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	if err != nil {
		dbMgr.Close()
		t.Fatalf("Failed to get system database: %v", err)
	}

	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	return tmpDir, dbMgr, schemaVersionMgr
}
