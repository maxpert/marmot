package db

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/maxpert/marmot/hlc"
)

func setupTestDatabaseManager(t *testing.T) (*DatabaseManager, string) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dm, err := NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}

	return dm, tmpDir
}

func TestNewDatabaseManager(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	// Verify directories were created
	dbDir := filepath.Join(tmpDir, "databases")
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		t.Error("Databases directory was not created")
	}

	// Verify system database exists
	systemDBPath := filepath.Join(tmpDir, SystemDatabaseName+".db")
	if _, err := os.Stat(systemDBPath); os.IsNotExist(err) {
		t.Error("System database was not created")
	}

	// Verify default database was created
	if !dm.DatabaseExists(DefaultDatabaseName) {
		t.Error("Default database was not created")
	}

	// Verify system database has registry table
	var count int
	err := dm.systemDB.GetDB().QueryRow("SELECT COUNT(*) FROM __marmot_databases").Scan(&count)
	if err != nil {
		t.Errorf("Failed to query database registry: %v", err)
	}

	if count < 1 {
		t.Error("Expected at least 1 database in registry (default database)")
	}
}

func TestCreateDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create a new database
	err := dm.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Verify database exists
	if !dm.DatabaseExists("testdb") {
		t.Error("Database was not created")
	}

	// Verify can get database
	db, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Errorf("Failed to get database: %v", err)
	}

	if db == nil {
		t.Error("GetDatabase returned nil")
	}

	// Verify database is in registry
	var name string
	err = dm.systemDB.GetDB().QueryRow(
		"SELECT name FROM __marmot_databases WHERE name = ?", "testdb",
	).Scan(&name)
	if err != nil {
		t.Errorf("Database not found in registry: %v", err)
	}

	if name != "testdb" {
		t.Errorf("Expected name 'testdb', got '%s'", name)
	}
}

func TestCreateDatabaseDuplicate(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create database
	err := dm.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Try to create again
	err = dm.CreateDatabase("testdb")
	if err == nil {
		t.Error("Expected error when creating duplicate database")
	}
}

func TestCreateSystemDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Try to create system database
	err := dm.CreateDatabase(SystemDatabaseName)
	if err == nil {
		t.Error("Expected error when creating system database")
	}
}

func TestDropDatabase(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create database
	err := dm.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "databases", "testdb.db")

	// Verify file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}

	// Drop database
	err = dm.DropDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// Verify database no longer exists
	if dm.DatabaseExists("testdb") {
		t.Error("Database still exists after drop")
	}

	// Verify file was deleted
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Error("Database file was not deleted")
	}

	// Verify not in registry
	var count int
	err = dm.systemDB.GetDB().QueryRow(
		"SELECT COUNT(*) FROM __marmot_databases WHERE name = ?", "testdb",
	).Scan(&count)
	if err != nil {
		t.Errorf("Failed to query registry: %v", err)
	}

	if count != 0 {
		t.Error("Database still in registry after drop")
	}
}

func TestDropNonExistentDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	err := dm.DropDatabase("nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent database")
	}
}

func TestDropSystemDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	err := dm.DropDatabase(SystemDatabaseName)
	if err == nil {
		t.Error("Expected error when dropping system database")
	}
}

func TestDropDefaultDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	err := dm.DropDatabase(DefaultDatabaseName)
	if err == nil {
		t.Error("Expected error when dropping default database")
	}
}

func TestGetDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Get default database
	db, err := dm.GetDatabase(DefaultDatabaseName)
	if err != nil {
		t.Errorf("Failed to get default database: %v", err)
	}

	if db == nil {
		t.Error("GetDatabase returned nil")
	}

	// Try to get non-existent database
	_, err = dm.GetDatabase("nonexistent")
	if err == nil {
		t.Error("Expected error when getting non-existent database")
	}
}

func TestListDatabases(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Initially should have default database
	dbs := dm.ListDatabases()
	if len(dbs) != 1 {
		t.Errorf("Expected 1 database, got %d", len(dbs))
	}

	// Create more databases
	dm.CreateDatabase("db1")
	dm.CreateDatabase("db2")
	dm.CreateDatabase("db3")

	dbs = dm.ListDatabases()
	if len(dbs) != 4 {
		t.Errorf("Expected 4 databases, got %d", len(dbs))
	}

	// Verify names
	dbMap := make(map[string]bool)
	for _, name := range dbs {
		dbMap[name] = true
	}

	expectedDBs := []string{DefaultDatabaseName, "db1", "db2", "db3"}
	for _, name := range expectedDBs {
		if !dbMap[name] {
			t.Errorf("Expected database '%s' in list", name)
		}
	}
}

func TestDatabasePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	// Create database manager and databases
	dm1, err := NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}

	dm1.CreateDatabase("persistent1")
	dm1.CreateDatabase("persistent2")

	dbs1 := dm1.ListDatabases()
	dm1.Close()

	// Create new database manager with same directory
	dm2, err := NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create second DatabaseManager: %v", err)
	}
	defer dm2.Close()

	dbs2 := dm2.ListDatabases()

	// Should have same databases
	if len(dbs1) != len(dbs2) {
		t.Errorf("Expected %d databases after reload, got %d", len(dbs1), len(dbs2))
	}

	// Verify each database
	for _, name := range []string{DefaultDatabaseName, "persistent1", "persistent2"} {
		if !dm2.DatabaseExists(name) {
			t.Errorf("Database '%s' not found after reload", name)
		}
	}
}

func TestConcurrentDatabaseOperations(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	const numWorkers = 20
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers)

	// Concurrent creates
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			dbName := "concurrent_db_" + string(rune('a'+id))
			if err := dm.CreateDatabase(dbName); err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent create error: %v", err)
	}

	// Verify all databases were created
	dbs := dm.ListDatabases()
	expectedCount := 1 + numWorkers // default + created
	if len(dbs) != expectedCount {
		t.Errorf("Expected %d databases, got %d", expectedCount, len(dbs))
	}
}

func TestDatabaseIsolation(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create two databases
	dm.CreateDatabase("db1")
	dm.CreateDatabase("db2")

	db1, _ := dm.GetDatabase("db1")
	db2, _ := dm.GetDatabase("db2")

	// Create table in db1
	_, err := db1.GetDB().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table in db1: %v", err)
	}

	// Insert data in db1
	_, err = db1.GetDB().Exec("INSERT INTO test (id, value) VALUES (1, 'db1_value')")
	if err != nil {
		t.Fatalf("Failed to insert in db1: %v", err)
	}

	// Verify data in db1
	var value string
	err = db1.GetDB().QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to read from db1: %v", err)
	}
	if value != "db1_value" {
		t.Errorf("Expected 'db1_value', got '%s'", value)
	}

	// Verify db2 doesn't have the table
	err = db2.GetDB().QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	if err == nil {
		t.Error("Expected error querying non-existent table in db2")
	}
}

func TestMigrateFromLegacy(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	// Create legacy database
	legacyPath := filepath.Join(tmpDir, "old_marmot.db")
	legacyDB, err := NewMVCCDatabase(legacyPath, 1, clock, nil)
	if err != nil {
		t.Fatalf("Failed to create legacy database: %v", err)
	}

	// Insert some data
	_, err = legacyDB.GetDB().Exec("CREATE TABLE legacy_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table in legacy database: %v", err)
	}

	_, err = legacyDB.GetDB().Exec("INSERT INTO legacy_test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert data in legacy database: %v", err)
	}

	legacyDB.Close()

	// Create new data directory
	newDataDir := filepath.Join(tmpDir, "new_data")

	// Migrate
	err = MigrateFromLegacy(legacyPath, newDataDir, 1, clock)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify legacy file was moved
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Error("Legacy database file still exists after migration")
	}

	// Create DatabaseManager with new directory
	dm, err := NewDatabaseManager(newDataDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager after migration: %v", err)
	}
	defer dm.Close()

	// Verify default database has migrated data
	db, err := dm.GetDatabase(DefaultDatabaseName)
	if err != nil {
		t.Fatalf("Failed to get default database: %v", err)
	}

	var count int
	err = db.GetDB().QueryRow("SELECT COUNT(*) FROM legacy_test").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query migrated data: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 row in migrated table, got %d", count)
	}
}

// TestTakeSnapshotIncludesMetaStores verifies that TakeSnapshot includes all meta databases
func TestTakeSnapshotIncludesMetaStores(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create additional databases
	if err := dm.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Take snapshot
	snapshots, _, err := dm.TakeSnapshot()
	if err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Build a map of snapshot names for easy lookup
	snapshotNames := make(map[string]bool)
	for _, snap := range snapshots {
		snapshotNames[snap.Name] = true
		t.Logf("Snapshot: %s (path: %s)", snap.Name, snap.Filename)
	}

	// Helper to check if any snapshot name has given prefix
	hasMetaPrefix := func(prefix string) bool {
		for name := range snapshotNames {
			if strings.HasPrefix(name, prefix+"_meta/") {
				return true
			}
		}
		return false
	}

	// Verify system database is included
	if !snapshotNames[SystemDatabaseName] {
		t.Error("System database not found in snapshots")
	}

	// Verify system meta database is included (BadgerDB directory files)
	if !hasMetaPrefix(SystemDatabaseName) {
		t.Error("System meta database not found in snapshots")
	}

	// Verify default database is included
	if !snapshotNames[DefaultDatabaseName] {
		t.Error("Default database not found in snapshots")
	}

	// Verify default meta database is included (BadgerDB directory files)
	if !hasMetaPrefix(DefaultDatabaseName) {
		t.Error("Default meta database not found in snapshots")
	}

	// Verify testdb is included
	if !snapshotNames["testdb"] {
		t.Error("testdb not found in snapshots")
	}

	// Verify testdb meta database is included (BadgerDB directory files)
	if !hasMetaPrefix("testdb") {
		t.Error("testdb meta database not found in snapshots")
	}

	t.Logf("✓ TakeSnapshot includes all databases and their meta stores")
}

// TestGetReplicationStateNilHandling verifies that GetReplicationState handles nil correctly
func TestGetReplicationStateNilHandling(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Get replication state for a non-existent peer
	// This should return nil, nil (not found) rather than panic
	state, err := dm.GetReplicationState(999, DefaultDatabaseName)
	if err != nil {
		t.Fatalf("GetReplicationState returned unexpected error: %v", err)
	}

	// State should be nil when no replication state exists
	if state != nil {
		t.Errorf("Expected nil state for non-existent peer, got %+v", state)
	}

	t.Log("✓ GetReplicationState handles nil correctly for non-existent peers")
}

// TestGetReplicationStateAfterUpdate verifies that replication state can be set and retrieved
func TestGetReplicationStateAfterUpdate(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Update replication state for a peer
	state := &ReplicationState{
		PeerNodeID:        42,
		DatabaseName:      DefaultDatabaseName,
		LastAppliedTxnID:  100,
		LastAppliedTSWall: 1234567890,
		LastAppliedTSLog:  5,
		SyncStatus:        "SYNCED",
	}

	err := dm.UpdateReplicationState(state)
	if err != nil {
		t.Fatalf("UpdateReplicationState failed: %v", err)
	}

	// Get replication state for the peer
	retrieved, err := dm.GetReplicationState(42, DefaultDatabaseName)
	if err != nil {
		t.Fatalf("GetReplicationState returned error: %v", err)
	}

	if retrieved == nil {
		t.Fatal("GetReplicationState returned nil for existing peer")
	}

	// Verify values
	if retrieved.PeerNodeID != 42 {
		t.Errorf("Expected PeerNodeID 42, got %d", retrieved.PeerNodeID)
	}
	if retrieved.LastAppliedTxnID != 100 {
		t.Errorf("Expected LastAppliedTxnID 100, got %d", retrieved.LastAppliedTxnID)
	}
	if retrieved.SyncStatus != "SYNCED" {
		t.Errorf("Expected SyncStatus SYNCED, got %s", retrieved.SyncStatus)
	}

	t.Log("✓ GetReplicationState retrieves correct values after update")
}
