package db

import (
	"database/sql"
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

func TestCreateDatabaseIdempotent(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create database
	err := dm.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Try to create again - should succeed (idempotent)
	err = dm.CreateDatabase("testdb")
	if err != nil {
		t.Errorf("Expected success for idempotent create, got error: %v", err)
	}

	// Verify only one database exists
	databases := dm.ListDatabases()
	count := 0
	for _, name := range databases {
		if name == "testdb" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected exactly 1 testdb, found %d", count)
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

	// The row stays as a tombstone (generation, dropped) rather than being
	// deleted - see DatabaseRegistryKey - so a later CREATE can fence a stale
	// peer with a strictly higher generation.
	var count int
	var dropped bool
	err = dm.systemDB.GetDB().QueryRow(
		"SELECT COUNT(*), dropped FROM __marmot_databases WHERE name = ? GROUP BY dropped", "testdb",
	).Scan(&count, &dropped)
	if err != nil {
		t.Fatalf("Failed to query registry: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected exactly 1 registry row for 'testdb' after drop, got %d", count)
	}
	if !dropped {
		t.Error("Expected 'testdb' row to be tombstoned (dropped=1) after drop")
	}

	// ListDatabases must exclude the tombstone.
	for _, name := range dm.ListDatabases() {
		if name == "testdb" {
			t.Error("ListDatabases still lists a tombstoned database")
		}
	}
}

func TestDropNonExistentDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	err := dm.DropDatabase("nonexistent")
	if err != nil {
		t.Errorf("Expected nil when dropping non-existent database, got: %v", err)
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
	_ = dm.CreateDatabase("db1")
	_ = dm.CreateDatabase("db2")
	_ = dm.CreateDatabase("db3")

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

	_ = dm1.CreateDatabase("persistent1")
	_ = dm1.CreateDatabase("persistent2")

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
	_ = dm.CreateDatabase("db1")
	_ = dm.CreateDatabase("db2")

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

// TestTakeSnapshotExcludesMetaStores verifies that TakeSnapshot includes only SQLite .db files
// MetaStore (PebbleDB) is NOT included to avoid race conditions from WAL rotation/compaction.
func TestTakeSnapshotExcludesMetaStores(t *testing.T) {
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

	// Helper to check if any snapshot name has given prefix (for meta stores)
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

	// Verify system meta database is NOT included (PebbleDB causes race conditions)
	if hasMetaPrefix(SystemDatabaseName) {
		t.Error("System meta database should NOT be in snapshots")
	}

	// Verify default database is included
	if !snapshotNames[DefaultDatabaseName] {
		t.Error("Default database not found in snapshots")
	}

	// Verify default meta database is NOT included
	if hasMetaPrefix(DefaultDatabaseName) {
		t.Error("Default meta database should NOT be in snapshots")
	}

	// Verify testdb is included
	if !snapshotNames["testdb"] {
		t.Error("testdb not found in snapshots")
	}

	// Verify testdb meta database is NOT included
	if hasMetaPrefix("testdb") {
		t.Error("testdb meta database should NOT be in snapshots")
	}

	// Verify only .db files are in snapshot (3 databases)
	if len(snapshots) != 3 {
		t.Errorf("Expected 3 snapshots (system, marmot, testdb), got %d", len(snapshots))
	}

	t.Logf("✓ TakeSnapshot includes only SQLite .db files, excludes MetaStore")
}

// TestTakeSnapshotForDatabase tests snapshotting a single database
func TestTakeSnapshotForDatabase(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create a test database
	if err := dm.CreateDatabase("testdb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Insert some test data
	db, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}

	_, err = db.GetDB().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.GetDB().Exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create snapshot directory
	snapshotDir := filepath.Join(tmpDir, "snapshot")

	// Take snapshot for testdb only
	snapshot, maxTxnID, err := dm.TakeSnapshotForDatabase(snapshotDir, "testdb")
	if err != nil {
		t.Fatalf("TakeSnapshotForDatabase failed: %v", err)
	}

	// Verify snapshot metadata
	if snapshot.Name != "testdb" {
		t.Errorf("Expected snapshot name 'testdb', got '%s'", snapshot.Name)
	}

	expectedFilename := filepath.Join("databases", "testdb.db")
	if snapshot.Filename != expectedFilename {
		t.Errorf("Expected filename '%s', got '%s'", expectedFilename, snapshot.Filename)
	}

	// Verify file was created
	if _, err := os.Stat(snapshot.FullPath); os.IsNotExist(err) {
		t.Errorf("Snapshot file does not exist at %s", snapshot.FullPath)
	}

	// Verify file size is non-zero
	if snapshot.Size == 0 {
		t.Error("Snapshot file size is 0")
	}

	// Verify SHA256 checksum is not empty
	if snapshot.SHA256 == "" {
		t.Error("Snapshot SHA256 checksum is empty")
	}

	// Verify max txn ID is reasonable (should be > 0 after inserts)
	t.Logf("Max TxnID: %d", maxTxnID)

	// Verify only testdb was snapshotted (other databases should not exist)
	marmotPath := filepath.Join(snapshotDir, "databases", "marmot.db")
	if _, err := os.Stat(marmotPath); !os.IsNotExist(err) {
		t.Error("marmot database should not be in per-database snapshot")
	}

	t.Logf("✓ TakeSnapshotForDatabase successfully created snapshot for single database")
}

// TestTakeSnapshotForDatabaseNotFound tests error handling for non-existent database
func TestTakeSnapshotForDatabaseNotFound(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	snapshotDir := filepath.Join(tmpDir, "snapshot")

	// Try to snapshot non-existent database
	_, _, err := dm.TakeSnapshotForDatabase(snapshotDir, "nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent database, got nil")
	}

	expectedErr := "database nonexistent does not exist"
	if !strings.Contains(err.Error(), expectedErr) {
		t.Errorf("Expected error containing '%s', got '%s'", expectedErr, err.Error())
	}

	t.Logf("✓ TakeSnapshotForDatabase correctly handles non-existent database")
}

// TestTakeSnapshotForDatabaseConcurrent tests concurrent per-database snapshots
func TestTakeSnapshotForDatabaseConcurrent(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create multiple test databases
	dbNames := []string{"db1", "db2", "db3"}
	for _, name := range dbNames {
		if err := dm.CreateDatabase(name); err != nil {
			t.Fatalf("Failed to create database %s: %v", name, err)
		}

		// Add some data to each database
		db, err := dm.GetDatabase(name)
		if err != nil {
			t.Fatalf("Failed to get database %s: %v", name, err)
		}

		_, err = db.GetDB().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
		if err != nil {
			t.Fatalf("Failed to create table in %s: %v", name, err)
		}

		_, err = db.GetDB().Exec("INSERT INTO test (data) VALUES ('data')")
		if err != nil {
			t.Fatalf("Failed to insert data in %s: %v", name, err)
		}
	}

	// Snapshot all databases concurrently
	var wg sync.WaitGroup
	errors := make(chan error, len(dbNames))

	for _, name := range dbNames {
		wg.Add(1)
		go func(dbName string) {
			defer wg.Done()

			snapshotDir := filepath.Join(tmpDir, "snapshot_"+dbName)
			snapshot, _, err := dm.TakeSnapshotForDatabase(snapshotDir, dbName)
			if err != nil {
				errors <- err
				return
			}

			// Verify snapshot was created
			if _, err := os.Stat(snapshot.FullPath); os.IsNotExist(err) {
				errors <- err
			}
		}(name)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Errorf("Concurrent snapshot failed: %v", err)
	}

	t.Logf("✓ TakeSnapshotForDatabase allows concurrent snapshots of different databases")
}

// TestTakeSnapshotForDatabaseMaxTxnID verifies max_txn_id is correctly returned
func TestTakeSnapshotForDatabaseMaxTxnID(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	// Create a test database
	if err := dm.CreateDatabase("txntest"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Get the database
	db, err := dm.GetDatabase("txntest")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}

	// Create a table and insert data to generate transactions
	_, err = db.GetDB().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.GetDB().Exec("INSERT INTO test (value) VALUES ('test1'), ('test2')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Take snapshot
	snapshotDir := filepath.Join(tmpDir, "txn_snapshot")
	_, maxTxnID, err := dm.TakeSnapshotForDatabase(snapshotDir, "txntest")
	if err != nil {
		t.Fatalf("TakeSnapshotForDatabase failed: %v", err)
	}

	// Verify max_txn_id is returned (should be >= 0)
	t.Logf("Max TxnID for txntest: %d", maxTxnID)

	// Get max_txn_id directly and verify it matches
	directMaxTxnID, err := dm.GetMaxTxnID("txntest")
	if err != nil {
		t.Fatalf("GetMaxTxnID failed: %v", err)
	}

	if maxTxnID != directMaxTxnID {
		t.Errorf("Expected maxTxnID %d to match direct query %d", maxTxnID, directMaxTxnID)
	}

	t.Logf("✓ TakeSnapshotForDatabase returns correct max_txn_id")
}

// TestReplica_CreatesOwnSystemDatabase tests that replica creates its own __marmot_system.db
func TestReplica_CreatesOwnSystemDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dm, err := NewDatabaseManager(tmpDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create DatabaseManager: %v", err)
	}
	defer dm.Close()

	systemDBPath := filepath.Join(tmpDir, SystemDatabaseName+".db")
	if _, err := os.Stat(systemDBPath); os.IsNotExist(err) {
		t.Error("System database file was not created")
	}

	systemDB := dm.GetSystemDatabase()
	if systemDB == nil {
		t.Fatal("System database is nil")
	}

	var tableName string
	err = systemDB.GetDB().QueryRow(
		"SELECT name FROM sqlite_master WHERE type='table' AND name='__marmot_databases'",
	).Scan(&tableName)
	if err != nil {
		t.Fatalf("__marmot_databases table does not exist: %v", err)
	}
	if tableName != "__marmot_databases" {
		t.Errorf("Expected table '__marmot_databases', got '%s'", tableName)
	}

	_, err = systemDB.GetDB().Exec("INSERT INTO __marmot_databases (name, created_at, path) VALUES (?, ?, ?)",
		"test_write", 123456789, "databases/test_write.db")
	if err != nil {
		t.Errorf("System DB is not writable: %v", err)
	}

	t.Log("✓ Replica creates own system database")
}

// TestReplica_TracksFollowedDatabasesInSystemDB tests database registration in local system DB
func TestReplica_TracksFollowedDatabasesInSystemDB(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("db1"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	var name, path string
	var createdAt int64
	err := dm.GetSystemDatabase().GetDB().QueryRow(
		"SELECT name, created_at, path FROM __marmot_databases WHERE name = ?", "db1",
	).Scan(&name, &createdAt, &path)
	if err != nil {
		t.Fatalf("Database 'db1' not found in system DB: %v", err)
	}

	if name != "db1" {
		t.Errorf("Expected name 'db1', got '%s'", name)
	}
	if createdAt == 0 {
		t.Error("Expected non-zero created_at timestamp")
	}
	if path != "databases/db1.db" {
		t.Errorf("Expected path 'databases/db1.db', got '%s'", path)
	}

	dbPath := filepath.Join(tmpDir, path)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("Database file does not exist at path: %s", dbPath)
	}

	t.Log("✓ Replica tracks followed databases in local system DB")
}

// TestReplica_CreateDatabaseDDL_UpdatesLocalSystemDB tests CREATE DATABASE DDL updates local system DB
func TestReplica_CreateDatabaseDDL_UpdatesLocalSystemDB(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("db2"); err != nil {
		t.Fatalf("Failed to create database via DDL: %v", err)
	}

	var count int
	err := dm.GetSystemDatabase().GetDB().QueryRow(
		"SELECT COUNT(*) FROM __marmot_databases WHERE name = ?", "db2",
	).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query system DB: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 row for 'db2', got %d", count)
	}

	if !dm.DatabaseExists("db2") {
		t.Error("Database 'db2' does not exist in DatabaseManager")
	}

	t.Log("✓ CREATE DATABASE DDL updates local system DB")
}

// TestReplica_DropDatabaseDDL_UpdatesLocalSystemDB tests DROP DATABASE DDL updates local system DB
func TestReplica_DropDatabaseDDL_UpdatesLocalSystemDB(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("db1"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	var count int
	err := dm.GetSystemDatabase().GetDB().QueryRow(
		"SELECT COUNT(*) FROM __marmot_databases WHERE name = ?", "db1",
	).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query system DB: %v", err)
	}
	if count != 1 {
		t.Fatalf("Database 'db1' not registered before drop")
	}

	if err := dm.DropDatabase("db1"); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// The registry row is kept as a tombstone, not deleted (DatabaseRegistryKey).
	var dropped bool
	err = dm.GetSystemDatabase().GetDB().QueryRow(
		"SELECT COUNT(*), dropped FROM __marmot_databases WHERE name = ? GROUP BY dropped", "db1",
	).Scan(&count, &dropped)
	if err != nil {
		t.Fatalf("Failed to query system DB after drop: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 tombstone row for 'db1' after drop, got %d", count)
	}
	if !dropped {
		t.Error("Expected 'db1' row to be tombstoned (dropped=1) after drop")
	}

	if dm.DatabaseExists("db1") {
		t.Error("Database 'db1' still exists in DatabaseManager after drop")
	}

	dbPath := filepath.Join(tmpDir, "databases", "db1.db")
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Error("Database file still exists after drop")
	}

	t.Log("✓ DROP DATABASE DDL removes database from local system DB")
}

// TestReplica_SystemDBIsolation tests that replica's system DB is independent from primary
func TestReplica_SystemDBIsolation(t *testing.T) {
	primaryDir := t.TempDir()
	replicaDir := t.TempDir()
	clock := hlc.NewClock(1)

	primaryDM, err := NewDatabaseManager(primaryDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create primary DatabaseManager: %v", err)
	}
	defer primaryDM.Close()

	if err := primaryDM.CreateDatabase("db1"); err != nil {
		t.Fatalf("Failed to create db1 on primary: %v", err)
	}
	if err := primaryDM.CreateDatabase("db2"); err != nil {
		t.Fatalf("Failed to create db2 on primary: %v", err)
	}
	if err := primaryDM.CreateDatabase("db3"); err != nil {
		t.Fatalf("Failed to create db3 on primary: %v", err)
	}

	replicaDM, err := NewDatabaseManager(replicaDir, 2, hlc.NewClock(2))
	if err != nil {
		t.Fatalf("Failed to create replica DatabaseManager: %v", err)
	}
	defer replicaDM.Close()

	if err := replicaDM.CreateDatabase("db1"); err != nil {
		t.Fatalf("Failed to create db1 on replica: %v", err)
	}

	var count int
	err = replicaDM.GetSystemDatabase().GetDB().QueryRow(
		"SELECT COUNT(*) FROM __marmot_databases WHERE name IN ('db1', 'db2', 'db3')",
	).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query replica system DB: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected replica to have only 1 database (db1), got %d", count)
	}

	var db1Name string
	err = replicaDM.GetSystemDatabase().GetDB().QueryRow(
		"SELECT name FROM __marmot_databases WHERE name = 'db1'",
	).Scan(&db1Name)
	if err != nil {
		t.Fatalf("db1 not found in replica system DB: %v", err)
	}
	if db1Name != "db1" {
		t.Errorf("Expected 'db1', got '%s'", db1Name)
	}

	err = replicaDM.GetSystemDatabase().GetDB().QueryRow(
		"SELECT name FROM __marmot_databases WHERE name = 'db2'",
	).Scan(&db1Name)
	if err == nil {
		t.Error("db2 should not exist in replica system DB")
	}

	err = replicaDM.GetSystemDatabase().GetDB().QueryRow(
		"SELECT name FROM __marmot_databases WHERE name = 'db3'",
	).Scan(&db1Name)
	if err == nil {
		t.Error("db3 should not exist in replica system DB")
	}

	t.Log("✓ Replica system DB is isolated from primary system DB")
}

// TestReplica_DoesNotReceiveSystemDBFromPrimary tests system DB exclusion during snapshot
func TestReplica_DoesNotReceiveSystemDBFromPrimary(t *testing.T) {
	primaryDir := t.TempDir()
	replicaDir := t.TempDir()
	clock := hlc.NewClock(1)

	primaryDM, err := NewDatabaseManager(primaryDir, 1, clock)
	if err != nil {
		t.Fatalf("Failed to create primary DatabaseManager: %v", err)
	}
	defer primaryDM.Close()

	if err := primaryDM.CreateDatabase("userdb"); err != nil {
		t.Fatalf("Failed to create userdb on primary: %v", err)
	}

	snapshotDir := filepath.Join(primaryDir, "snapshot")
	snapshots, _, err := primaryDM.TakeSnapshotToDir(snapshotDir)
	if err != nil {
		t.Fatalf("Failed to take snapshot: %v", err)
	}

	hasSystemDB := false
	hasUserDB := false
	for _, snap := range snapshots {
		if snap.Name == SystemDatabaseName {
			hasSystemDB = true
		}
		if snap.Name == "userdb" {
			hasUserDB = true
		}
	}

	if !hasSystemDB {
		t.Error("Expected system DB in primary snapshot for backward compatibility")
	}
	if !hasUserDB {
		t.Error("Expected userdb in primary snapshot")
	}

	replicaDM, err := NewDatabaseManager(replicaDir, 2, hlc.NewClock(2))
	if err != nil {
		t.Fatalf("Failed to create replica DatabaseManager: %v", err)
	}
	defer replicaDM.Close()

	replicaSystemDBPath := filepath.Join(replicaDir, SystemDatabaseName+".db")
	initialInfo, err := os.Stat(replicaSystemDBPath)
	if err != nil {
		t.Fatalf("Replica system DB does not exist: %v", err)
	}
	initialModTime := initialInfo.ModTime()

	for _, snap := range snapshots {
		if snap.Name == SystemDatabaseName {
			continue
		}

		srcPath := snap.FullPath
		destPath := filepath.Join(replicaDir, snap.Filename)

		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			t.Fatalf("Failed to create directories: %v", err)
		}

		if err := copyFile(srcPath, destPath); err != nil {
			t.Fatalf("Failed to copy database file: %v", err)
		}

		if snap.Name == "userdb" {
			if err := replicaDM.CreateDatabase("userdb"); err != nil && !strings.Contains(err.Error(), "already exists") {
				t.Fatalf("Failed to register userdb on replica: %v", err)
			}
		}
	}

	afterInfo, err := os.Stat(replicaSystemDBPath)
	if err != nil {
		t.Fatalf("Replica system DB missing after snapshot: %v", err)
	}

	if afterInfo.ModTime() != initialModTime && afterInfo.Size() != initialInfo.Size() {
		t.Error("Replica system DB was modified by snapshot restore")
	}

	var count int
	err = replicaDM.GetSystemDatabase().GetDB().QueryRow(
		"SELECT COUNT(*) FROM __marmot_databases WHERE name = ?", "userdb",
	).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query replica system DB: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected userdb registered in replica system DB, got %d rows", count)
	}

	t.Log("✓ Replica does not receive system DB from primary during snapshot")
}

// TestReplica_SnapshotExcludesSystemDB tests that snapshot preparation excludes system DB for replicas
func TestReplica_SnapshotExcludesSystemDB(t *testing.T) {
	dm, tmpDir := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("app_db"); err != nil {
		t.Fatalf("Failed to create app_db: %v", err)
	}

	snapshots, _, err := dm.TakeSnapshot()
	if err != nil {
		t.Fatalf("Failed to take snapshot: %v", err)
	}

	systemDBIncluded := false
	appDBIncluded := false
	defaultDBIncluded := false

	for _, snap := range snapshots {
		t.Logf("Snapshot file: %s (name: %s)", snap.Filename, snap.Name)
		if snap.Name == SystemDatabaseName {
			systemDBIncluded = true
		}
		if snap.Name == "app_db" {
			appDBIncluded = true
		}
		if snap.Name == DefaultDatabaseName {
			defaultDBIncluded = true
		}
	}

	if !systemDBIncluded {
		t.Error("System DB should be included in snapshot for backward compatibility")
	}
	if !appDBIncluded {
		t.Error("app_db should be included in snapshot")
	}
	if !defaultDBIncluded {
		t.Error("Default database should be included in snapshot")
	}

	snapshotDir := filepath.Join(tmpDir, "snapshot_test")
	_, _, err = dm.TakeSnapshotForDatabase(snapshotDir, "app_db")
	if err != nil {
		t.Fatalf("TakeSnapshotForDatabase failed: %v", err)
	}

	systemDBSnapshot := filepath.Join(snapshotDir, SystemDatabaseName+".db")
	if _, err := os.Stat(systemDBSnapshot); !os.IsNotExist(err) {
		t.Error("System DB should NOT be in per-database snapshot")
	}

	appDBSnapshot := filepath.Join(snapshotDir, "databases", "app_db.db")
	if _, err := os.Stat(appDBSnapshot); os.IsNotExist(err) {
		t.Error("app_db should be in per-database snapshot")
	}

	t.Log("✓ Snapshot correctly includes/excludes system DB based on context")
}

// TestReplica_SnapshotFileListExcludesSystemDB tests that replicas filter out system DB from snapshot file list
func TestReplica_SnapshotFileListExcludesSystemDB(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("app_db"); err != nil {
		t.Fatalf("Failed to create app_db: %v", err)
	}

	snapshots, _, err := dm.TakeSnapshot()
	if err != nil {
		t.Fatalf("Failed to take snapshot: %v", err)
	}

	fileList := make([]SnapshotInfo, 0)
	for _, snap := range snapshots {
		if snap.Name != SystemDatabaseName {
			fileList = append(fileList, snap)
		} else {
			t.Logf("Filtering out system DB from snapshot: %s", snap.Name)
		}
	}

	for _, snap := range fileList {
		if snap.Name == SystemDatabaseName {
			t.Errorf("System DB should be filtered from replica snapshot file list")
		}
	}

	if len(fileList) == len(snapshots) {
		t.Error("Expected file list to be smaller after filtering system DB")
	}

	expectedDBs := map[string]bool{
		DefaultDatabaseName: false,
		"app_db":            false,
	}

	for _, snap := range fileList {
		if _, ok := expectedDBs[snap.Name]; ok {
			expectedDBs[snap.Name] = true
		}
	}

	for dbName, found := range expectedDBs {
		if !found {
			t.Errorf("Expected database %s in filtered snapshot list", dbName)
		}
	}

	t.Log("✓ Replica filters system DB from snapshot file list")
}

// TestMigrateDatabaseRegistrySchema_ExistingRowsBecomeLiveGenerationOne pins
// the idempotent migration: a system database
// created before the generation/dropped columns existed gets them added, and
// its existing rows become (generation 1, live) - the same key a first-ever
// CREATE stamps.
func TestMigrateDatabaseRegistrySchema_ExistingRowsBecomeLiveGenerationOne(t *testing.T) {
	sqlDB, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqlDB.Close()

	// Pre-migration shape: no generation/dropped columns.
	if _, err := sqlDB.Exec(`CREATE TABLE __marmot_databases (
		name TEXT PRIMARY KEY,
		created_at INTEGER NOT NULL,
		path TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("Failed to create pre-migration table: %v", err)
	}
	if _, err := sqlDB.Exec(
		"INSERT INTO __marmot_databases (name, created_at, path) VALUES (?, ?, ?)",
		"legacy", 123, "databases/legacy.db",
	); err != nil {
		t.Fatalf("Failed to seed pre-migration row: %v", err)
	}

	if err := migrateDatabaseRegistrySchema(sqlDB); err != nil {
		t.Fatalf("migrateDatabaseRegistrySchema failed: %v", err)
	}
	// Idempotent: a second call over the now-migrated schema must not error.
	if err := migrateDatabaseRegistrySchema(sqlDB); err != nil {
		t.Fatalf("migrateDatabaseRegistrySchema failed on re-run: %v", err)
	}

	var generation int64
	var dropped bool
	if err := sqlDB.QueryRow(
		"SELECT generation, dropped FROM __marmot_databases WHERE name = ?", "legacy",
	).Scan(&generation, &dropped); err != nil {
		t.Fatalf("Failed to read migrated row: %v", err)
	}
	if generation != 1 {
		t.Errorf("Expected migrated row's generation to be 1, got %d", generation)
	}
	if dropped {
		t.Error("Expected migrated row to be live (dropped=0)")
	}
}

// TestCreateDatabase_RecreateAfterDrop_YieldsNextGenerationLive pins that
// CREATE DATABASE on a tombstoned name stamps one generation above the
// tombstone, live.
func TestCreateDatabase_RecreateAfterDrop_YieldsNextGenerationLive(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("recreatedb"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if err := dm.DropDatabase("recreatedb"); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}
	if err := dm.CreateDatabase("recreatedb"); err != nil {
		t.Fatalf("Failed to re-create database: %v", err)
	}

	key, err := dm.RegistryKey("recreatedb")
	if err != nil {
		t.Fatalf("RegistryKey failed: %v", err)
	}
	if key != (DatabaseRegistryKey{Generation: 2, Dropped: false}) {
		t.Errorf("Expected (2, live) after re-create, got %+v", key)
	}
	if !dm.DatabaseExists("recreatedb") {
		t.Error("Expected re-created database to exist")
	}
}

// TestRegistryEntries_IncludesTombstonesExcludesSystem pins RegistryEntries'
// contract: every name ever seen, live or tombstoned, and never the system
// database.
func TestRegistryEntries_IncludesTombstonesExcludesSystem(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("liveentry"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if err := dm.CreateDatabase("droppedentry"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if err := dm.DropDatabase("droppedentry"); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	entries, err := dm.RegistryEntries()
	if err != nil {
		t.Fatalf("RegistryEntries failed: %v", err)
	}

	byName := make(map[string]DatabaseRegistryKey, len(entries))
	for _, e := range entries {
		if e.Name == SystemDatabaseName {
			t.Error("RegistryEntries must exclude the system database")
		}
		byName[e.Name] = e.Key
	}

	if got, ok := byName["liveentry"]; !ok || got != (DatabaseRegistryKey{Generation: 1, Dropped: false}) {
		t.Errorf("Expected liveentry (1, live), got %+v (present=%v)", got, ok)
	}
	if got, ok := byName["droppedentry"]; !ok || got != (DatabaseRegistryKey{Generation: 1, Dropped: true}) {
		t.Errorf("Expected droppedentry (1, dropped), got %+v (present=%v)", got, ok)
	}
}

// TestApplyDatabaseOp_HigherKeyApplies verifies ApplyDatabaseOp adopts a key
// strictly above the local one and reports changed=true.
func TestApplyDatabaseOp_HigherKeyApplies(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	changed, err := dm.ApplyDatabaseOp("applyhigher", DatabaseRegistryKey{Generation: 1, Dropped: false})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if !changed {
		t.Error("Expected changed=true for a key above local (never seen)")
	}
	if !dm.DatabaseExists("applyhigher") {
		t.Error("Expected database to be created")
	}
}

// TestApplyDatabaseOp_EqualOrLowerIsNoOp verifies ApplyDatabaseOp is a
// no-op, reporting changed=false, for a key at or below the local one.
func TestApplyDatabaseOp_EqualOrLowerIsNoOp(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("applyeq"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Equal.
	changed, err := dm.ApplyDatabaseOp("applyeq", DatabaseRegistryKey{Generation: 1, Dropped: false})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if changed {
		t.Error("Expected changed=false for a key equal to local")
	}

	// Below: generation 0 is only ever below a real (>=1) live generation.
	changed, err = dm.ApplyDatabaseOp("applyeq", DatabaseRegistryKey{Generation: 0, Dropped: true})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if changed {
		t.Error("Expected changed=false for a key below local")
	}
	if !dm.DatabaseExists("applyeq") {
		t.Error("A no-op ApplyDatabaseOp must not drop the database")
	}
}

// TestApplyDatabaseOp_DroppedBeatsLiveAtSameGeneration verifies (g, dropped)
// outranks (g, live): a DROP at the same generation as the local live key is
// applied.
func TestApplyDatabaseOp_DroppedBeatsLiveAtSameGeneration(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("dropbeatslive"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	changed, err := dm.ApplyDatabaseOp("dropbeatslive", DatabaseRegistryKey{Generation: 1, Dropped: true})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if !changed {
		t.Error("Expected (1, dropped) to outrank and replace local (1, live)")
	}
	if dm.DatabaseExists("dropbeatslive") {
		t.Error("Expected database to be dropped")
	}
}

// TestApplyDatabaseOp_LiveAtNextGenerationBeatsDropped verifies (g+1, live)
// outranks a local (g, dropped) tombstone: a re-create is applied over a
// drop.
func TestApplyDatabaseOp_LiveAtNextGenerationBeatsDropped(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("recreatebeatsdrop"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if err := dm.DropDatabase("recreatebeatsdrop"); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	changed, err := dm.ApplyDatabaseOp("recreatebeatsdrop", DatabaseRegistryKey{Generation: 2, Dropped: false})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if !changed {
		t.Error("Expected (2, live) to outrank and replace local (1, dropped)")
	}
	if !dm.DatabaseExists("recreatebeatsdrop") {
		t.Error("Expected database to be re-created")
	}
}

// TestApplyDatabaseOp_PeerLiveNeverResurrectsLocalDropped verifies a peer's
// (g, live) entry can never resurrect a local (g, dropped) tombstone - the
// core convergence guarantee (ApplyDatabaseOp's proof).
func TestApplyDatabaseOp_PeerLiveNeverResurrectsLocalDropped(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("neverresurrect"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if err := dm.DropDatabase("neverresurrect"); err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// A peer that missed the DROP still reports (1, live) - the key from
	// before the drop.
	changed, err := dm.ApplyDatabaseOp("neverresurrect", DatabaseRegistryKey{Generation: 1, Dropped: false})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if changed {
		t.Error("Expected a peer's (1, live) to lose to local (1, dropped)")
	}
	if dm.DatabaseExists("neverresurrect") {
		t.Error("Database must stay dropped")
	}
}

// TestApplyDatabaseOp_MissedCreate_CreatesDatabase models a node that missed
// the original CREATE DATABASE entirely: it has never seen the name (local
// key (0, dropped)), and a peer's key creates it.
func TestApplyDatabaseOp_MissedCreate_CreatesDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if dm.DatabaseExists("missedcreate") {
		t.Fatal("Database must not exist before ApplyDatabaseOp")
	}

	changed, err := dm.ApplyDatabaseOp("missedcreate", DatabaseRegistryKey{Generation: 1, Dropped: false})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if !changed {
		t.Error("Expected changed=true")
	}
	if !dm.DatabaseExists("missedcreate") {
		t.Error("Expected the missed database to be created")
	}
}

// TestApplyDatabaseOp_MissedDrop_DropsDatabase models a node that missed a
// DROP DATABASE: it still has the database live locally, and a peer's
// tombstone key drops it.
func TestApplyDatabaseOp_MissedDrop_DropsDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("misseddrop"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	changed, err := dm.ApplyDatabaseOp("misseddrop", DatabaseRegistryKey{Generation: 1, Dropped: true})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if !changed {
		t.Error("Expected changed=true")
	}
	if dm.DatabaseExists("misseddrop") {
		t.Error("Expected the missed drop to be applied")
	}
}

// TestApplyDatabaseOp_RefusesSystemAndDefaultDatabase verifies ApplyDatabaseOp
// keeps the same drop protections DropDatabase enforces directly.
func TestApplyDatabaseOp_RefusesSystemAndDefaultDatabase(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if _, err := dm.ApplyDatabaseOp(SystemDatabaseName, DatabaseRegistryKey{Generation: 1, Dropped: false}); err == nil {
		t.Error("Expected an error applying a create op to the system database")
	}
	if _, err := dm.ApplyDatabaseOp(SystemDatabaseName, DatabaseRegistryKey{Generation: 1, Dropped: true}); err == nil {
		t.Error("Expected an error applying a drop op to the system database")
	}

	defaultKey, err := dm.RegistryKey(DefaultDatabaseName)
	if err != nil {
		t.Fatalf("RegistryKey failed: %v", err)
	}
	if _, err := dm.ApplyDatabaseOp(DefaultDatabaseName, DatabaseRegistryKey{Generation: defaultKey.Generation + 1, Dropped: true}); err == nil {
		t.Error("Expected an error applying a drop op to the default database")
	}
	if !dm.DatabaseExists(DefaultDatabaseName) {
		t.Error("Default database must survive a refused drop")
	}
}

// TestApplyDatabaseOpClearsLegacyOnAStampedCreate (see ApplyDatabaseOp's doc
// comment): a legacy registry row (migrated from an older release's registry,
// no real generation
// history) stops being legacy the moment a real, post-upgrade CREATE merges
// a higher generation into it, so it reconciles as an ordinary live key from
// then on - including on a peer that has no row for the name at all, which
// a still-legacy row must never create (reconcileRegistryWithPeer,
// grpc/anti_entropy.go).
func TestApplyDatabaseOpClearsLegacyOnAStampedCreate(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("legacydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	// Simulate a row migrated from an older release's registry (migrateDatabaseRegistrySchema).
	if _, err := dm.systemDB.GetDB().Exec("UPDATE __marmot_databases SET legacy = 1 WHERE name = ?", "legacydb"); err != nil {
		t.Fatalf("failed to mark row legacy: %v", err)
	}

	// A peer's real, post-upgrade CREATE at a higher generation - merged the
	// same way anti-entropy's registry reconciliation would.
	changed, err := dm.ApplyDatabaseOp("legacydb", DatabaseRegistryKey{Generation: 5, Dropped: false})
	if err != nil {
		t.Fatalf("ApplyDatabaseOp failed: %v", err)
	}
	if !changed {
		t.Fatal("expected the higher-generation key to be adopted")
	}

	entries, err := dm.RegistryEntries()
	if err != nil {
		t.Fatalf("RegistryEntries failed: %v", err)
	}
	var found bool
	for _, e := range entries {
		if e.Name != "legacydb" {
			continue
		}
		found = true
		if e.Legacy {
			t.Error("a stamped CREATE merged via ApplyDatabaseOp did not clear the legacy flag")
		}
		if e.Key.Generation != 5 || e.Key.Dropped {
			t.Errorf("expected key (5, live), got (%d, dropped=%v)", e.Key.Generation, e.Key.Dropped)
		}
	}
	if !found {
		t.Fatal("legacydb not found in RegistryEntries")
	}
}
