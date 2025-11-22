package test

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDDLIdempotencyRewriter tests the DDL rewriting for idempotency
func TestDDLIdempotencyRewriter(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "CREATE TABLE",
			input:    "CREATE TABLE users (id INT PRIMARY KEY)",
			expected: "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)",
		},
		{
			name:     "CREATE TABLE already has IF NOT EXISTS",
			input:    "CREATE TABLE IF NOT EXISTS users (id INT)",
			expected: "CREATE TABLE IF NOT EXISTS users (id INT)",
		},
		{
			name:     "DROP TABLE",
			input:    "DROP TABLE users",
			expected: "DROP TABLE IF EXISTS users",
		},
		{
			name:     "DROP TABLE already has IF EXISTS",
			input:    "DROP TABLE IF EXISTS users",
			expected: "DROP TABLE IF EXISTS users",
		},
		{
			name:     "CREATE INDEX",
			input:    "CREATE INDEX idx_name ON users(name)",
			expected: "CREATE INDEX IF NOT EXISTS idx_name ON users(name)",
		},
		{
			name:     "CREATE UNIQUE INDEX",
			input:    "CREATE UNIQUE INDEX idx_email ON users(email)",
			expected: "CREATE UNIQUE INDEX IF NOT EXISTS idx_email ON users(email)",
		},
		{
			name:     "DROP INDEX",
			input:    "DROP INDEX idx_name",
			expected: "DROP INDEX IF EXISTS idx_name",
		},
		{
			name:     "ALTER TABLE (unchanged)",
			input:    "ALTER TABLE users ADD COLUMN email TEXT",
			expected: "ALTER TABLE users ADD COLUMN email TEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := protocol.RewriteDDLForIdempotency(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSchemaVersionManager tests schema version tracking
func TestSchemaVersionManager(t *testing.T) {
	// Create in-memory system database
	dbPath := "file::memory:?cache=shared"
	clock := hlc.NewClock(1)
	mvccDB, err := db.NewMVCCDatabase(dbPath, 1, clock)
	require.NoError(t, err)
	defer mvccDB.Close()

	// Create schema version manager
	svm := db.NewSchemaVersionManager(mvccDB.GetDB())

	t.Run("Initial version is 0", func(t *testing.T) {
		version, err := svm.GetSchemaVersion("testdb")
		require.NoError(t, err)
		assert.Equal(t, uint64(0), version)
	})

	t.Run("Increment version", func(t *testing.T) {
		newVersion, err := svm.IncrementSchemaVersion("testdb", "CREATE TABLE users (id INT)", 1001)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), newVersion)

		// Verify it was persisted
		version, err := svm.GetSchemaVersion("testdb")
		require.NoError(t, err)
		assert.Equal(t, uint64(1), version)
	})

	t.Run("Increment again", func(t *testing.T) {
		newVersion, err := svm.IncrementSchemaVersion("testdb", "ALTER TABLE users ADD email TEXT", 1002)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), newVersion)
	})

	t.Run("Multiple databases", func(t *testing.T) {
		v1, err := svm.IncrementSchemaVersion("db1", "CREATE TABLE foo (x INT)", 2001)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), v1)

		v2, err := svm.IncrementSchemaVersion("db2", "CREATE TABLE bar (y INT)", 2002)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), v2)

		// Verify independent tracking
		versionDB1, _ := svm.GetSchemaVersion("db1")
		versionDB2, _ := svm.GetSchemaVersion("db2")
		assert.Equal(t, uint64(1), versionDB1)
		assert.Equal(t, uint64(1), versionDB2)
	})

	t.Run("GetAllSchemaVersions", func(t *testing.T) {
		allVersions, err := svm.GetAllSchemaVersions()
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(allVersions), 3) // testdb, db1, db2
		assert.Equal(t, uint64(2), allVersions["testdb"])
		assert.Equal(t, uint64(1), allVersions["db1"])
		assert.Equal(t, uint64(1), allVersions["db2"])
	})
}

// TestDDLLockManager tests cluster-wide DDL locking
func TestDDLLockManager(t *testing.T) {
	lockMgr := coordinator.NewDDLLockManager(1 * time.Second)

	clock := hlc.NewClock(1)
	ts := clock.Now()

	t.Run("Acquire lock", func(t *testing.T) {
		lock, err := lockMgr.AcquireLock("testdb", 1, 1001, ts)
		require.NoError(t, err)
		assert.NotNil(t, lock)
		assert.Equal(t, "testdb", lock.Database)
		assert.Equal(t, uint64(1), lock.NodeID)
		assert.Equal(t, uint64(1001), lock.TxnID)

		// Release lock
		err = lockMgr.ReleaseLock("testdb", 1001)
		require.NoError(t, err)
	})

	t.Run("Concurrent lock acquisition fails", func(t *testing.T) {
		// Node 1 acquires lock
		_, err := lockMgr.AcquireLock("testdb", 1, 2001, ts)
		require.NoError(t, err)

		// Node 2 tries to acquire same lock
		_, err = lockMgr.AcquireLock("testdb", 2, 2002, ts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is held by")

		// Release lock
		err = lockMgr.ReleaseLock("testdb", 2001)
		require.NoError(t, err)
	})

	t.Run("Lock expires after lease duration", func(t *testing.T) {
		shortLockMgr := coordinator.NewDDLLockManager(100 * time.Millisecond)

		// Node 1 acquires lock
		_, err := shortLockMgr.AcquireLock("testdb", 1, 3001, ts)
		require.NoError(t, err)

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		// Node 2 can now acquire lock
		_, err = shortLockMgr.AcquireLock("testdb", 2, 3002, ts)
		assert.NoError(t, err)

		// Cleanup
		shortLockMgr.ReleaseLock("testdb", 3002)
	})

	t.Run("Different databases have independent locks", func(t *testing.T) {
		// Node 1 acquires lock on db1
		_, err := lockMgr.AcquireLock("db1", 1, 4001, ts)
		require.NoError(t, err)

		// Node 2 can acquire lock on db2
		_, err = lockMgr.AcquireLock("db2", 2, 4002, ts)
		assert.NoError(t, err)

		// Cleanup
		lockMgr.ReleaseLock("db1", 4001)
		lockMgr.ReleaseLock("db2", 4002)
	})

	t.Run("WaitForLock", func(t *testing.T) {
		// Node 1 acquires lock
		_, err := lockMgr.AcquireLock("testdb", 1, 5001, ts)
		require.NoError(t, err)

		// Start goroutine to wait for lock
		waitDone := make(chan error)
		go func() {
			err := lockMgr.WaitForLock("testdb", 500*time.Millisecond)
			waitDone <- err
		}()

		// Release lock after 200ms
		time.Sleep(200 * time.Millisecond)
		lockMgr.ReleaseLock("testdb", 5001)

		// WaitForLock should complete successfully
		err = <-waitDone
		assert.NoError(t, err)
	})
}

// TestDDLReplicationBasic tests basic DDL replication
func TestDDLReplicationBasic(t *testing.T) {
	// Create test node
	dbPath := "file:ddl_test?mode=memory&cache=shared"
	clock := hlc.NewClock(1)
	mvccDB, err := db.NewMVCCDatabase(dbPath, 1, clock)
	require.NoError(t, err)
	defer mvccDB.Close()

	// Setup database manager
	dbMgr := NewTestDatabaseManager(mvccDB)

	// Setup coordinators
	nodeProvider := &MockNodeProvider{nodes: []uint64{1}}
	replicator := &MockReplicator{}
	localReplicator := db.NewLocalReplicator(1, dbMgr, clock)
	writeCoord := coordinator.NewWriteCoordinator(1, nodeProvider, replicator, localReplicator, 2*time.Second)

	localReader := db.NewLocalReader(dbMgr)
	readCoord := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 2*time.Second)

	// Create DDL lock manager
	ddlLockMgr := coordinator.NewDDLLockManager(30 * time.Second)

	// Create schema version manager (using system database)
	systemDB, err := db.NewMVCCDatabase("file:system_ddl?mode=memory&cache=shared", 1, clock)
	require.NoError(t, err)
	defer systemDB.Close()
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetDB())

	// Create handler with DDL support
	handler := coordinator.NewCoordinatorHandler(1, writeCoord, readCoord, clock, nil, ddlLockMgr, schemaVersionMgr, nil)

	session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}

	t.Run("CREATE TABLE", func(t *testing.T) {
		ddl := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
		_, err := handler.HandleQuery(session, ddl)
		require.NoError(t, err)

		// Verify table exists
		var tableName string
		err = mvccDB.GetDB().QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
		require.NoError(t, err)
		assert.Equal(t, "users", tableName)

		// Verify schema version incremented
		version, err := schemaVersionMgr.GetSchemaVersion("marmot")
		require.NoError(t, err)
		assert.Equal(t, uint64(1), version)
	})

	t.Run("CREATE TABLE idempotent replay", func(t *testing.T) {
		// Replay same DDL (should not error due to IF NOT EXISTS)
		ddl := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
		_, err := handler.HandleQuery(session, ddl)
		assert.NoError(t, err)

		// Schema version should increment again
		version, err := schemaVersionMgr.GetSchemaVersion("marmot")
		require.NoError(t, err)
		assert.Equal(t, uint64(2), version)
	})

	t.Run("CREATE INDEX", func(t *testing.T) {
		ddl := "CREATE INDEX idx_name ON users(name)"
		_, err := handler.HandleQuery(session, ddl)
		require.NoError(t, err)

		// Verify index exists
		var indexName string
		err = mvccDB.GetDB().QueryRow("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_name'").Scan(&indexName)
		require.NoError(t, err)
		assert.Equal(t, "idx_name", indexName)

		// Schema version should be 3 now
		version, err := schemaVersionMgr.GetSchemaVersion("marmot")
		require.NoError(t, err)
		assert.Equal(t, uint64(3), version)
	})

	t.Run("DROP TABLE", func(t *testing.T) {
		ddl := "DROP TABLE users"
		_, err := handler.HandleQuery(session, ddl)
		require.NoError(t, err)

		// Verify table no longer exists
		var count int
		err = mvccDB.GetDB().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Schema version should be 4
		version, err := schemaVersionMgr.GetSchemaVersion("marmot")
		require.NoError(t, err)
		assert.Equal(t, uint64(4), version)
	})

	t.Run("DROP TABLE idempotent replay", func(t *testing.T) {
		// Replay DROP (should not error due to IF EXISTS)
		ddl := "DROP TABLE users"
		_, err := handler.HandleQuery(session, ddl)
		assert.NoError(t, err)

		// Schema version should be 5
		version, err := schemaVersionMgr.GetSchemaVersion("marmot")
		require.NoError(t, err)
		assert.Equal(t, uint64(5), version)
	})
}

// TestDDLWithConcurrentDML tests DDL execution with concurrent DML operations
func TestDDLWithConcurrentDML(t *testing.T) {
	// Create test node
	dbPath := "file:ddl_dml_test?mode=memory&cache=shared"
	clock := hlc.NewClock(1)
	mvccDB, err := db.NewMVCCDatabase(dbPath, 1, clock)
	require.NoError(t, err)
	defer mvccDB.Close()

	// Setup
	dbMgr := NewTestDatabaseManager(mvccDB)
	nodeProvider := &MockNodeProvider{nodes: []uint64{1}}
	replicator := &MockReplicator{}
	localReplicator := db.NewLocalReplicator(1, dbMgr, clock)
	writeCoord := coordinator.NewWriteCoordinator(1, nodeProvider, replicator, localReplicator, 2*time.Second)

	localReader := db.NewLocalReader(dbMgr)
	readCoord := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 2*time.Second)

	ddlLockMgr := coordinator.NewDDLLockManager(30 * time.Second)

	systemDB, err := db.NewMVCCDatabase("file:system_ddl_dml?mode=memory&cache=shared", 1, clock)
	require.NoError(t, err)
	defer systemDB.Close()
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetDB())

	handler := coordinator.NewCoordinatorHandler(1, writeCoord, readCoord, clock, nil, ddlLockMgr, schemaVersionMgr, nil)

	session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}

	// Create initial table
	_, err = handler.HandleQuery(session, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
	require.NoError(t, err)

	t.Run("DML before DDL", func(t *testing.T) {
		// Insert data
		_, err := handler.HandleQuery(session, "INSERT INTO products VALUES (1, 'Widget', 100)")
		require.NoError(t, err)

		// Verify data
		rs, err := handler.HandleQuery(session, "SELECT * FROM products WHERE id = 1")
		require.NoError(t, err)
		assert.Equal(t, 1, len(rs.Rows))
	})

	t.Run("DDL adds column", func(t *testing.T) {
		// SQLite doesn't support ADD COLUMN IF NOT EXISTS, so skip idempotency check
		_, err := handler.HandleQuery(session, "ALTER TABLE products ADD COLUMN stock INT DEFAULT 0")
		require.NoError(t, err)

		// Verify schema version incremented
		version, err := schemaVersionMgr.GetSchemaVersion("marmot")
		require.NoError(t, err)
		assert.Equal(t, uint64(2), version) // 1 for CREATE TABLE, 2 for ALTER TABLE
	})

	t.Run("DML after DDL uses new schema", func(t *testing.T) {
		// Insert with new column
		_, err := handler.HandleQuery(session, "INSERT INTO products VALUES (2, 'Gadget', 200, 50)")
		require.NoError(t, err)

		// Query new column
		rs, err := handler.HandleQuery(session, "SELECT stock FROM products WHERE id = 2")
		require.NoError(t, err)
		assert.Equal(t, 1, len(rs.Rows))
	})
}

// TestDDLLockingSerialization tests that DDL operations are serialized
func TestDDLLockingSerialization(t *testing.T) {
	lockMgr := coordinator.NewDDLLockManager(2 * time.Second)
	clock := hlc.NewClock(1)

	database := "testdb"

	// Track DDL execution order
	executionOrder := make(chan int, 2)

	// Goroutine 1: Acquire lock and hold for 500ms
	go func() {
		ts := clock.Now()
		lock, err := lockMgr.AcquireLock(database, 1, 1001, ts)
		if err != nil {
			return
		}
		executionOrder <- 1
		time.Sleep(500 * time.Millisecond)
		lockMgr.ReleaseLock(database, lock.TxnID)
	}()

	// Give goroutine 1 time to acquire lock
	time.Sleep(100 * time.Millisecond)

	// Goroutine 2: Try to acquire lock (should wait)
	go func() {
		err := lockMgr.WaitForLock(database, 1*time.Second)
		if err == nil {
			ts := clock.Now()
			lock, err := lockMgr.AcquireLock(database, 2, 1002, ts)
			if err == nil {
				executionOrder <- 2
				lockMgr.ReleaseLock(database, lock.TxnID)
			}
		}
	}()

	// Check execution order
	first := <-executionOrder
	second := <-executionOrder

	assert.Equal(t, 1, first, "First DDL should be from node 1")
	assert.Equal(t, 2, second, "Second DDL should be from node 2")
}

// TestSchemaVersionDrift tests detection of schema version differences
func TestSchemaVersionDrift(t *testing.T) {
	// This test simulates gossip-based schema version exchange
	// In reality, this would happen via NodeRegistry.DetectSchemaDrift()

	clock := hlc.NewClock(1)

	// Create two separate system databases for two nodes
	systemDB1, err := db.NewMVCCDatabase("file:system1?mode=memory&cache=shared", 1, clock)
	require.NoError(t, err)
	defer systemDB1.Close()

	systemDB2, err := db.NewMVCCDatabase("file:system2?mode=memory&cache=shared", 2, clock)
	require.NoError(t, err)
	defer systemDB2.Close()

	svm1 := db.NewSchemaVersionManager(systemDB1.GetDB())
	svm2 := db.NewSchemaVersionManager(systemDB2.GetDB())

	t.Run("Nodes start in sync", func(t *testing.T) {
		v1, _ := svm1.GetSchemaVersion("mydb")
		v2, _ := svm2.GetSchemaVersion("mydb")
		assert.Equal(t, v1, v2)
	})

	t.Run("Node 1 executes DDL", func(t *testing.T) {
		_, err := svm1.IncrementSchemaVersion("mydb", "CREATE TABLE foo (id INT)", 1001)
		require.NoError(t, err)

		v1, _ := svm1.GetSchemaVersion("mydb")
		v2, _ := svm2.GetSchemaVersion("mydb")

		assert.Equal(t, uint64(1), v1)
		assert.Equal(t, uint64(0), v2)
		assert.NotEqual(t, v1, v2, "Schema drift detected")
	})

	t.Run("Node 2 catches up", func(t *testing.T) {
		// In real system, this would happen via delta sync
		_, err := svm2.IncrementSchemaVersion("mydb", "CREATE TABLE foo (id INT)", 1001)
		require.NoError(t, err)

		v1, _ := svm1.GetSchemaVersion("mydb")
		v2, _ := svm2.GetSchemaVersion("mydb")

		assert.Equal(t, v1, v2, "Nodes back in sync")
	})
}

// TestMultiDatabaseDDL tests DDL operations across multiple databases
func TestMultiDatabaseDDL(t *testing.T) {
	clock := hlc.NewClock(1)

	// Create system database
	systemDB, err := db.NewMVCCDatabase("file:system_multi?mode=memory&cache=shared", 1, clock)
	require.NoError(t, err)
	defer systemDB.Close()

	svm := db.NewSchemaVersionManager(systemDB.GetDB())

	t.Run("Independent schema versions", func(t *testing.T) {
		// Execute DDL on db1
		v1, err := svm.IncrementSchemaVersion("db1", "CREATE TABLE users (id INT)", 1001)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), v1)

		// Execute DDL on db2
		v2, err := svm.IncrementSchemaVersion("db2", "CREATE TABLE products (id INT)", 1002)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), v2)

		// Verify independent tracking
		db1Version, _ := svm.GetSchemaVersion("db1")
		db2Version, _ := svm.GetSchemaVersion("db2")

		assert.Equal(t, uint64(1), db1Version)
		assert.Equal(t, uint64(1), db2Version)
	})

	t.Run("Get all schema versions", func(t *testing.T) {
		// Add more DDL
		svm.IncrementSchemaVersion("db1", "ALTER TABLE users ADD email TEXT", 1003)
		svm.IncrementSchemaVersion("db3", "CREATE TABLE orders (id INT)", 1004)

		allVersions, err := svm.GetAllSchemaVersions()
		require.NoError(t, err)

		assert.Equal(t, uint64(2), allVersions["db1"])
		assert.Equal(t, uint64(1), allVersions["db2"])
		assert.Equal(t, uint64(1), allVersions["db3"])
	})
}

// TestDDLReplayAfterFailure tests idempotent DDL replay
func TestDDLReplayAfterFailure(t *testing.T) {
	dbPath := "file:ddl_replay?mode=memory&cache=shared"
	clock := hlc.NewClock(1)
	mvccDB, err := db.NewMVCCDatabase(dbPath, 1, clock)
	require.NoError(t, err)
	defer mvccDB.Close()

	// Setup minimal handler
	dbMgr := NewTestDatabaseManager(mvccDB)
	nodeProvider := &MockNodeProvider{nodes: []uint64{1}}
	replicator := &MockReplicator{}
	localReplicator := db.NewLocalReplicator(1, dbMgr, clock)
	writeCoord := coordinator.NewWriteCoordinator(1, nodeProvider, replicator, localReplicator, 2*time.Second)

	localReader := db.NewLocalReader(dbMgr)
	readCoord := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 2*time.Second)

	ddlLockMgr := coordinator.NewDDLLockManager(30 * time.Second)

	systemDB, err := db.NewMVCCDatabase("file:system_replay?mode=memory&cache=shared", 1, clock)
	require.NoError(t, err)
	defer systemDB.Close()
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetDB())

	handler := coordinator.NewCoordinatorHandler(1, writeCoord, readCoord, clock, nil, ddlLockMgr, schemaVersionMgr, nil)

	session := &protocol.ConnectionSession{ConnID: 1, CurrentDatabase: "marmot"}

	t.Run("Replay CREATE TABLE multiple times", func(t *testing.T) {
		ddl := "CREATE TABLE test (id INT)"

		// First execution
		_, err := handler.HandleQuery(session, ddl)
		require.NoError(t, err)

		// Replay 5 times (simulating retries after failure)
		for i := 0; i < 5; i++ {
			time.Sleep(10 * time.Millisecond) // Small delay to avoid overwhelming the mock coordinator
			_, err := handler.HandleQuery(session, ddl)
			assert.NoError(t, err, "Replay %d should not error", i+1)
		}

		// Verify table exists exactly once
		var count int
		err = mvccDB.GetDB().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("Replay DROP TABLE multiple times", func(t *testing.T) {
		ddl := "DROP TABLE test"

		// First execution
		_, err := handler.HandleQuery(session, ddl)
		require.NoError(t, err)

		// Replay 3 times
		for i := 0; i < 3; i++ {
			time.Sleep(10 * time.Millisecond) // Small delay to avoid overwhelming the mock coordinator
			_, err := handler.HandleQuery(session, ddl)
			assert.NoError(t, err, "Replay %d should not error", i+1)
		}

		// Verify table doesn't exist
		var count int
		err = mvccDB.GetDB().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test'").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// TestDDLStatementDetection tests detection of DDL statements
func TestDDLStatementDetection(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		isDDL bool
	}{
		{"CREATE TABLE", "CREATE TABLE users (id INT)", true},
		{"DROP TABLE", "DROP TABLE users", true},
		{"ALTER TABLE", "ALTER TABLE users ADD COLUMN email TEXT", true},
		{"CREATE INDEX", "CREATE INDEX idx ON users(name)", true},
		{"DROP INDEX", "DROP INDEX idx", true},
		{"INSERT", "INSERT INTO users VALUES (1, 'alice')", false},
		{"UPDATE", "UPDATE users SET name = 'bob' WHERE id = 1", false},
		{"DELETE", "DELETE FROM users WHERE id = 1", false},
		{"SELECT", "SELECT * FROM users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := protocol.ParseStatement(tt.sql)
			isDDL := stmt.Type == protocol.StatementDDL
			assert.Equal(t, tt.isDDL, isDDL)
		})
	}
}

// Helper to check if a string contains a substring (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

// TestDDLRewriterPreservesSemantics tests that rewritten DDL has same semantics
func TestDDLRewriterPreservesSemantics(t *testing.T) {
	// Create in-memory database
	testDB, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer testDB.Close()

	t.Run("Rewritten CREATE TABLE works", func(t *testing.T) {
		original := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
		rewritten := protocol.RewriteDDLForIdempotency(original)

		// Execute rewritten DDL
		_, err := testDB.Exec(rewritten)
		require.NoError(t, err)

		// Verify table exists
		var tableName string
		err = testDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
		require.NoError(t, err)
		assert.Equal(t, "users", tableName)

		// Execute again (should not error)
		_, err = testDB.Exec(rewritten)
		assert.NoError(t, err)
	})

	t.Run("Rewritten CREATE INDEX works", func(t *testing.T) {
		original := "CREATE INDEX idx_name ON users(name)"
		rewritten := protocol.RewriteDDLForIdempotency(original)

		// Execute rewritten DDL
		_, err := testDB.Exec(rewritten)
		require.NoError(t, err)

		// Execute again (should not error)
		_, err = testDB.Exec(rewritten)
		assert.NoError(t, err)
	})

	t.Run("Rewritten DROP TABLE works", func(t *testing.T) {
		// Create table first
		_, err := testDB.Exec("CREATE TABLE temp_table (x INT)")
		require.NoError(t, err)

		original := "DROP TABLE temp_table"
		rewritten := protocol.RewriteDDLForIdempotency(original)

		// Execute rewritten DDL
		_, err = testDB.Exec(rewritten)
		require.NoError(t, err)

		// Execute again (should not error)
		_, err = testDB.Exec(rewritten)
		assert.NoError(t, err)
	})
}
