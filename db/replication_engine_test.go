package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestReplicationEngine creates a test DatabaseManager and ReplicationEngine
func setupTestReplicationEngine(t *testing.T) (*ReplicationEngine, *DatabaseManager, func()) {
	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dm, err := NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err, "Failed to create DatabaseManager")

	engine := NewReplicationEngine(1, dm, clock)

	cleanup := func() {
		dm.Close()
	}

	return engine, dm, cleanup
}

// TestReplicationEngine_PrepareWithDDL verifies DDL statement preparation
func TestReplicationEngine_PrepareWithDDL(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 1000, Logical: 1}

	// Prepare DDL statement
	req := &PrepareRequest{
		TxnID:    1001,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
				TableName: "users",
			},
		},
	}

	result := engine.Prepare(ctx, req)

	// Verify success
	require.True(t, result.Success, "Prepare should succeed")
	require.Empty(t, result.Error, "No error expected")
	require.False(t, result.ConflictDetected, "No conflict expected")

	// Verify write intent created
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1001)
	require.NoError(t, err)
	require.Len(t, intents, 1, "Should have 1 write intent")

	// Verify intent details
	intent := intents[0]
	assert.Equal(t, IntentTypeDDL, intent.IntentType)
	assert.Equal(t, "users", intent.TableName)

	// Verify intent key uses SQL hash
	hash := sha256.Sum256([]byte(req.Statements[0].SQL))
	expectedKey := "users:" + hex.EncodeToString(hash[:8])
	assert.Equal(t, expectedKey, intent.IntentKey)

	// Verify DDL snapshot data
	var snapshot DDLSnapshot
	err = DeserializeData(intent.DataSnapshot, &snapshot)
	require.NoError(t, err)
	assert.Equal(t, int(protocol.StatementDDL), snapshot.Type)
	assert.Equal(t, req.Statements[0].SQL, snapshot.SQL)
	assert.Equal(t, "users", snapshot.TableName)
	assert.Equal(t, startTS.WallTime, snapshot.Timestamp)
}

// TestReplicationEngine_PrepareWithCDC verifies CDC data handling
func TestReplicationEngine_PrepareWithCDC(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	_, err = db.GetDB().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 2000, Logical: 1}

	// Prepare statements with CDC data
	req := &PrepareRequest{
		TxnID:    1002,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				TableName: "users",
				IntentKey: "users:1",
				SQL:       "INSERT INTO users (id, name) VALUES (1, 'alice')",
				NewValues: map[string][]byte{
					"id":   []byte("1"),
					"name": []byte("alice"),
				},
			},
			{
				Type:      protocol.StatementUpdate,
				TableName: "users",
				IntentKey: "users:2",
				SQL:       "UPDATE users SET name = 'bob_updated' WHERE id = 2",
				OldValues: map[string][]byte{
					"id":   []byte("2"),
					"name": []byte("bob"),
				},
				NewValues: map[string][]byte{
					"id":   []byte("2"),
					"name": []byte("bob_updated"),
				},
			},
			{
				Type:      protocol.StatementDelete,
				TableName: "users",
				IntentKey: "users:3",
				SQL:       "DELETE FROM users WHERE id = 3",
				OldValues: map[string][]byte{
					"id":   []byte("3"),
					"name": []byte("charlie"),
				},
			},
		},
	}

	result := engine.Prepare(ctx, req)

	// Verify success
	require.True(t, result.Success)
	require.Empty(t, result.Error)

	// Verify write intents created
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1002)
	require.NoError(t, err)
	require.Len(t, intents, 3, "Should have 3 write intents")

	// Verify CDC intent entries stored
	entries, err := metaStore.GetIntentEntries(1002)
	require.NoError(t, err)
	require.Len(t, entries, 3, "Should have 3 CDC entries")

	// Verify first entry (INSERT)
	assert.Equal(t, "users", entries[0].Table)
	assert.Equal(t, "users:1", entries[0].IntentKey)
	assert.Equal(t, uint8(OpTypeInsert), entries[0].Operation)

	// Verify NewValues (already deserialized by MetaStore)
	require.NotNil(t, entries[0].NewValues)
	assert.Equal(t, []byte("1"), entries[0].NewValues["id"])
	assert.Equal(t, []byte("alice"), entries[0].NewValues["name"])

	// Verify second entry (UPDATE)
	assert.Equal(t, uint8(OpTypeUpdate), entries[1].Operation)
	require.NotNil(t, entries[1].OldValues)
	assert.Equal(t, []byte("bob"), entries[1].OldValues["name"])

	// Verify third entry (DELETE)
	assert.Equal(t, uint8(OpTypeDelete), entries[2].Operation)
	assert.NotEmpty(t, entries[2].OldValues)
}

// TestReplicationEngine_PrepareWithDatabaseOps verifies database operation handling
func TestReplicationEngine_PrepareWithDatabaseOps(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 3000, Logical: 1}

	// Test CREATE DATABASE prepare
	createReq := &PrepareRequest{
		TxnID:    1003,
		NodeID:   1,
		StartTS:  startTS,
		Database: "",
		Statements: []protocol.Statement{
			{
				Type:     protocol.StatementCreateDatabase,
				Database: "newdb",
			},
		},
	}

	result := engine.Prepare(ctx, createReq)

	// Verify success
	require.True(t, result.Success)
	require.Empty(t, result.Error)

	// Verify transaction created in system database
	systemDB, err := dm.GetDatabase(SystemDatabaseName)
	require.NoError(t, err)

	txnMgr := systemDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(1003)
	require.NotNil(t, txn, "Transaction should exist in system database")

	// Verify write intent created
	metaStore := systemDB.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1003)
	require.NoError(t, err)
	require.Len(t, intents, 1)

	// Verify intent details
	intent := intents[0]
	assert.Equal(t, IntentTypeDatabaseOp, intent.IntentType)
	assert.Equal(t, "newdb", intent.IntentKey)

	// Verify DatabaseOperationSnapshot
	var snapshot DatabaseOperationSnapshot
	err = DeserializeData(intent.DataSnapshot, &snapshot)
	require.NoError(t, err)
	assert.Equal(t, "newdb", snapshot.DatabaseName)
	assert.Equal(t, DatabaseOpCreate, snapshot.Operation)

	// Test DROP DATABASE prepare
	dropReq := &PrepareRequest{
		TxnID:    1004,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 3001, Logical: 1},
		Database: "",
		Statements: []protocol.Statement{
			{
				Type:     protocol.StatementDropDatabase,
				Database: "olddb",
			},
		},
	}

	result = engine.Prepare(ctx, dropReq)
	require.True(t, result.Success)

	// Verify DROP intent
	intents, err = metaStore.GetIntentsByTxn(1004)
	require.NoError(t, err)
	require.Len(t, intents, 1)

	var dropSnapshot DatabaseOperationSnapshot
	err = DeserializeData(intents[0].DataSnapshot, &dropSnapshot)
	require.NoError(t, err)
	assert.Equal(t, DatabaseOpDrop, dropSnapshot.Operation)
}

// TestReplicationEngine_PrepareConflictDetection verifies conflict detection
func TestReplicationEngine_PrepareConflictDetection(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("conflictdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("conflictdb")
	require.NoError(t, err)

	_, err = db.GetDB().Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
	require.NoError(t, err)

	ctx := context.Background()
	startTS1 := hlc.Timestamp{WallTime: 4000, Logical: 1}
	startTS2 := hlc.Timestamp{WallTime: 4001, Logical: 1}

	// First transaction prepares row "items:1"
	req1 := &PrepareRequest{
		TxnID:    1005,
		NodeID:   1,
		StartTS:  startTS1,
		Database: "conflictdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementUpdate,
				TableName: "items",
				IntentKey: "items:1",
				SQL:       "UPDATE items SET value = 'a' WHERE id = 1",
				OldValues: map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				NewValues: map[string][]byte{"id": []byte("1"), "value": []byte("a")},
			},
		},
	}

	result1 := engine.Prepare(ctx, req1)
	require.True(t, result1.Success, "First prepare should succeed")

	// Second transaction tries same row - should conflict
	req2 := &PrepareRequest{
		TxnID:    1006,
		NodeID:   1,
		StartTS:  startTS2,
		Database: "conflictdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementUpdate,
				TableName: "items",
				IntentKey: "items:1",
				SQL:       "UPDATE items SET value = 'b' WHERE id = 1",
				OldValues: map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				NewValues: map[string][]byte{"id": []byte("1"), "value": []byte("b")},
			},
		},
	}

	result2 := engine.Prepare(ctx, req2)

	// Verify conflict detected
	require.False(t, result2.Success, "Second prepare should fail")
	require.True(t, result2.ConflictDetected, "Conflict should be detected")
	require.NotEmpty(t, result2.ConflictDetails, "Conflict details should be provided")
}

// TestReplicationEngine_PrepareAutoIncrementInsert verifies auto-increment handling
func TestReplicationEngine_PrepareAutoIncrementInsert(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	_, err = db.GetDB().Exec("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 5000, Logical: 1}

	// INSERT without IntentKey (auto-increment)
	req := &PrepareRequest{
		TxnID:    1007,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				TableName: "items",
				IntentKey: "", // Empty - auto-increment
				SQL:       "INSERT INTO items (value) VALUES ('test')",
			},
		},
	}

	result := engine.Prepare(ctx, req)

	// Verify success
	require.True(t, result.Success)
	require.Empty(t, result.Error)

	// Verify NO write intent created (auto-increment skip logic)
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1007)
	require.NoError(t, err)
	require.Len(t, intents, 0, "Should skip write intent for auto-increment INSERT")

	// Verify transaction still exists
	txnMgr := db.GetTransactionManager()
	txn := txnMgr.GetTransaction(1007)
	require.NotNil(t, txn, "Transaction should exist")
}

// TestReplicationEngine_PrepareUpdateWithoutIntentKey verifies UPDATE without IntentKey is skipped
func TestReplicationEngine_PrepareUpdateWithoutIntentKey(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	_, err = db.GetDB().Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 6000, Logical: 1}

	// UPDATE without IntentKey (should be skipped - IntentKey must come from CDC hooks)
	req := &PrepareRequest{
		TxnID:    1008,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementUpdate,
				TableName: "data",
				IntentKey: "", // Empty - must come from CDC preupdate hooks
				SQL:       "UPDATE data SET val = 'new' WHERE id = 1",
				OldValues: map[string][]byte{"id": []byte("1"), "val": []byte("old")},
				NewValues: map[string][]byte{"id": []byte("1"), "val": []byte("new")},
			},
		},
	}

	result := engine.Prepare(ctx, req)

	// Verify success
	require.True(t, result.Success)
	require.Empty(t, result.Error)

	// Verify NO write intent created (IntentKey is required for UPDATE/DELETE)
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1008)
	require.NoError(t, err)
	require.Len(t, intents, 0, "Should skip write intent when IntentKey empty for UPDATE")

	// Verify NO CDC entry stored (IntentKey is required - this is the correct behavior)
	// CDC hooks must provide IntentKey by extracting actual PK values during execution
	entries, err := metaStore.GetIntentEntries(1008)
	require.NoError(t, err)
	require.Len(t, entries, 0, "Should skip CDC entry when IntentKey empty - CDC hooks must provide it")
}

// TestReplicationEngine_CommitSuccess verifies successful commit with DDL
func TestReplicationEngine_CommitSuccess(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 7000, Logical: 1}

	// Prepare transaction with DDL (easier to verify than CDC data)
	prepReq := &PrepareRequest{
		TxnID:    1009,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				TableName: "users",
				SQL:       "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			},
		},
	}

	prepResult := engine.Prepare(ctx, prepReq)
	require.True(t, prepResult.Success)

	// Commit transaction
	commitReq := &CommitRequest{
		TxnID:    1009,
		Database: "testdb",
	}

	commitResult := engine.Commit(ctx, commitReq)

	// Verify commit success
	require.True(t, commitResult.Success)
	require.Empty(t, commitResult.Error)

	// Verify DDL was applied - table should exist
	var tableName string
	err = db.GetDB().QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='users'").Scan(&tableName)
	require.NoError(t, err)
	assert.Equal(t, "users", tableName)

	// Verify transaction marked committed
	metaStore := db.GetMetaStore()
	txnRec, err := metaStore.GetTransaction(1009)
	require.NoError(t, err)
	require.NotNil(t, txnRec)
	assert.Equal(t, TxnStatusCommitted, txnRec.Status)

	// Verify intents cleaned up
	intents, err := metaStore.GetIntentsByTxn(1009)
	require.NoError(t, err)
	assert.Len(t, intents, 0, "Intents should be cleaned up after commit")
}

// TestReplicationEngine_CommitDatabaseOp verifies database operation commit
func TestReplicationEngine_CommitDatabaseOp(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 8000, Logical: 1}

	// Prepare CREATE DATABASE
	prepReq := &PrepareRequest{
		TxnID:    1010,
		NodeID:   1,
		StartTS:  startTS,
		Database: "",
		Statements: []protocol.Statement{
			{
				Type:     protocol.StatementCreateDatabase,
				Database: "commitdb",
			},
		},
	}

	prepResult := engine.Prepare(ctx, prepReq)
	require.True(t, prepResult.Success)

	// Verify database NOT created yet
	assert.False(t, dm.DatabaseExists("commitdb"), "Database should not exist before commit")

	// Commit transaction
	commitReq := &CommitRequest{
		TxnID:    1010,
		Database: "",
	}

	commitResult := engine.Commit(ctx, commitReq)

	// Verify commit success
	require.True(t, commitResult.Success)
	require.Empty(t, commitResult.Error)

	// Verify database created
	assert.True(t, dm.DatabaseExists("commitdb"), "Database should exist after commit")

	// Verify transaction marked committed in system DB
	systemDB, err := dm.GetDatabase(SystemDatabaseName)
	require.NoError(t, err)

	metaStore := systemDB.GetMetaStore()
	txnRec, err := metaStore.GetTransaction(1010)
	require.NoError(t, err)
	require.NotNil(t, txnRec)
	assert.Equal(t, TxnStatusCommitted, txnRec.Status)
}

// TestReplicationEngine_CommitNotFound verifies error when transaction doesn't exist
func TestReplicationEngine_CommitNotFound(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()

	// Try to commit non-existent transaction
	commitReq := &CommitRequest{
		TxnID:    9999,
		Database: "testdb",
	}

	result := engine.Commit(ctx, commitReq)

	// Verify failure
	require.False(t, result.Success)
	require.Contains(t, result.Error, "transaction not found")
}

// TestReplicationEngine_AbortSuccess verifies successful abort
func TestReplicationEngine_AbortSuccess(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database and table
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	_, err = db.GetDB().Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 9000, Logical: 1}

	// Prepare transaction
	prepReq := &PrepareRequest{
		TxnID:    1011,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				TableName: "items",
				IntentKey: "items:1",
				SQL:       "INSERT INTO items (id, value) VALUES (1, 'test')",
				NewValues: map[string][]byte{
					"id":    []byte("1"),
					"value": []byte("test"),
				},
			},
		},
	}

	prepResult := engine.Prepare(ctx, prepReq)
	require.True(t, prepResult.Success)

	// Verify transaction exists
	txnMgr := db.GetTransactionManager()
	txn := txnMgr.GetTransaction(1011)
	require.NotNil(t, txn)

	// Abort transaction
	abortReq := &AbortRequest{
		TxnID:    1011,
		Database: "testdb",
	}

	abortResult := engine.Abort(ctx, abortReq)

	// Verify abort success
	require.True(t, abortResult.Success)
	require.Empty(t, abortResult.Error)

	// Verify transaction cleaned up
	txn = txnMgr.GetTransaction(1011)
	assert.Nil(t, txn, "Transaction should be cleaned up after abort")

	// Verify intents cleaned up
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1011)
	require.NoError(t, err)
	assert.Len(t, intents, 0, "Intents should be cleaned up after abort")

	// Verify data NOT applied
	var count int
	err = db.GetDB().QueryRow("SELECT COUNT(*) FROM items WHERE id = 1").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Data should not be applied after abort")
}

// TestReplicationEngine_AbortDatabaseNotFound verifies abort is idempotent
func TestReplicationEngine_AbortDatabaseNotFound(t *testing.T) {
	engine, _, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Abort with non-existent database (should succeed - idempotent)
	abortReq := &AbortRequest{
		TxnID:    9999,
		Database: "nonexistent",
	}

	result := engine.Abort(ctx, abortReq)

	// Verify success (abort is idempotent)
	require.True(t, result.Success)
	require.Empty(t, result.Error)
}

// TestReplicationEngine_PrepareMultipleDDL verifies multiple DDL statements
func TestReplicationEngine_PrepareMultipleDDL(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	db, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()
	startTS := hlc.Timestamp{WallTime: 10000, Logical: 1}

	// Prepare multiple DDL statements for same table
	req := &PrepareRequest{
		TxnID:    1012,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE TABLE test (id INTEGER PRIMARY KEY)",
				TableName: "test",
			},
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE INDEX idx_test ON test (id)",
				TableName: "test",
			},
		},
	}

	result := engine.Prepare(ctx, req)

	// Verify success
	require.True(t, result.Success)

	// Verify 2 write intents created with different keys
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1012)
	require.NoError(t, err)
	require.Len(t, intents, 2, "Should have 2 intents for 2 DDL statements")

	// Verify intent keys are different (SQL hash makes them unique)
	assert.NotEqual(t, intents[0].IntentKey, intents[1].IntentKey, "Intent keys should be unique")
	assert.Contains(t, intents[0].IntentKey, "test:", "Intent key should contain table name")
	assert.Contains(t, intents[1].IntentKey, "test:", "Intent key should contain table name")
}

// TestReplicationEngine_ClockUpdate verifies clock update during prepare
func TestReplicationEngine_ClockUpdate(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	// Create test database
	err := dm.CreateDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()

	// Initial clock should be at ~1
	initialTime := engine.clock.Now()

	// Prepare with future timestamp
	futureTS := hlc.Timestamp{WallTime: 99999, Logical: 1}
	req := &PrepareRequest{
		TxnID:    1013,
		NodeID:   1,
		StartTS:  futureTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE TABLE test (id INTEGER)",
				TableName: "test",
			},
		},
	}

	result := engine.Prepare(ctx, req)
	require.True(t, result.Success)

	// Verify clock updated to future timestamp
	updatedTime := engine.clock.Now()
	assert.Greater(t, updatedTime.WallTime, initialTime.WallTime, "Clock should be updated")
	assert.GreaterOrEqual(t, updatedTime.WallTime, futureTS.WallTime, "Clock should advance to at least the request timestamp")
}
