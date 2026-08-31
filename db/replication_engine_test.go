package db

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeTestValues encodes a map of column values to msgpack-encoded bytes for testing
func encodeTestValues(values map[string]interface{}) map[string][]byte {
	encoded := make(map[string][]byte, len(values))
	for key, val := range values {
		data, _ := encoding.Marshal(val)
		encoded[key] = data
	}
	return encoded
}

func testProtocolDMLStatement(stmtType protocol.StatementCode, database, table string, intentKey []byte, oldValues, newValues map[string][]byte) protocol.Statement {
	var op uint8
	switch stmtType {
	case protocol.StatementDelete:
		op = uint8(OpTypeDelete)
	case protocol.StatementUpdate:
		op = uint8(OpTypeUpdate)
	case protocol.StatementReplace:
		op = uint8(OpTypeReplace)
	default:
		op = uint8(OpTypeInsert)
	}
	row := &EncodedCapturedRow{
		Table:     table,
		Op:        op,
		IntentKey: intentKey,
		OldValues: oldValues,
		NewValues: newValues,
	}
	encoded, err := EncodeRow(row)
	if err != nil {
		panic(err)
	}
	return protocol.Statement{
		Type:         stmtType,
		Database:     database,
		TableName:    table,
		IntentKey:    intentKey,
		OldValues:    oldValues,
		NewValues:    newValues,
		Operation:    op,
		EncodedRow:   encoded,
		EncodedCodec: EncodedCapturedRowCodecMsgpack(),
	}
}

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

	// Verify intent key uses binary DDL format
	expectedKey := filter.EncodeDDLIntentKey("users")
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

// TestReplicationEngine_PrepareWithCDC verifies PREPARE creates write intents but does NOT store CDC
// CDC data is deferred to COMMIT phase for bandwidth optimization
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

	// PREPARE carries CDC row images because it is the 2PC durability point.
	req := &PrepareRequest{
		TxnID:    1002,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			testProtocolDMLStatement(protocol.StatementInsert, "testdb", "users", []byte("users:1"), nil, map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("alice"),
			}),
			testProtocolDMLStatement(protocol.StatementUpdate, "testdb", "users", []byte("users:2"), map[string][]byte{
				"id":   []byte("2"),
				"name": []byte("bob"),
			}, map[string][]byte{
				"id":   []byte("2"),
				"name": []byte("bob_updated"),
			}),
			testProtocolDMLStatement(protocol.StatementDelete, "testdb", "users", []byte("users:3"), map[string][]byte{
				"id":   []byte("3"),
				"name": []byte("charlie"),
			}, nil),
		},
	}

	result := engine.Prepare(ctx, req)

	// Verify success
	require.True(t, result.Success)
	require.Empty(t, result.Error)

	// Verify write intents created (for conflict detection)
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1002)
	require.NoError(t, err)
	require.Len(t, intents, 3, "Should have 3 write intents for conflict detection")

	// PREPARE is the 2PC durability point, so CDC row images are persisted before ACK.
	entries, err := metaStore.GetIntentEntries(1002)
	require.NoError(t, err)
	require.Len(t, entries, 3, "CDC entries should be stored during PREPARE")
}

// TestReplicationEngine_PrepareDurableCDCFlow tests the full PREPARE → COMMIT flow
// with CDC row images durable at PREPARE and decision-only COMMIT.
func TestReplicationEngine_DeferredCDCFlow(t *testing.T) {
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
	startTS := hlc.Timestamp{WallTime: 3000, Logical: 1}

	encodedValues := encodeTestValues(map[string]interface{}{
		"id":   int64(1),
		"name": "alice",
	})

	// PREPARE phase carries CDC row data because participants must be able to
	// commit after a crash once they ACK PREPARE.
	prepareReq := &PrepareRequest{
		TxnID:    1099,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			testProtocolDMLStatement(protocol.StatementInsert, "testdb", "users", []byte("users:1"), nil, encodedValues),
		},
	}

	prepResult := engine.Prepare(ctx, prepareReq)
	require.True(t, prepResult.Success, "PREPARE should succeed")

	// Verify write intent and CDC entries are created during PREPARE.
	metaStore := db.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(1099)
	require.NoError(t, err)
	require.Len(t, intents, 1, "Should have 1 write intent")

	entries, err := metaStore.GetIntentEntries(1099)
	require.NoError(t, err)
	require.Len(t, entries, 1, "CDC entries should be durable during PREPARE")

	// COMMIT phase is decision metadata only; row data is already durable.
	commitReq := &CommitRequest{
		TxnID:    1099,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				TableName: "users",
				IntentKey: []byte("users:1"),
			},
		},
	}

	commitResult := engine.Commit(ctx, commitReq)
	require.True(t, commitResult.Success, "COMMIT should succeed: %s", commitResult.Error)

	// Verify CDC was applied (entries should be cleaned up after commit, but data should be in SQLite)
	rows, err := db.GetDB().Query("SELECT id, name FROM users")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "alice", name)
		count++
	}
	assert.Equal(t, 1, count, "Should have 1 row in users table")
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
	expectedKey := filter.EncodeDBOpIntentKey("newdb")
	assert.Equal(t, expectedKey, intent.IntentKey)

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
			testProtocolDMLStatement(protocol.StatementUpdate, "conflictdb", "items", []byte("items:1"),
				map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				map[string][]byte{"id": []byte("1"), "value": []byte("a")}),
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
			testProtocolDMLStatement(protocol.StatementUpdate, "conflictdb", "items", []byte("items:1"),
				map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				map[string][]byte{"id": []byte("1"), "value": []byte("b")}),
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
				IntentKey: []byte(""), // Empty - auto-increment
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
			testProtocolDMLStatement(protocol.StatementUpdate, "testdb", "data", []byte(""),
				map[string][]byte{"id": []byte("1"), "val": []byte("old")},
				map[string][]byte{"id": []byte("1"), "val": []byte("new")}),
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
			testProtocolDMLStatement(protocol.StatementInsert, "testdb", "items", []byte("items:1"), nil, map[string][]byte{
				"id":    []byte("1"),
				"value": []byte("test"),
			}),
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

// TestReplicationEngine_PrepareMultipleDDL verifies multiple DDL statements on different tables
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

	// Prepare multiple DDL statements for different tables
	req := &PrepareRequest{
		TxnID:    1012,
		NodeID:   1,
		StartTS:  startTS,
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE TABLE users (id INTEGER PRIMARY KEY)",
				TableName: "users",
			},
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE TABLE posts (id INTEGER PRIMARY KEY)",
				TableName: "posts",
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

	// Verify intent keys are different (different tables)
	assert.NotEqual(t, intents[0].IntentKey, intents[1].IntentKey, "Intent keys should be unique")
	expectedKey1 := filter.EncodeDDLIntentKey("users")
	expectedKey2 := filter.EncodeDDLIntentKey("posts")
	assert.Contains(t, [][]byte{expectedKey1, expectedKey2}, intents[0].IntentKey, "Intent key should match table")
	assert.Contains(t, [][]byte{expectedKey1, expectedKey2}, intents[1].IntentKey, "Intent key should match table")
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

// TestReplicationEngine_PrepareRejectsInvalidDDL verifies that DDL SQLite cannot
// apply is rejected during PREPARE. PREPARE is the 2PC promise point: accepting
// such a statement makes COMMIT fail after peers have already committed.
func TestReplicationEngine_PrepareRejectsInvalidDDL(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	require.NoError(t, dm.CreateDatabase("testdb"))
	replicatedDB, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()

	createReq := &PrepareRequest{
		TxnID:    2001,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 1000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "CREATE TABLE groups (group_id INTEGER PRIMARY KEY, creation_date datetime)",
			TableName: "groups",
		}},
	}
	require.True(t, engine.Prepare(ctx, createReq).Success)
	commitResult := engine.Commit(ctx, &CommitRequest{
		TxnID:      2001,
		Database:   "testdb",
		Statements: createReq.Statements,
	})
	require.True(t, commitResult.Success, "setup commit failed: %s", commitResult.Error)

	// Adding an existing column can never commit - PREPARE must reject it.
	dupReq := &PrepareRequest{
		TxnID:    2002,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 2000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "ALTER TABLE groups ADD COLUMN creation_date datetime NOT NULL DEFAULT '2026-08-10 19:38:56'",
			TableName: "groups",
		}},
	}

	result := engine.Prepare(ctx, dupReq)

	require.False(t, result.Success, "PREPARE must reject DDL that cannot be applied")
	require.Contains(t, result.Error, "duplicate column name: creation_date")

	// A rejected PREPARE must not leave transaction or intent state behind.
	metaStore := replicatedDB.GetMetaStore()
	intents, err := metaStore.GetIntentsByTxn(2002)
	require.NoError(t, err)
	require.Empty(t, intents, "rejected DDL must not create write intents")
	require.Nil(t, replicatedDB.GetTransactionManager().GetTransaction(2002),
		"rejected DDL must not leave a pending transaction")

	// The database must be untouched and still usable for valid DDL.
	validReq := &PrepareRequest{
		TxnID:    2003,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 3000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "ALTER TABLE groups ADD COLUMN uuid TEXT",
			TableName: "groups",
		}},
	}
	require.True(t, engine.Prepare(ctx, validReq).Success, "valid DDL must still prepare")
}

// TestReplicationEngine_PrepareValidatesDependentDDL verifies that DDL depending
// on an earlier statement in the same transaction is not falsely rejected.
func TestReplicationEngine_PrepareValidatesDependentDDL(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	require.NoError(t, dm.CreateDatabase("testdb"))

	result := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:    2101,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 1000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE TABLE memberships (id INTEGER PRIMARY KEY, group_id INTEGER)",
				TableName: "memberships",
			},
			{
				Type:      protocol.StatementDDL,
				SQL:       "CREATE INDEX idx_memberships_group ON memberships(group_id)",
				TableName: "memberships",
			},
		},
	})

	require.True(t, result.Success, "dependent DDL must prepare: %s", result.Error)
}

// TestReplicationEngine_PrepareCancelledDDLIsNotRejection verifies that a
// validation that could not finish is reported as a plain failure, never as a
// rejection. Only a verdict on the statement itself may be final: a timeout says
// nothing about whether the DDL is applicable.
func TestReplicationEngine_PrepareCancelledDDLIsNotRejection(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	require.NoError(t, dm.CreateDatabase("testdb"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := engine.Prepare(ctx, &PrepareRequest{
		TxnID:    2201,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 1000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "CREATE TABLE cancelled_t (id INTEGER PRIMARY KEY)",
			TableName: "cancelled_t",
		}},
	})

	require.False(t, result.Success, "cancelled validation must not report success")
	require.False(t, result.Rejected, "a cancelled validation is not a rejection of the statement")
}

// A statement SQLite refuses is a final verdict and must be marked as such.
func TestReplicationEngine_PrepareInvalidDDLIsRejection(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	require.NoError(t, dm.CreateDatabase("testdb"))

	result := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:    2202,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 1000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "ALTER TABLE missing_table ADD COLUMN x TEXT",
			TableName: "missing_table",
		}},
	})

	require.False(t, result.Success)
	require.True(t, result.Rejected, "invalid DDL must be a final rejection")
	require.True(t, result.ToCoordinatorResponse().Rejected, "rejection must survive the coordinator conversion")
}

// A DDL that loses a lock race against a concurrent DML on the same table via
// hookDB must not be treated as a final rejection.
//
// db_integration.go documents writeDB and hookDB as sharing one SQLite page
// cache via "cache=shared" so contention between them surfaces as immediate
// SQLITE_LOCKED. In practice the DSN mattn's driver receives here is a plain
// filesystem path with no "file:" scheme prefix, and the driver only forwards
// query parameters (including cache=shared) to SQLite when the DSN starts
// with "file:" - otherwise it strips them before opening
// (github.com/mattn/go-sqlite3@v1.14.24/sqlite3.go:1450). So cache=shared is
// silently dropped today, and contention between the two connections is an
// ordinary whole-file SQLITE_BUSY bounded by busy_timeout, not SQLITE_LOCKED.
// Confirmed empirically: with the exact DSN shape db_integration.go builds,
// the error is "database is locked" with Code=sqlite3.ErrBusy, not
// "database table is locked" with Code=sqlite3.ErrLocked. This is a
// pre-existing gap in db_integration.go, outside this fix's scope; it is
// exercised here, not fixed, because it changes what this test must prove.
// Either way the classifier must not reject: SQLITE_BUSY is transient too.
func TestReplicationEngine_PrepareHookDBLockContentionIsNotRejection(t *testing.T) {
	// Shrink the busy-wait window so the test doesn't block for the default
	// 50s lock_wait_timeout_seconds while still forcing a real wait long
	// enough to prove genuine contention, not a fluke.
	originalTimeout := cfg.Config.Transaction.LockWaitTimeoutSeconds
	cfg.Config.Transaction.LockWaitTimeoutSeconds = 1
	t.Cleanup(func() { cfg.Config.Transaction.LockWaitTimeoutSeconds = originalTimeout })

	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()

	require.NoError(t, dm.CreateDatabase("testdb"))
	replicatedDB, err := dm.GetDatabase("testdb")
	require.NoError(t, err)

	ctx := context.Background()

	createReq := &PrepareRequest{
		TxnID:    2301,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 1000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "CREATE TABLE locked_t (id INTEGER PRIMARY KEY, val TEXT)",
			TableName: "locked_t",
		}},
	}
	require.True(t, engine.Prepare(ctx, createReq).Success)
	commitResult := engine.Commit(ctx, &CommitRequest{
		TxnID:      2301,
		Database:   "testdb",
		Statements: createReq.Statements,
	})
	require.True(t, commitResult.Success, "setup commit failed: %s", commitResult.Error)

	// Hold a write lock on locked_t via hookDB - the same connection
	// ExecuteLocalWithHooks uses for CDC capture - to reproduce the real
	// production race rather than a synthetic stand-in.
	hookTx, err := replicatedDB.hookDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hookTx.Rollback() })
	_, err = hookTx.ExecContext(ctx, "INSERT INTO locked_t (id, val) VALUES (1, 'x')")
	require.NoError(t, err)

	start := time.Now()
	ddlReq := &PrepareRequest{
		TxnID:    2302,
		NodeID:   1,
		StartTS:  hlc.Timestamp{WallTime: 2000, Logical: 1},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type:      protocol.StatementDDL,
			SQL:       "ALTER TABLE locked_t ADD COLUMN extra TEXT",
			TableName: "locked_t",
		}},
	}
	result := engine.Prepare(ctx, ddlReq)
	elapsed := time.Since(start)

	require.False(t, result.Success, "DDL contending for the hookDB lock must not succeed while the lock is held")
	// Premise: the failure actually is lock contention with hookDB, not some
	// unrelated failure, and it genuinely waited out busy_timeout rather than
	// failing for a different reason entirely.
	require.Contains(t, result.Error, "locked", "premise failed: expected a lock-contention failure")
	require.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "premise failed: did not wait out busy_timeout - contention was not provoked")
	require.Less(t, elapsed, 5*time.Second, "premise failed: took far longer than the 1s busy_timeout budget")

	require.False(t, result.Rejected,
		"lock contention on this node is a transient condition, not a verdict on the DDL - it must stay a retryable missing ACK")
}
