package db

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransactionRecord_SchemaVersionPersistence verifies that RequiredSchemaVersion
// is correctly stored in TransactionRecord and retrieved for anti-entropy streaming
func TestTransactionRecord_SchemaVersionPersistence(t *testing.T) {
	// Setup temp directories
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Create meta store
	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	defer metaStore.Close()

	// Create HLC clock
	clock := hlc.NewClock(1)

	// Create schema cache
	schemaCache := NewSchemaCache()

	// Create transaction manager
	txnMgr := NewTransactionManager(db, metaStore, clock, schemaCache)
	defer txnMgr.StopGarbageCollection()

	// Test schema version to persist
	requiredSchemaVersion := uint64(42)

	// Begin transaction
	txn, err := txnMgr.BeginTransaction(1)
	require.NoError(t, err)

	// Set required schema version (this would be set by coordinator)
	txn.RequiredSchemaVersion = requiredSchemaVersion

	// Add a statement
	stmt := protocol.Statement{
		SQL:       "INSERT INTO users (id, name) VALUES (1, 'test')",
		Type:      protocol.StatementInsert,
		TableName: "users",
	}
	err = txnMgr.AddStatement(txn, stmt)
	require.NoError(t, err)

	// Commit transaction (this should persist RequiredSchemaVersion)
	err = txnMgr.CommitTransaction(txn)
	require.NoError(t, err)

	// Retrieve the transaction record from MetaStore
	rec, err := metaStore.GetTransaction(txn.ID)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Verify RequiredSchemaVersion was persisted correctly
	assert.Equal(t, requiredSchemaVersion, rec.RequiredSchemaVersion,
		"RequiredSchemaVersion should be persisted in TransactionRecord")
	assert.Equal(t, TxnStatusCommitted, rec.Status)

	t.Logf("Transaction %d persisted with RequiredSchemaVersion=%d", txn.ID, rec.RequiredSchemaVersion)
}

// TestTransactionRecord_SchemaVersionZeroDefault verifies that transactions
// without a RequiredSchemaVersion (e.g., non-DDL transactions) default to 0
func TestTransactionRecord_SchemaVersionZeroDefault(t *testing.T) {
	// Setup temp directories
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Create meta store
	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	defer metaStore.Close()

	// Create HLC clock
	clock := hlc.NewClock(1)

	// Create schema cache
	schemaCache := NewSchemaCache()

	// Create transaction manager
	txnMgr := NewTransactionManager(db, metaStore, clock, schemaCache)
	defer txnMgr.StopGarbageCollection()

	// Begin transaction without setting RequiredSchemaVersion
	txn, err := txnMgr.BeginTransaction(1)
	require.NoError(t, err)

	// Verify default is 0
	assert.Equal(t, uint64(0), txn.RequiredSchemaVersion)

	// Add a statement
	stmt := protocol.Statement{
		SQL:       "INSERT INTO users (id, name) VALUES (1, 'test')",
		Type:      protocol.StatementInsert,
		TableName: "users",
	}
	err = txnMgr.AddStatement(txn, stmt)
	require.NoError(t, err)

	// Commit transaction
	err = txnMgr.CommitTransaction(txn)
	require.NoError(t, err)

	// Retrieve the transaction record
	rec, err := metaStore.GetTransaction(txn.ID)
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Verify RequiredSchemaVersion defaults to 0
	assert.Equal(t, uint64(0), rec.RequiredSchemaVersion,
		"RequiredSchemaVersion should default to 0 for transactions without schema requirements")

	t.Logf("Transaction %d has default RequiredSchemaVersion=0", txn.ID)
}

// TestStreamCommittedTransactions_IncludesSchemaVersion verifies that
// anti-entropy streaming includes RequiredSchemaVersion from TransactionRecord
func TestStreamCommittedTransactions_IncludesSchemaVersion(t *testing.T) {
	// Setup temp directories
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Create meta store
	metaStore, err := NewMetaStore(dbPath)
	require.NoError(t, err)
	defer metaStore.Close()

	// Create HLC clock
	clock := hlc.NewClock(1)

	// Create schema cache
	schemaCache := NewSchemaCache()

	// Create transaction manager
	txnMgr := NewTransactionManager(db, metaStore, clock, schemaCache)
	txnMgr.SetDatabaseName("test_db")
	defer txnMgr.StopGarbageCollection()

	// Create multiple transactions with different schema versions
	testCases := []struct {
		requiredSchemaVersion uint64
	}{
		{requiredSchemaVersion: 0},  // No schema requirement
		{requiredSchemaVersion: 10}, // Schema version 10 required
		{requiredSchemaVersion: 25}, // Schema version 25 required
	}

	txnIDs := make([]uint64, 0, len(testCases))

	for _, tc := range testCases {
		txn, err := txnMgr.BeginTransaction(1)
		require.NoError(t, err)

		txn.RequiredSchemaVersion = tc.requiredSchemaVersion

		stmt := protocol.Statement{
			SQL:       "INSERT INTO users (id, name) VALUES (1, 'test')",
			Type:      protocol.StatementInsert,
			TableName: "users",
			Database:  "test_db",
		}
		err = txnMgr.AddStatement(txn, stmt)
		require.NoError(t, err)

		err = txnMgr.CommitTransaction(txn)
		require.NoError(t, err)

		txnIDs = append(txnIDs, txn.ID)
	}

	// Stream committed transactions and verify RequiredSchemaVersion
	streamedCount := 0
	err = metaStore.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
		streamedCount++

		// Find matching test case
		idx := -1
		for i, id := range txnIDs {
			if id == rec.TxnID {
				idx = i
				break
			}
		}
		require.NotEqual(t, -1, idx, "Transaction ID should match one of our test cases")

		// Verify RequiredSchemaVersion matches what we set
		expectedVersion := testCases[idx].requiredSchemaVersion
		assert.Equal(t, expectedVersion, rec.RequiredSchemaVersion,
			"RequiredSchemaVersion should match for txn_id=%d", rec.TxnID)

		t.Logf("Streamed transaction %d with RequiredSchemaVersion=%d", rec.TxnID, rec.RequiredSchemaVersion)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, len(testCases), streamedCount, "Should stream all committed transactions")
}
