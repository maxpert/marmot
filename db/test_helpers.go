package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Test helper functions for transaction testing

// assertNoError verifies no error occurred
func assertNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// isWriteWriteConflict checks if error is a write-write conflict
func isWriteWriteConflict(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "write-write conflict") ||
		strings.Contains(errStr, "locked by transaction") ||
		strings.Contains(errStr, "write conflict")
}

// testDBWithMetaStore holds both the user DB and MetaStore for testing
type testDBWithMetaStore struct {
	DB        *sql.DB
	MetaStore MetaStore
	dbPath    string
	metaPath  string
}

// Close closes both db and metastore, and optionally removes files
func (t *testDBWithMetaStore) Close() {
	if t.DB != nil {
		t.DB.Close()
	}
	if t.MetaStore != nil {
		t.MetaStore.Close()
	}
}

// setupTestDBWithMeta creates both a user DB and MetaStore for testing
func setupTestDBWithMeta(t *testing.T) *testDBWithMetaStore {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	return openTestDBWithMeta(t, dbPath)
}

// openTestDBWithMeta opens an existing path with both user DB and MetaStore (PebbleDB)
func openTestDBWithMeta(t *testing.T, dbPath string) *testDBWithMetaStore {
	t.Helper()

	db, err := sql.Open(SQLiteDriverName, dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	metaPath := strings.TrimSuffix(dbPath, ".db") + "_meta.pebble"
	metaStore, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:           16, // Smaller for tests
		MemTableSizeMB:        8,
		MemTableCount:         2,
		L0CompactionThreshold: 4,
		L0StopWrites:          12,
	})
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create meta store: %v", err)
	}

	result := &testDBWithMetaStore{
		DB:        db,
		MetaStore: metaStore,
		dbPath:    dbPath,
		metaPath:  metaPath,
	}

	t.Cleanup(func() {
		result.Close()
		os.Remove(dbPath)
		os.RemoveAll(metaPath)
	})

	return result
}

// verifyWriteIntentsClearedMeta checks that all write intents for a transaction are cleaned up using MetaStore
func verifyWriteIntentsClearedMeta(t *testing.T, ms MetaStore, txnID uint64) {
	t.Helper()
	intents, err := ms.GetIntentsByTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to query write intents: %v", err)
	}
	if len(intents) != 0 {
		t.Errorf("Expected 0 write intents for txn %d, found %d", txnID, len(intents))
	}
}

// verifyTransactionStatusMeta checks the status of a transaction using MetaStore
// Note: Aborted transactions are deleted rather than marked with ABORTED status
func verifyTransactionStatusMeta(t *testing.T, ms MetaStore, txnID uint64, expectedStatus TxnStatus) {
	t.Helper()
	txnRecord, err := ms.GetTransaction(txnID)

	// Aborted transactions are deleted rather than marked with status
	if expectedStatus == TxnStatusAborted {
		if err == sql.ErrNoRows || txnRecord == nil {
			// Expected: aborted transactions are deleted
			return
		}
		if err == nil {
			t.Errorf("Expected transaction %d to be deleted (aborted), but found status %s", txnID, txnRecord.Status.String())
			return
		}
	}

	if err != nil {
		t.Fatalf("Failed to query transaction status: %v", err)
	}
	if txnRecord == nil {
		t.Fatalf("Transaction %d not found", txnID)
	}
	if txnRecord.Status != expectedStatus {
		t.Errorf("Expected transaction %d status %s, got %s", txnID, expectedStatus.String(), txnRecord.Status.String())
	}
}
