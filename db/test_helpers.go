package db

import (
	"database/sql"
	"strings"
	"testing"
)

// Test helper functions for MVCC testing

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

// verifyWriteIntentsCleared checks that all write intents for a transaction are cleaned up
func verifyWriteIntentsCleared(t *testing.T, db *sql.DB, txnID uint64) {
	t.Helper()
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM __marmot__write_intents WHERE txn_id = ?", txnID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query write intents: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 write intents for txn %d, found %d", txnID, count)
	}
}

// verifyTransactionStatus checks the status of a transaction in the database
func verifyTransactionStatus(t *testing.T, db *sql.DB, txnID uint64, expectedStatus string) {
	t.Helper()
	var status string
	err := db.QueryRow("SELECT status FROM __marmot__txn_records WHERE txn_id = ?", txnID).Scan(&status)
	if err != nil {
		t.Fatalf("Failed to query transaction status: %v", err)
	}
	if status != expectedStatus {
		t.Errorf("Expected transaction %d status %s, got %s", txnID, expectedStatus, status)
	}
}

// countMVCCVersions returns the number of MVCC versions for a given table and row
func countMVCCVersions(t *testing.T, db *sql.DB, tableName, rowKey string) int {
	t.Helper()
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM __marmot__mvcc_versions WHERE table_name = ? AND row_key = ?",
		tableName, rowKey).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count MVCC versions: %v", err)
	}
	return count
}
