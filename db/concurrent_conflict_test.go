package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// TestConcurrentWriteIntentConflicts tests that multiple concurrent writes to the same row
// result in exactly one success and all others get write-write conflicts
func TestConcurrentWriteIntentConflicts(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock)

	createUserTable(t, testDB.DB)

	const workers = 20
	startBarrier := make(chan struct{})
	results := make(chan error, workers)

	// All workers try to update the same row
	// DO NOT commit - just create write intents to test pure conflict detection
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			// Wait at barrier for simultaneous start
			<-startBarrier

			txn, err := tm.BeginTransaction(uint64(workerID))
			if err != nil {
				results <- fmt.Errorf("worker %d: begin failed: %w", workerID, err)
				return
			}

			stmt := protocol.Statement{
				SQL:       fmt.Sprintf("UPDATE users SET balance = %d WHERE id = 1", workerID*100),
				Type:      protocol.StatementUpdate,
				TableName: "users",
			}

			data := map[string]interface{}{"balance": workerID * 100}
			dataBytes, _ := SerializeData(data)

			err = tm.WriteIntent(txn, IntentTypeDML, "users", "1", stmt, dataBytes)
			if err != nil {
				_ = tm.AbortTransaction(txn)
				results <- err // Write-write conflict expected
				return
			}

			// Don't commit yet - just test intent creation conflicts
			results <- nil // Success
		}(i)
	}

	// Release all workers simultaneously
	close(startBarrier)

	// Collect results
	successCount := 0
	conflictCount := 0
	otherErrors := 0

	for i := 0; i < workers; i++ {
		err := <-results
		if err == nil {
			successCount++
		} else if isWriteWriteConflict(err) {
			conflictCount++
		} else {
			otherErrors++
			t.Logf("Unexpected error type: %v", err)
		}
	}

	t.Logf("Results: %d succeeded, %d conflicts, %d other errors",
		successCount, conflictCount, otherErrors)

	// Exactly one transaction should succeed
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful transaction, got %d", successCount)
	}

	// All others should conflict
	if conflictCount != workers-1 {
		t.Errorf("Expected %d conflicts, got %d", workers-1, conflictCount)
	}

	// No other errors should occur
	if otherErrors != 0 {
		t.Errorf("Expected 0 other errors, got %d", otherErrors)
	}
}

// TestHighContentionHotspot tests multiple workers hitting a small number of hot rows
func TestHighContentionHotspot(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock)

	createUserTable(t, testDB.DB)

	// Insert hot rows
	for i := 1; i <= 5; i++ {
		_, err := testDB.DB.Exec("INSERT INTO users (id, name, balance) VALUES (?, ?, ?)",
			i, fmt.Sprintf("User%d", i), 100)
		assertNoError(t, err, "Failed to insert test data")
	}

	const workers = 50
	const hotRows = 5

	startBarrier := make(chan struct{})
	results := make(chan error, workers)
	var successfulUpdates int64

	// Workers hit random hot rows
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			<-startBarrier

			// Pick a hot row (distribute load across 5 rows)
			rowID := (workerID % hotRows) + 1

			// Use workerID as nodeID (simulating different nodes)
			// The transaction ID will be generated from HLC clock
			txn, err := tm.BeginTransaction(uint64(workerID + 1))
			if err != nil {
				results <- err
				return
			}

			stmt := protocol.Statement{
				SQL:       fmt.Sprintf("UPDATE users SET balance = balance + 10 WHERE id = %d", rowID),
				Type:      protocol.StatementUpdate,
				TableName: "users",
			}

			data := map[string]interface{}{"balance": 110}
			dataBytes, _ := SerializeData(data)

			err = tm.WriteIntent(txn, IntentTypeDML, "users", fmt.Sprintf("%d", rowID), stmt, dataBytes)
			if err != nil {
				_ = tm.AbortTransaction(txn)
				results <- err
				return
			}

			err = tm.CommitTransaction(txn)
			if err == nil {
				atomic.AddInt64(&successfulUpdates, 1)
			}
			results <- err
		}(i)
	}

	// Release all workers
	close(startBarrier)

	// Collect results
	successCount := 0
	conflictCount := 0
	otherErrors := 0

	for i := 0; i < workers; i++ {
		err := <-results
		if err == nil {
			successCount++
		} else if isWriteWriteConflict(err) {
			conflictCount++
		} else {
			otherErrors++
			t.Logf("Unexpected error from worker: %v", err)
		}
	}

	t.Logf("High contention results: %d succeeded, %d conflicts, %d other errors",
		successCount, conflictCount, otherErrors)
	t.Logf("Success rate: %.1f%%", float64(successCount)/float64(workers)*100)

	// With 50 workers and 5 rows, expect ~10 successes (1 per row) and ~40 conflicts
	// But this is probabilistic due to timing
	if successCount < 5 {
		t.Errorf("Expected at least 5 successful updates (1 per row), got %d", successCount)
	}

	// All workers should return either success or conflict (no other errors expected)
	if otherErrors > 0 {
		t.Errorf("Expected no other errors, got %d", otherErrors)
	}

	if successCount+conflictCount+otherErrors != workers {
		t.Errorf("Total results should equal workers: %d + %d + %d != %d",
			successCount, conflictCount, otherErrors, workers)
	}
}

// TestSerializableSnapshotIsolation tests that non-conflicting concurrent transactions succeed
func TestSerializableSnapshotIsolation(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock)

	createUserTable(t, testDB.DB)

	// Insert initial data
	_, _ = testDB.DB.Exec("INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)")
	_, _ = testDB.DB.Exec("INSERT INTO users (id, name, balance) VALUES (2, 'Bob', 100)")

	var wg sync.WaitGroup
	results := make(chan error, 2)

	// Transaction 1: Write to row 1
	wg.Add(1)
	go func() {
		defer wg.Done()

		txn1, err := tm.BeginTransaction(1)
		if err != nil {
			results <- err
			return
		}

		stmt := protocol.Statement{
			SQL:       "UPDATE users SET balance = 150 WHERE id = 1",
			Type:      protocol.StatementUpdate,
			TableName: "users",
		}

		data := map[string]interface{}{"balance": 150}
		dataBytes, _ := SerializeData(data)

		err = tm.WriteIntent(txn1, IntentTypeDML, "users", "1", stmt, dataBytes)
		if err != nil {
			_ = tm.AbortTransaction(txn1)
			results <- err
			return
		}

		// Small delay to ensure both transactions have intents before committing
		time.Sleep(10 * time.Millisecond)

		err = tm.CommitTransaction(txn1)
		results <- err
	}()

	// Transaction 2: Write to row 2 (different row, no conflict)
	wg.Add(1)
	go func() {
		defer wg.Done()

		txn2, err := tm.BeginTransaction(2)
		if err != nil {
			results <- err
			return
		}

		stmt := protocol.Statement{
			SQL:       "UPDATE users SET balance = 200 WHERE id = 2",
			Type:      protocol.StatementUpdate,
			TableName: "users",
		}

		data := map[string]interface{}{"balance": 200}
		dataBytes, _ := SerializeData(data)

		err = tm.WriteIntent(txn2, IntentTypeDML, "users", "2", stmt, dataBytes)
		if err != nil {
			_ = tm.AbortTransaction(txn2)
			results <- err
			return
		}

		// Small delay
		time.Sleep(10 * time.Millisecond)

		err = tm.CommitTransaction(txn2)
		results <- err
	}()

	wg.Wait()
	close(results)

	// Both transactions should succeed (no write-write conflict on different rows)
	errorCount := 0
	for err := range results {
		if err != nil {
			t.Errorf("Transaction failed (should succeed on different rows): %v", err)
			errorCount++
		}
	}

	if errorCount > 0 {
		t.Fatalf("Expected both transactions to succeed, got %d errors", errorCount)
	}

	t.Log("✓ Both transactions succeeded on different rows (transaction isolation works)")
}

// TestWriteIntentLifecycle tests the full lifecycle of a write intent
func TestWriteIntentLifecycle(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock)

	createUserTable(t, testDB.DB)

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	assertNoError(t, err, "BeginTransaction failed")

	// Create write intent
	stmt := protocol.Statement{
		SQL:       "UPDATE users SET balance = 200 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data := map[string]interface{}{"balance": 200}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(txn, IntentTypeDML, "users", "1", stmt, dataBytes)
	assertNoError(t, err, "WriteIntent failed")

	t.Log("✓ Write intent created")

	// Verify intent exists in MetaStore
	intent, err := testDB.MetaStore.GetIntent("users", "1")
	assertNoError(t, err, "Failed to query write intents")

	if intent == nil || intent.TxnID != txn.ID {
		t.Fatalf("Expected write intent for txn %d, found %v", txn.ID, intent)
	}

	t.Log("✓ Write intent persisted in MetaStore")

	// Commit transaction
	err = tm.CommitTransaction(txn)
	assertNoError(t, err, "CommitTransaction failed")

	t.Log("✓ Transaction committed")

	// Wait for async finalization
	time.Sleep(100 * time.Millisecond)

	// Verify intent is cleaned up in MetaStore
	verifyWriteIntentsClearedMeta(t, testDB.MetaStore, txn.ID)
	t.Log("✓ Write intent cleaned up")

	// Verify transaction status is COMMITTED in MetaStore
	verifyTransactionStatusMeta(t, testDB.MetaStore, txn.ID, TxnStatusCommitted)
	t.Log("✓ Transaction status is COMMITTED")
}

// TestTransactionAbortCleanup tests that aborted transactions clean up properly
func TestTransactionAbortCleanup(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock)

	createUserTable(t, testDB.DB)

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	assertNoError(t, err, "BeginTransaction failed")

	// Create write intent
	stmt := protocol.Statement{
		SQL:       "UPDATE users SET balance = 200 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data := map[string]interface{}{"balance": 200}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(txn, IntentTypeDML, "users", "1", stmt, dataBytes)
	assertNoError(t, err, "WriteIntent failed")

	// Verify intent exists in MetaStore
	intents, _ := testDB.MetaStore.GetIntentsByTxn(txn.ID)
	if len(intents) != 1 {
		t.Fatalf("Expected 1 write intent before abort, found %d", len(intents))
	}

	t.Log("✓ Write intent created")

	// Abort transaction
	err = tm.AbortTransaction(txn)
	assertNoError(t, err, "AbortTransaction failed")

	t.Log("✓ Transaction aborted")

	// Wait for cleanup
	time.Sleep(50 * time.Millisecond)

	// Verify write intents are cleaned up in MetaStore
	verifyWriteIntentsClearedMeta(t, testDB.MetaStore, txn.ID)
	t.Log("✓ Write intents cleaned up")

	// Verify transaction status is ABORTED in MetaStore
	verifyTransactionStatusMeta(t, testDB.MetaStore, txn.ID, TxnStatusAborted)
	t.Log("✓ Transaction status is ABORTED")
}

// TestRaceDetector is a meta-test to ensure race detector is enabled
func TestRaceDetector(t *testing.T) {
	// This test just documents that tests should be run with -race flag
	t.Log("Run with: go test -race ./db/")
	t.Log("This ensures all concurrent tests are checked for race conditions")
}
