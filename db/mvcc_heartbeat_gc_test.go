package db

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// TestHeartbeat tests that Heartbeat() updates the last_heartbeat timestamp
func TestHeartbeat(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(testDB.DB, testDB.MetaStore, clock)
	defer tm.StopGarbageCollection()

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	assertNoError(t, err, "BeginTransaction failed")

	// Get initial heartbeat from MetaStore
	txnRecord, err := testDB.MetaStore.GetTransaction(txn.ID)
	assertNoError(t, err, "Failed to get initial transaction record")
	initialHeartbeat := txnRecord.LastHeartbeat

	t.Logf("Initial heartbeat: %d", initialHeartbeat)

	// Wait a bit to ensure time passes
	time.Sleep(50 * time.Millisecond)

	// Send heartbeat
	err = tm.Heartbeat(txn)
	assertNoError(t, err, "Heartbeat failed")

	// Get updated heartbeat from MetaStore
	txnRecord, err = testDB.MetaStore.GetTransaction(txn.ID)
	assertNoError(t, err, "Failed to get updated transaction record")
	updatedHeartbeat := txnRecord.LastHeartbeat

	t.Logf("Updated heartbeat: %d", updatedHeartbeat)

	// Verify heartbeat was updated
	if updatedHeartbeat <= initialHeartbeat {
		t.Errorf("Heartbeat was not updated: initial=%d, updated=%d", initialHeartbeat, updatedHeartbeat)
	}

	t.Log("✓ Heartbeat successfully updated last_heartbeat timestamp")
}

// TestHeartbeatKeepsTransactionAlive tests that regular heartbeats prevent GC cleanup
func TestHeartbeatKeepsTransactionAlive(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)

	// Create transaction manager with very short timeout for faster testing
	tm := NewMVCCTransactionManager(testDB.DB, testDB.MetaStore, clock)
	tm.heartbeatTimeout = 200 * time.Millisecond // Short timeout for testing
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

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

	err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
	assertNoError(t, err, "WriteIntent failed")

	t.Log("✓ Transaction created with write intent")

	// Send heartbeats regularly for 1 second
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		defer close(heartbeatDone)

		for {
			select {
			case <-ticker.C:
				if err := tm.Heartbeat(txn); err != nil {
					t.Logf("Heartbeat error: %v", err)
					return
				}
			case <-stopHeartbeat:
				return
			}
		}
	}()

	// Wait for 1 second (5x the timeout period)
	time.Sleep(1 * time.Second)

	// Stop heartbeats
	close(stopHeartbeat)
	<-heartbeatDone

	// Verify transaction is still PENDING
	verifyTransactionStatusMeta(t, testDB.MetaStore, txn.ID, TxnStatusPending)

	// Verify write intent still exists in MetaStore
	intents, _ := testDB.MetaStore.GetIntentsByTxn(txn.ID)
	if len(intents) != 1 {
		t.Errorf("Expected write intent to still exist, found %d", len(intents))
	}

	t.Log("✓ Transaction kept alive by regular heartbeats")

	// Clean up
	tm.AbortTransaction(txn)
}

// TestStaleTransactionCleanup tests that GC aborts transactions without heartbeat
func TestStaleTransactionCleanup(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)

	// Create transaction manager with short timeout
	tm := NewMVCCTransactionManager(testDB.DB, testDB.MetaStore, clock)
	tm.heartbeatTimeout = 100 * time.Millisecond
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

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

	err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
	assertNoError(t, err, "WriteIntent failed")

	t.Log("✓ Transaction created with write intent")

	// Verify transaction is PENDING
	verifyTransactionStatusMeta(t, testDB.MetaStore, txn.ID, TxnStatusPending)

	// Don't send any heartbeats - let it go stale
	// Wait for timeout + GC interval
	time.Sleep(500 * time.Millisecond)

	// Manually trigger GC
	count, err := tm.cleanupStaleTransactions()
	if err != nil {
		t.Logf("GC error (may be expected): %v", err)
	}

	t.Logf("GC cleaned up %d stale transactions", count)

	// Wait a bit for async cleanup
	time.Sleep(100 * time.Millisecond)

	// Verify transaction was aborted
	verifyTransactionStatusMeta(t, testDB.MetaStore, txn.ID, TxnStatusAborted)

	// Verify write intent was cleaned up
	verifyWriteIntentsClearedMeta(t, testDB.MetaStore, txn.ID)

	t.Log("✓ Stale transaction was aborted by GC")
}

// TestOldTransactionRecordCleanup tests that GC removes old transaction records
func TestOldTransactionRecordCleanup(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(testDB.DB, testDB.MetaStore, clock)
	defer tm.StopGarbageCollection()

	createUserTable(t, testDB.DB)

	// Create and commit a transaction
	txn, err := tm.BeginTransaction(1)
	assertNoError(t, err, "BeginTransaction failed")

	stmt := protocol.Statement{
		SQL:       "UPDATE users SET balance = 200 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data := map[string]interface{}{"balance": 200}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
	assertNoError(t, err, "WriteIntent failed")

	err = tm.CommitTransaction(txn)
	assertNoError(t, err, "CommitTransaction failed")

	t.Log("✓ Transaction committed")

	// Verify transaction record exists in MetaStore
	txnRecord, err := testDB.MetaStore.GetTransaction(txn.ID)
	if err != nil || txnRecord == nil {
		t.Fatalf("Expected transaction record to exist, got error: %v", err)
	}

	// Manually set the created_at to 25 hours ago (older than 24 hour default max retention)
	// Note: The config default is 24 hours, not the code fallback of 4 hours
	oldTS := time.Now().Add(-25 * time.Hour).UnixNano()
	_, err = testDB.MetaStore.WriteDB().Exec("UPDATE __marmot__txn_records SET created_at = ? WHERE txn_id = ?", oldTS, txn.ID)
	assertNoError(t, err, "Failed to update created_at")

	t.Log("✓ Simulated old transaction record (25 hours old)")

	// Run GC cleanup
	cleanedCount, err := tm.cleanupOldTransactionRecords()
	assertNoError(t, err, "cleanupOldTransactionRecords failed")

	t.Logf("GC cleaned up %d old transaction records", cleanedCount)

	// Verify transaction record was removed from MetaStore
	txnRecord, _ = testDB.MetaStore.GetTransaction(txn.ID)
	if txnRecord != nil {
		t.Errorf("Expected transaction record to be removed, but it still exists")
	}

	t.Log("✓ Old transaction record was cleaned up")
}

// TestOldMVCCVersionCleanup tests that GC keeps only the last N versions per row
func TestOldMVCCVersionCleanup(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(testDB.DB, testDB.MetaStore, clock)
	defer tm.StopGarbageCollection()

	createUserTable(t, testDB.DB)

	// Insert initial row
	testDB.DB.Exec("INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)")

	// Create 15 versions of the same row
	for i := 0; i < 15; i++ {
		txn, err := tm.BeginTransaction(uint64(i + 1))
		assertNoError(t, err, "BeginTransaction failed")

		stmt := protocol.Statement{
			SQL:       "UPDATE users SET balance = ? WHERE id = 1",
			Type:      protocol.StatementUpdate,
			TableName: "users",
		}

		data := map[string]interface{}{"balance": 100 + (i * 10)}
		dataBytes, _ := SerializeData(data)

		err = tm.WriteIntent(txn, "users", "1", stmt, dataBytes)
		assertNoError(t, err, "WriteIntent failed")

		err = tm.CommitTransaction(txn)
		assertNoError(t, err, "CommitTransaction failed")

		// Wait for async finalization
		time.Sleep(50 * time.Millisecond)
	}

	t.Log("✓ Created 15 MVCC versions for users:1")

	// Verify we have 15 versions in MetaStore
	versionCount := countMVCCVersionsMeta(t, testDB.MetaStore, "users", "1")
	if versionCount != 15 {
		t.Errorf("Expected 15 versions, found %d", versionCount)
	}

	// Run GC to keep only last 10 versions
	cleanedCount, err := tm.cleanupOldMVCCVersions(10)
	assertNoError(t, err, "cleanupOldMVCCVersions failed")

	t.Logf("GC cleaned up %d old MVCC versions", cleanedCount)

	// Verify only 10 versions remain in MetaStore
	versionCount = countMVCCVersionsMeta(t, testDB.MetaStore, "users", "1")
	if versionCount != 10 {
		t.Errorf("Expected 10 versions after GC, found %d", versionCount)
	}

	t.Log("✓ Old MVCC versions cleaned up, keeping last 10")
}

// TestGarbageCollectionIntegration tests the full GC lifecycle
func TestGarbageCollectionIntegration(t *testing.T) {
	testDB := setupTestDBWithMeta(t)

	clock := hlc.NewClock(1)

	tm := NewMVCCTransactionManager(testDB.DB, testDB.MetaStore, clock)
	tm.heartbeatTimeout = 100 * time.Millisecond
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

	createUserTable(t, testDB.DB)

	// Create a stale transaction
	staleTxn, err := tm.BeginTransaction(1)
	assertNoError(t, err, "BeginTransaction failed")

	stmt := protocol.Statement{
		SQL:       "UPDATE users SET balance = 200 WHERE id = 1",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data := map[string]interface{}{"balance": 200}
	dataBytes, _ := SerializeData(data)

	err = tm.WriteIntent(staleTxn, "users", "1", stmt, dataBytes)
	assertNoError(t, err, "WriteIntent failed")

	// Create an active transaction with heartbeat
	activeTxn, err := tm.BeginTransaction(2)
	assertNoError(t, err, "BeginTransaction failed")

	stmt2 := protocol.Statement{
		SQL:       "UPDATE users SET balance = 300 WHERE id = 2",
		Type:      protocol.StatementUpdate,
		TableName: "users",
	}

	data2 := map[string]interface{}{"balance": 300}
	dataBytes2, _ := SerializeData(data2)

	err = tm.WriteIntent(activeTxn, "users", "2", stmt2, dataBytes2)
	assertNoError(t, err, "WriteIntent failed")

	// Keep active transaction alive with heartbeats
	stopHeartbeat := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tm.Heartbeat(activeTxn)
			case <-stopHeartbeat:
				return
			}
		}
	}()

	// Wait for GC to run
	time.Sleep(500 * time.Millisecond)

	// Trigger manual GC
	tm.runGarbageCollection()
	time.Sleep(100 * time.Millisecond)

	// Stop heartbeats
	close(stopHeartbeat)

	// Verify stale transaction was aborted using MetaStore
	verifyTransactionStatusMeta(t, testDB.MetaStore, staleTxn.ID, TxnStatusAborted)

	// Verify active transaction is still pending using MetaStore
	verifyTransactionStatusMeta(t, testDB.MetaStore, activeTxn.ID, TxnStatusPending)

	t.Log("✓ GC correctly distinguished stale vs active transactions")

	// Clean up
	tm.CommitTransaction(activeTxn)
}
