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
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)
	defer tm.StopGarbageCollection()

	// Begin transaction
	txn, err := tm.BeginTransaction(1)
	assertNoError(t, err, "BeginTransaction failed")

	// Get initial heartbeat
	var initialHeartbeat int64
	err = db.QueryRow("SELECT last_heartbeat FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).Scan(&initialHeartbeat)
	assertNoError(t, err, "Failed to get initial heartbeat")

	t.Logf("Initial heartbeat: %d", initialHeartbeat)

	// Wait a bit to ensure time passes
	time.Sleep(50 * time.Millisecond)

	// Send heartbeat
	err = tm.Heartbeat(txn)
	assertNoError(t, err, "Heartbeat failed")

	// Get updated heartbeat
	var updatedHeartbeat int64
	err = db.QueryRow("SELECT last_heartbeat FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).Scan(&updatedHeartbeat)
	assertNoError(t, err, "Failed to get updated heartbeat")

	t.Logf("Updated heartbeat: %d", updatedHeartbeat)

	// Verify heartbeat was updated
	if updatedHeartbeat <= initialHeartbeat {
		t.Errorf("Heartbeat was not updated: initial=%d, updated=%d", initialHeartbeat, updatedHeartbeat)
	}

	t.Log("✓ Heartbeat successfully updated last_heartbeat timestamp")
}

// TestHeartbeatKeepsTransactionAlive tests that regular heartbeats prevent GC cleanup
func TestHeartbeatKeepsTransactionAlive(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)

	// Create transaction manager with very short timeout for faster testing
	tm := NewMVCCTransactionManager(db, clock)
	tm.heartbeatTimeout = 200 * time.Millisecond // Short timeout for testing
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

	createUserTable(t, db)

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
	verifyTransactionStatus(t, db, txn.ID, TxnStatusPending)

	// Verify write intent still exists
	var intentCount int
	db.QueryRow("SELECT COUNT(*) FROM __marmot__write_intents WHERE txn_id = ?", txn.ID).Scan(&intentCount)
	if intentCount != 1 {
		t.Errorf("Expected write intent to still exist, found %d", intentCount)
	}

	t.Log("✓ Transaction kept alive by regular heartbeats")

	// Clean up
	tm.AbortTransaction(txn)
}

// TestStaleTransactionCleanup tests that GC aborts transactions without heartbeat
func TestStaleTransactionCleanup(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)

	// Create transaction manager with short timeout
	tm := NewMVCCTransactionManager(db, clock)
	tm.heartbeatTimeout = 100 * time.Millisecond
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

	createUserTable(t, db)

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
	verifyTransactionStatus(t, db, txn.ID, TxnStatusPending)

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
	verifyTransactionStatus(t, db, txn.ID, TxnStatusAborted)

	// Verify write intent was cleaned up
	verifyWriteIntentsCleared(t, db, txn.ID)

	t.Log("✓ Stale transaction was aborted by GC")
}

// TestOldTransactionRecordCleanup tests that GC removes old transaction records
func TestOldTransactionRecordCleanup(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)
	defer tm.StopGarbageCollection()

	createUserTable(t, db)

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

	// Verify transaction record exists
	var count int
	db.QueryRow("SELECT COUNT(*) FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).Scan(&count)
	if count != 1 {
		t.Fatalf("Expected transaction record to exist, found %d", count)
	}

	// Manually set the created_at to 5 hours ago (older than 4 hour max retention)
	oldTS := time.Now().Add(-5 * time.Hour).UnixNano()
	_, err = db.Exec("UPDATE __marmot__txn_records SET created_at = ? WHERE txn_id = ?", oldTS, txn.ID)
	assertNoError(t, err, "Failed to update created_at")

	t.Log("✓ Simulated old transaction record (5 hours old)")

	// Run GC cleanup
	cleanedCount, err := tm.cleanupOldTransactionRecords()
	assertNoError(t, err, "cleanupOldTransactionRecords failed")

	t.Logf("GC cleaned up %d old transaction records", cleanedCount)

	// Verify transaction record was removed
	db.QueryRow("SELECT COUNT(*) FROM __marmot__txn_records WHERE txn_id = ?", txn.ID).Scan(&count)
	if count != 0 {
		t.Errorf("Expected transaction record to be removed, found %d", count)
	}

	t.Log("✓ Old transaction record was cleaned up")
}

// TestOldMVCCVersionCleanup tests that GC keeps only the last N versions per row
func TestOldMVCCVersionCleanup(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)
	tm := NewMVCCTransactionManager(db, clock)
	defer tm.StopGarbageCollection()

	createUserTable(t, db)

	// Insert initial row
	db.Exec("INSERT INTO users (id, name, balance) VALUES (1, 'Alice', 100)")

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

	// Verify we have 15 versions
	versionCount := countMVCCVersions(t, db, "users", "1")
	if versionCount != 15 {
		t.Errorf("Expected 15 versions, found %d", versionCount)
	}

	// Run GC to keep only last 10 versions
	cleanedCount, err := tm.cleanupOldMVCCVersions(10)
	assertNoError(t, err, "cleanupOldMVCCVersions failed")

	t.Logf("GC cleaned up %d old MVCC versions", cleanedCount)

	// Verify only 10 versions remain
	versionCount = countMVCCVersions(t, db, "users", "1")
	if versionCount != 10 {
		t.Errorf("Expected 10 versions after GC, found %d", versionCount)
	}

	t.Log("✓ Old MVCC versions cleaned up, keeping last 10")
}

// TestGarbageCollectionIntegration tests the full GC lifecycle
func TestGarbageCollectionIntegration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	clock := hlc.NewClock(1)

	tm := NewMVCCTransactionManager(db, clock)
	tm.heartbeatTimeout = 100 * time.Millisecond
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

	createUserTable(t, db)

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

	// Verify stale transaction was aborted
	verifyTransactionStatus(t, db, staleTxn.ID, TxnStatusAborted)

	// Verify active transaction is still pending
	verifyTransactionStatus(t, db, activeTxn.ID, TxnStatusPending)

	t.Log("✓ GC correctly distinguished stale vs active transactions")

	// Clean up
	tm.CommitTransaction(activeTxn)
}
