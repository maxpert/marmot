//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/vmihailenco/msgpack/v5"
)

func createTestMetaStore(t *testing.T) (*BadgerMetaStore, func()) {
	tmpDir, err := os.MkdirTemp("", "metastore_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	metaPath := filepath.Join(tmpDir, "test_meta.badger")
	store, err := NewBadgerMetaStore(metaPath, BadgerMetaStoreOptions{
		SyncWrites:    false, // Faster for tests
		NumCompactors: 2,
		ValueLogGC:    false,
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create meta store: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

func TestMetaStoreTransactionLifecycle(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()
	nodeID := uint64(1)

	// Begin transaction
	err := store.BeginTransaction(txnID, nodeID, startTS)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Verify transaction exists with PENDING status
	rec, err := store.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("GetTransaction failed: %v", err)
	}
	if rec == nil {
		t.Fatal("Transaction not found")
	}
	if rec.Status != MetaTxnStatusPending {
		t.Errorf("Expected status PENDING, got %s", rec.Status)
	}

	// Commit transaction
	commitTS := clock.Now()
	err = store.CommitTransaction(txnID, commitTS, []byte(`[{"sql":"INSERT INTO t VALUES(1)"}]`), "testdb")
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Verify transaction is COMMITTED
	rec, err = store.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("GetTransaction failed: %v", err)
	}
	if rec.Status != MetaTxnStatusCommitted {
		t.Errorf("Expected status COMMITTED, got %s", rec.Status)
	}
	if rec.DatabaseName != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", rec.DatabaseName)
	}
}

func TestMetaStoreTransactionAbort(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()

	err := store.BeginTransaction(txnID, 1, startTS)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	err = store.AbortTransaction(txnID)
	if err != nil {
		t.Fatalf("AbortTransaction failed: %v", err)
	}

	rec, err := store.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("GetTransaction failed: %v", err)
	}
	if rec != nil {
		t.Error("Transaction should be deleted after abort")
	}
}

func TestMetaStoreWriteIntents(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()

	store.BeginTransaction(txnID, 1, startTS)

	// Write intent
	err := store.WriteIntent(txnID, "users", "user:1", "INSERT", "INSERT INTO users VALUES(1,'alice')", nil, startTS, 1)
	if err != nil {
		t.Fatalf("WriteIntent failed: %v", err)
	}

	// Validate intent
	valid, err := store.ValidateIntent("users", "user:1", txnID)
	if err != nil {
		t.Fatalf("ValidateIntent failed: %v", err)
	}
	if !valid {
		t.Error("Intent should be valid")
	}

	// Get intent
	intent, err := store.GetIntent("users", "user:1")
	if err != nil {
		t.Fatalf("GetIntent failed: %v", err)
	}
	if intent == nil {
		t.Fatal("Intent not found")
	}
	if intent.Operation != "INSERT" {
		t.Errorf("Expected operation INSERT, got %s", intent.Operation)
	}

	// Validate with wrong txn_id
	valid, err = store.ValidateIntent("users", "user:1", txnID+1)
	if err != nil {
		t.Fatalf("ValidateIntent failed: %v", err)
	}
	if valid {
		t.Error("Intent should not be valid for different txn_id")
	}

	// Delete intent
	err = store.DeleteIntent("users", "user:1", txnID)
	if err != nil {
		t.Fatalf("DeleteIntent failed: %v", err)
	}

	intent, err = store.GetIntent("users", "user:1")
	if err != nil {
		t.Fatalf("GetIntent failed: %v", err)
	}
	if intent != nil {
		t.Error("Intent should be deleted")
	}
}

func TestMetaStoreWriteIntentConflict(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS1 := clock.Now()
	txnID1 := startTS1.ToTxnID()
	startTS2 := clock.Now()
	txnID2 := startTS2.ToTxnID()

	store.BeginTransaction(txnID1, 1, startTS1)
	store.BeginTransaction(txnID2, 1, startTS2)

	// First intent succeeds
	err := store.WriteIntent(txnID1, "users", "user:1", "INSERT", "INSERT INTO users VALUES(1)", nil, startTS1, 1)
	if err != nil {
		t.Fatalf("First WriteIntent failed: %v", err)
	}

	// Second intent on same row should fail with conflict
	err = store.WriteIntent(txnID2, "users", "user:1", "UPDATE", "UPDATE users SET name='bob' WHERE id=1", nil, startTS2, 1)
	if err == nil {
		t.Fatal("Expected write-write conflict error")
	}

	// Same transaction updating its own intent should succeed
	err = store.WriteIntent(txnID1, "users", "user:1", "UPDATE", "UPDATE users SET name='alice2' WHERE id=1", nil, startTS1, 1)
	if err != nil {
		t.Fatalf("Same transaction update failed: %v", err)
	}
}

func TestMetaStoreMVCCVersions(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	ts1 := clock.Now()
	ts2 := clock.Now()

	// Create version 1
	err := store.CreateMVCCVersion("users", "user:1", ts1, 1, 100, "INSERT", []byte(`{"name":"alice"}`))
	if err != nil {
		t.Fatalf("CreateMVCCVersion failed: %v", err)
	}

	// Create version 2
	err = store.CreateMVCCVersion("users", "user:1", ts2, 1, 101, "UPDATE", []byte(`{"name":"alice2"}`))
	if err != nil {
		t.Fatalf("CreateMVCCVersion failed: %v", err)
	}

	// Get latest should return version 2
	ver, err := store.GetLatestVersion("users", "user:1")
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if ver == nil {
		t.Fatal("Version not found")
	}
	if ver.TxnID != 101 {
		t.Errorf("Expected txn_id 101, got %d", ver.TxnID)
	}
	if ver.Operation != "UPDATE" {
		t.Errorf("Expected operation UPDATE, got %s", ver.Operation)
	}
}

func TestMetaStoreReplicationState(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	ts := clock.Now()

	// Update replication state
	err := store.UpdateReplicationState(2, "mydb", 100, ts)
	if err != nil {
		t.Fatalf("UpdateReplicationState failed: %v", err)
	}

	// Get replication state
	state, err := store.GetReplicationState(2, "mydb")
	if err != nil {
		t.Fatalf("GetReplicationState failed: %v", err)
	}
	if state == nil {
		t.Fatal("State not found")
	}
	if state.LastAppliedTxnID != 100 {
		t.Errorf("Expected last_applied_txn_id 100, got %d", state.LastAppliedTxnID)
	}

	// Get min applied txn_id
	minTxnID, err := store.GetMinAppliedTxnID("mydb")
	if err != nil {
		t.Fatalf("GetMinAppliedTxnID failed: %v", err)
	}
	if minTxnID != 100 {
		t.Errorf("Expected min 100, got %d", minTxnID)
	}
}

func TestMetaStoreSchemaVersion(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	// Initial version should be 0
	ver, err := store.GetSchemaVersion("mydb")
	if err != nil {
		t.Fatalf("GetSchemaVersion failed: %v", err)
	}
	if ver != 0 {
		t.Errorf("Expected version 0, got %d", ver)
	}

	// Update version
	err = store.UpdateSchemaVersion("mydb", 1, "CREATE TABLE users(id INT)", 100)
	if err != nil {
		t.Fatalf("UpdateSchemaVersion failed: %v", err)
	}

	ver, err = store.GetSchemaVersion("mydb")
	if err != nil {
		t.Fatalf("GetSchemaVersion failed: %v", err)
	}
	if ver != 1 {
		t.Errorf("Expected version 1, got %d", ver)
	}
}

func TestMetaStoreDDLLock(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	// Acquire lock
	acquired, err := store.TryAcquireDDLLock("mydb", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("TryAcquireDDLLock failed: %v", err)
	}
	if !acquired {
		t.Error("Should acquire lock")
	}

	// Second acquire should fail (lock held)
	acquired, err = store.TryAcquireDDLLock("mydb", 2, 5*time.Second)
	if err != nil {
		t.Fatalf("TryAcquireDDLLock failed: %v", err)
	}
	if acquired {
		t.Error("Should not acquire lock held by another node")
	}

	// Release lock
	err = store.ReleaseDDLLock("mydb", 1)
	if err != nil {
		t.Fatalf("ReleaseDDLLock failed: %v", err)
	}

	// Now second node can acquire
	acquired, err = store.TryAcquireDDLLock("mydb", 2, 5*time.Second)
	if err != nil {
		t.Fatalf("TryAcquireDDLLock failed: %v", err)
	}
	if !acquired {
		t.Error("Should acquire released lock")
	}
}

func TestMetaStoreIntentEntries(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	txnID := uint64(12345)

	// Prepare msgpack-encoded values
	aliceVals := map[string]interface{}{"name": "alice"}
	bobVals := map[string]interface{}{"name": "bob"}
	aliceBytes, _ := msgpack.Marshal(aliceVals)
	bobBytes, _ := msgpack.Marshal(bobVals)

	// Write entries
	err := store.WriteIntentEntry(txnID, 1, 0, "users", "user:1", nil, aliceBytes)
	if err != nil {
		t.Fatalf("WriteIntentEntry failed: %v", err)
	}
	err = store.WriteIntentEntry(txnID, 2, 1, "users", "user:1", aliceBytes, bobBytes)
	if err != nil {
		t.Fatalf("WriteIntentEntry failed: %v", err)
	}

	// Get entries
	entries, err := store.GetIntentEntries(txnID)
	if err != nil {
		t.Fatalf("GetIntentEntries failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}
	if entries[0].Seq != 1 {
		t.Errorf("Expected seq 1, got %d", entries[0].Seq)
	}
	if entries[1].Seq != 2 {
		t.Errorf("Expected seq 2, got %d", entries[1].Seq)
	}

	// Delete entries
	err = store.DeleteIntentEntries(txnID)
	if err != nil {
		t.Fatalf("DeleteIntentEntries failed: %v", err)
	}

	entries, err = store.GetIntentEntries(txnID)
	if err != nil {
		t.Fatalf("GetIntentEntries failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after delete, got %d", len(entries))
	}
}

func TestMetaStoreGCStaleTransactions(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create old transaction with old heartbeat
	oldTS := clock.Now()
	oldTxnID := oldTS.ToTxnID()
	store.BeginTransaction(oldTxnID, 1, oldTS)

	// Wait to ensure the transaction ages
	time.Sleep(50 * time.Millisecond)

	// Create recent transaction
	newTS := clock.Now()
	newTxnID := newTS.ToTxnID()
	store.BeginTransaction(newTxnID, 1, newTS)

	// GC with very short timeout (10ms) - old transaction should be cleaned
	cleaned, err := store.CleanupStaleTransactions(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("CleanupStaleTransactions failed: %v", err)
	}
	if cleaned != 1 {
		t.Errorf("Expected 1 cleaned, got %d", cleaned)
	}

	// Old transaction should be gone
	rec, _ := store.GetTransaction(oldTxnID)
	if rec != nil {
		t.Error("Old transaction should be cleaned up")
	}

	// New transaction should still exist (we just created it)
	rec, _ = store.GetTransaction(newTxnID)
	if rec == nil {
		t.Error("New transaction should still exist")
	}
}

func BenchmarkMetaStoreWriteIntent(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "metastore_bench")
	defer os.RemoveAll(tmpDir)

	metaPath := filepath.Join(tmpDir, "bench_meta.badger")
	store, _ := NewBadgerMetaStore(metaPath, BadgerMetaStoreOptions{
		SyncWrites:    false,
		NumCompactors: 2,
		ValueLogGC:    false,
	})
	defer store.Close()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()
	store.BeginTransaction(txnID, 1, startTS)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowKey := "user:" + string(rune(i))
		store.WriteIntent(txnID, "users", rowKey, "INSERT", "INSERT INTO users VALUES(?)", nil, startTS, 1)
	}
}

func BenchmarkMetaStoreBeginCommit(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "metastore_bench")
	defer os.RemoveAll(tmpDir)

	metaPath := filepath.Join(tmpDir, "bench_meta.badger")
	store, _ := NewBadgerMetaStore(metaPath, BadgerMetaStoreOptions{
		SyncWrites:    false,
		NumCompactors: 2,
		ValueLogGC:    false,
	})
	defer store.Close()

	clock := hlc.NewClock(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startTS := clock.Now()
		txnID := startTS.ToTxnID()
		store.BeginTransaction(txnID, 1, startTS)
		commitTS := clock.Now()
		store.CommitTransaction(txnID, commitTS, nil, "testdb")
	}
}

func TestExtractMVCCRowPrefix(t *testing.T) {
	prefixLen := len("/mvcc/")

	tests := []struct {
		key      string
		expected string
	}{
		{"/mvcc/users/user:1/0000000000000001000000010000000000000001", "/mvcc/users/user:1/"},
		{"/mvcc/orders/order:123/0000000000000002000000020000000000000002", "/mvcc/orders/order:123/"},
		{"/mvcc/t/r/ts", "/mvcc/t/r/"},
		{"/mvcc/table-with-dash/row-key/ts", "/mvcc/table-with-dash/row-key/"},
		{"/mvcc/bad", ""},           // Missing rowKey and timestamp
		{"/mvcc/table/", ""},        // Missing rowKey
		{"/mvcc/table/row", ""},     // Missing timestamp
	}

	for _, tc := range tests {
		result := extractMVCCRowPrefix(tc.key, prefixLen)
		if result != tc.expected {
			t.Errorf("extractMVCCRowPrefix(%q) = %q, expected %q", tc.key, result, tc.expected)
		}
	}
}

func TestCleanupOldMVCCVersions_Basic(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create 5 versions for row1
	for i := 0; i < 5; i++ {
		ts := clock.Now()
		err := store.CreateMVCCVersion("users", "user:1", ts, 1, uint64(100+i), "UPDATE", []byte(`{"v":`+string(rune('0'+i))+`}`))
		if err != nil {
			t.Fatalf("CreateMVCCVersion failed: %v", err)
		}
	}

	// Create 3 versions for row2
	for i := 0; i < 3; i++ {
		ts := clock.Now()
		err := store.CreateMVCCVersion("users", "user:2", ts, 1, uint64(200+i), "UPDATE", nil)
		if err != nil {
			t.Fatalf("CreateMVCCVersion failed: %v", err)
		}
	}

	// Keep 2 versions - should delete 3 from row1, 1 from row2
	deleted, err := store.CleanupOldMVCCVersions(2)
	if err != nil {
		t.Fatalf("CleanupOldMVCCVersions failed: %v", err)
	}
	if deleted != 4 {
		t.Errorf("Expected 4 deleted, got %d", deleted)
	}

	// Verify row1 has 2 versions (latest two)
	ver, err := store.GetLatestVersion("users", "user:1")
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if ver == nil {
		t.Fatal("Latest version should exist")
	}
	if ver.TxnID != 104 {
		t.Errorf("Expected latest version txnID 104, got %d", ver.TxnID)
	}
}

func TestCleanupOldMVCCVersions_KeepsNewest(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create 10 versions - timestamps increase
	for i := 0; i < 10; i++ {
		ts := clock.Now()
		err := store.CreateMVCCVersion("test", "row:1", ts, 1, uint64(i+1), "UPDATE", nil)
		if err != nil {
			t.Fatalf("CreateMVCCVersion failed: %v", err)
		}
	}

	// Keep 3 versions
	deleted, err := store.CleanupOldMVCCVersions(3)
	if err != nil {
		t.Fatalf("CleanupOldMVCCVersions failed: %v", err)
	}
	if deleted != 7 {
		t.Errorf("Expected 7 deleted, got %d", deleted)
	}

	// Latest version should be txnID 10 (newest)
	ver, err := store.GetLatestVersion("test", "row:1")
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if ver.TxnID != 10 {
		t.Errorf("Expected txnID 10, got %d", ver.TxnID)
	}
}

func TestCleanupOldMVCCVersions_NoDelete(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create 2 versions
	for i := 0; i < 2; i++ {
		ts := clock.Now()
		store.CreateMVCCVersion("test", "row:1", ts, 1, uint64(i+1), "UPDATE", nil)
	}

	// Keep 5 versions - nothing should be deleted
	deleted, err := store.CleanupOldMVCCVersions(5)
	if err != nil {
		t.Fatalf("CleanupOldMVCCVersions failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("Expected 0 deleted, got %d", deleted)
	}
}

func TestCleanupOldMVCCVersions_MultipleRows(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create versions for multiple tables and rows
	tables := []string{"users", "orders", "items"}
	rows := []string{"row:1", "row:2", "row:3"}

	for _, table := range tables {
		for _, row := range rows {
			for i := 0; i < 5; i++ {
				ts := clock.Now()
				store.CreateMVCCVersion(table, row, ts, 1, uint64(i+1), "UPDATE", nil)
			}
		}
	}

	// Keep 2 versions per row - should delete 3 * 3 * 3 = 27 versions
	deleted, err := store.CleanupOldMVCCVersions(2)
	if err != nil {
		t.Fatalf("CleanupOldMVCCVersions failed: %v", err)
	}
	// 9 rows * (5 - 2) = 27 deleted
	if deleted != 27 {
		t.Errorf("Expected 27 deleted, got %d", deleted)
	}
}

func TestCleanupOldMVCCVersions_Empty(t *testing.T) {
	store, cleanup := createTestMetaStore(t)
	defer cleanup()

	// No versions - should return 0
	deleted, err := store.CleanupOldMVCCVersions(2)
	if err != nil {
		t.Fatalf("CleanupOldMVCCVersions failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("Expected 0 deleted, got %d", deleted)
	}
}

func BenchmarkCleanupOldMVCCVersions(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "metastore_bench")
	defer os.RemoveAll(tmpDir)

	metaPath := filepath.Join(tmpDir, "bench_meta.badger")
	store, _ := NewBadgerMetaStore(metaPath, BadgerMetaStoreOptions{
		SyncWrites:    false,
		NumCompactors: 2,
		ValueLogGC:    false,
	})
	defer store.Close()

	clock := hlc.NewClock(1)

	// Create 1000 rows with 10 versions each
	for row := 0; row < 1000; row++ {
		for ver := 0; ver < 10; ver++ {
			ts := clock.Now()
			store.CreateMVCCVersion("bench", "row:"+string(rune(row)), ts, 1, uint64(ver+1), "UPDATE", nil)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.CleanupOldMVCCVersions(5)
	}
}
