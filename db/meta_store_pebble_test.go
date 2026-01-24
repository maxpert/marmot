//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

func createTestPebbleMetaStore(t *testing.T) (*PebbleMetaStore, func()) {
	tmpDir, err := os.MkdirTemp("", "pebble_metastore_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	metaPath := filepath.Join(tmpDir, "test_meta.pebble")
	store, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32, // Smaller for tests
		MemTableSizeMB: 8,  // Smaller for tests
		MemTableCount:  2,
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create pebble meta store: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

func TestPebbleMetaStoreTransactionLifecycle(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
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
	if rec.Status != TxnStatusPending {
		t.Errorf("Expected status PENDING, got %s", rec.Status.String())
	}

	// Commit transaction
	commitTS := clock.Now()
	err = store.CommitTransaction(txnID, commitTS, []byte(`[{"sql":"INSERT INTO t VALUES(1)"}]`), "testdb", "t", 0, 1)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Verify transaction is COMMITTED
	rec, err = store.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("GetTransaction failed: %v", err)
	}
	if rec.Status != TxnStatusCommitted {
		t.Errorf("Expected status COMMITTED, got %s", rec.Status.String())
	}
	if rec.DatabaseName != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", rec.DatabaseName)
	}
}

func TestPebbleMetaStoreTransactionAbort(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
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

func TestPebbleMetaStoreWriteIntents(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()

	store.BeginTransaction(txnID, 1, startTS)

	// Write intent
	err := store.WriteIntent(txnID, IntentTypeDML, "users", "user:1", OpTypeInsert, "INSERT INTO users VALUES(1,'alice')", nil, startTS, 1)
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
	if intent.Operation != OpTypeInsert {
		t.Errorf("Expected operation INSERT, got %s", intent.Operation.String())
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

func TestPebbleMetaStoreWriteIntentConflict(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS1 := clock.Now()
	txnID1 := startTS1.ToTxnID()
	startTS2 := clock.Now()
	txnID2 := startTS2.ToTxnID()

	store.BeginTransaction(txnID1, 1, startTS1)
	store.BeginTransaction(txnID2, 1, startTS2)

	// First intent succeeds
	err := store.WriteIntent(txnID1, IntentTypeDML, "users", "user:1", OpTypeInsert, "INSERT INTO users VALUES(1)", nil, startTS1, 1)
	if err != nil {
		t.Fatalf("First WriteIntent failed: %v", err)
	}

	// Second intent on same row should fail with conflict
	err = store.WriteIntent(txnID2, IntentTypeDML, "users", "user:1", OpTypeUpdate, "UPDATE users SET name='bob' WHERE id=1", nil, startTS2, 1)
	if err == nil {
		t.Fatal("Expected write-write conflict error")
	}
	if !isWriteWriteConflict(err) {
		t.Errorf("Expected write-write conflict, got: %v", err)
	}
}

func TestPebbleMetaStoreReplicationState(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	peerNodeID := uint64(2)
	dbName := "testdb"
	clock := hlc.NewClock(1)
	ts := clock.Now()

	// Initially no state
	state, err := store.GetReplicationState(peerNodeID, dbName)
	if err != nil {
		t.Fatalf("GetReplicationState failed: %v", err)
	}
	if state != nil {
		t.Error("Expected no initial state")
	}

	// Update state
	err = store.UpdateReplicationState(peerNodeID, dbName, 100, ts)
	if err != nil {
		t.Fatalf("UpdateReplicationState failed: %v", err)
	}

	// Verify state
	state, err = store.GetReplicationState(peerNodeID, dbName)
	if err != nil {
		t.Fatalf("GetReplicationState failed: %v", err)
	}
	if state == nil {
		t.Fatal("State not found")
	}
	if state.LastAppliedTxnID != 100 {
		t.Errorf("Expected txn_id 100, got %d", state.LastAppliedTxnID)
	}
}

func TestPebbleMetaStoreCDCIntentEntries(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	txnID := uint64(12345)

	// Write CDC entries - the correct format is map[string][]byte
	oldVals := map[string][]byte{"id": {0, 0, 0, 1}, "name": []byte("alice")}
	newVals := map[string][]byte{"id": {0, 0, 0, 1}, "name": []byte("bob")}

	err := store.WriteIntentEntry(txnID, 1, 1, "users", "user:1", oldVals, newVals)
	if err != nil {
		t.Fatalf("WriteIntentEntry failed: %v", err)
	}

	err = store.WriteIntentEntry(txnID, 2, 0, "users", "user:2", nil, newVals)
	if err != nil {
		t.Fatalf("WriteIntentEntry failed: %v", err)
	}

	// Get entries
	entries, err := store.GetIntentEntries(txnID)
	if err != nil {
		t.Fatalf("GetIntentEntries failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	// Should be sorted by seq
	if entries[0].Seq != 1 {
		t.Errorf("Expected first entry seq 1, got %d", entries[0].Seq)
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

func TestPebbleMetaStoreSequenceNumbers(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	nodeID := uint64(1)

	// Get sequence numbers - should be monotonically increasing
	seq1, err := store.GetNextSeqNum(nodeID)
	if err != nil {
		t.Fatalf("GetNextSeqNum failed: %v", err)
	}

	seq2, err := store.GetNextSeqNum(nodeID)
	if err != nil {
		t.Fatalf("GetNextSeqNum failed: %v", err)
	}

	seq3, err := store.GetNextSeqNum(nodeID)
	if err != nil {
		t.Fatalf("GetNextSeqNum failed: %v", err)
	}

	if seq2 != seq1+1 {
		t.Errorf("Expected seq2 (%d) = seq1 (%d) + 1", seq2, seq1)
	}
	if seq3 != seq2+1 {
		t.Errorf("Expected seq3 (%d) = seq2 (%d) + 1", seq3, seq2)
	}
}

func TestPebbleMetaStoreCommitCounters(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()

	// Initial counts
	count, _ := store.GetCommittedTxnCount()
	if count != 0 {
		t.Errorf("Expected initial count 0, got %d", count)
	}

	// Begin and commit
	store.BeginTransaction(txnID, 1, startTS)
	store.CommitTransaction(txnID, clock.Now(), nil, "testdb", "", 0, 0)

	// Count should increase
	count, _ = store.GetCommittedTxnCount()
	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}

	maxTxnID, _ := store.GetMaxCommittedTxnID()
	if maxTxnID != txnID {
		t.Errorf("Expected max txn_id %d, got %d", txnID, maxTxnID)
	}
}

func TestPebbleMetaStoreSchemaVersion(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	dbName := "testdb"

	// Initial version
	version, err := store.GetSchemaVersion(dbName)
	if err != nil {
		t.Fatalf("GetSchemaVersion failed: %v", err)
	}
	if version != 0 {
		t.Errorf("Expected initial version 0, got %d", version)
	}

	// Update version
	err = store.UpdateSchemaVersion(dbName, 1, "CREATE TABLE users (id INT)", 100)
	if err != nil {
		t.Fatalf("UpdateSchemaVersion failed: %v", err)
	}

	version, err = store.GetSchemaVersion(dbName)
	if err != nil {
		t.Fatalf("GetSchemaVersion failed: %v", err)
	}
	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}
}

func TestPebbleMetaStoreDDLLock(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	dbName := "testdb"
	nodeID1 := uint64(1)
	nodeID2 := uint64(2)

	// First node acquires lock
	acquired, err := store.TryAcquireDDLLock(dbName, nodeID1, 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquireDDLLock failed: %v", err)
	}
	if !acquired {
		t.Error("Expected lock to be acquired")
	}

	// Second node can't acquire
	acquired, err = store.TryAcquireDDLLock(dbName, nodeID2, 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquireDDLLock failed: %v", err)
	}
	if acquired {
		t.Error("Second node should not acquire lock")
	}

	// Release lock
	err = store.ReleaseDDLLock(dbName, nodeID1)
	if err != nil {
		t.Fatalf("ReleaseDDLLock failed: %v", err)
	}

	// Now second node can acquire
	acquired, err = store.TryAcquireDDLLock(dbName, nodeID2, 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquireDDLLock failed: %v", err)
	}
	if !acquired {
		t.Error("Second node should acquire lock after release")
	}
}

func TestPebbleMetaStoreDeleteIntentsByTxn(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()

	store.BeginTransaction(txnID, 1, startTS)

	// Create multiple intents
	for i := 0; i < 5; i++ {
		err := store.WriteIntent(txnID, IntentTypeDML, "users", "user:"+string(rune('0'+i)), OpTypeInsert, "", nil, startTS, 1)
		if err != nil {
			t.Fatalf("WriteIntent failed: %v", err)
		}
	}

	// Get all intents
	intents, _ := store.GetIntentsByTxn(txnID)
	if len(intents) != 5 {
		t.Fatalf("Expected 5 intents, got %d", len(intents))
	}

	// Delete all
	err := store.DeleteIntentsByTxn(txnID)
	if err != nil {
		t.Fatalf("DeleteIntentsByTxn failed: %v", err)
	}

	intents, _ = store.GetIntentsByTxn(txnID)
	if len(intents) != 0 {
		t.Errorf("Expected 0 intents after delete, got %d", len(intents))
	}
}

func TestPebbleMetaStoreStoreReplayedTransaction(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)
	commitTS := clock.Now()
	txnID := commitTS.ToTxnID()
	nodeID := uint64(2)

	// Store a replayed transaction (no prior BeginTransaction)
	err := store.StoreReplayedTransaction(txnID, nodeID, commitTS, "testdb", 1)
	if err != nil {
		t.Fatalf("StoreReplayedTransaction failed: %v", err)
	}

	// Verify it's stored as COMMITTED
	rec, err := store.GetTransaction(txnID)
	if err != nil {
		t.Fatalf("GetTransaction failed: %v", err)
	}
	if rec == nil {
		t.Fatal("Transaction not found")
	}
	if rec.Status != TxnStatusCommitted {
		t.Errorf("Expected COMMITTED, got %s", rec.Status.String())
	}
	if rec.NodeID != nodeID {
		t.Errorf("Expected nodeID %d, got %d", nodeID, rec.NodeID)
	}
}

func TestPebbleMetaStoreStreamCommittedTransactions(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create and commit multiple transactions
	var txnIDs []uint64
	for i := 0; i < 5; i++ {
		startTS := clock.Now()
		txnID := startTS.ToTxnID()
		txnIDs = append(txnIDs, txnID)

		store.BeginTransaction(txnID, 1, startTS)
		store.CommitTransaction(txnID, clock.Now(), nil, "testdb", "", 0, 0)
	}

	// Stream all (from 0)
	var streamed []*TransactionRecord
	err := store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
		streamed = append(streamed, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamCommittedTransactions failed: %v", err)
	}
	if len(streamed) != 5 {
		t.Errorf("Expected 5 streamed, got %d", len(streamed))
	}

	// Stream from middle
	streamed = nil
	err = store.StreamCommittedTransactions(txnIDs[2], func(rec *TransactionRecord) error {
		streamed = append(streamed, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamCommittedTransactions failed: %v", err)
	}
	if len(streamed) != 2 {
		t.Errorf("Expected 2 streamed after txn[2], got %d", len(streamed))
	}
}

func TestBuildKeyUint64(t *testing.T) {
	prefix := "/test/"
	id := uint64(12345)

	key := buildKeyUint64(prefix, id)

	// Verify length
	expectedLen := len(prefix) + 8
	if len(key) != expectedLen {
		t.Errorf("Expected key length %d, got %d", expectedLen, len(key))
	}

	// Verify prefix
	if string(key[:len(prefix)]) != prefix {
		t.Errorf("Expected prefix '%s', got '%s'", prefix, string(key[:len(prefix)]))
	}

	// Verify uint64 encoding (big-endian)
	decodedID := binary.BigEndian.Uint64(key[len(prefix):])
	if decodedID != id {
		t.Errorf("Expected decoded ID %d, got %d", id, decodedID)
	}
}

func TestBuildKeyUint64x2(t *testing.T) {
	prefix := "/test/"
	id1 := uint64(111)
	id2 := uint64(222)

	key := buildKeyUint64x2(prefix, id1, id2)

	// Verify length
	expectedLen := len(prefix) + 16
	if len(key) != expectedLen {
		t.Errorf("Expected key length %d, got %d", expectedLen, len(key))
	}

	// Verify prefix
	if string(key[:len(prefix)]) != prefix {
		t.Errorf("Expected prefix '%s', got '%s'", prefix, string(key[:len(prefix)]))
	}

	// Verify first uint64
	decoded1 := binary.BigEndian.Uint64(key[len(prefix):])
	if decoded1 != id1 {
		t.Errorf("Expected first ID %d, got %d", id1, decoded1)
	}

	// Verify second uint64
	decoded2 := binary.BigEndian.Uint64(key[len(prefix)+8:])
	if decoded2 != id2 {
		t.Errorf("Expected second ID %d, got %d", id2, decoded2)
	}
}

func TestBuildKeyString(t *testing.T) {
	prefix := "/test/"
	suffix := "mydb"

	key := buildKeyString(prefix, suffix)

	// Verify length
	expectedLen := len(prefix) + len(suffix)
	if len(key) != expectedLen {
		t.Errorf("Expected key length %d, got %d", expectedLen, len(key))
	}

	// Verify full key
	expected := prefix + suffix
	if string(key) != expected {
		t.Errorf("Expected key '%s', got '%s'", expected, string(key))
	}
}

func TestPebbleKeyFunctionsProduceCorrectBytes(t *testing.T) {
	// Test that all key-building functions produce expected byte sequences
	tests := []struct {
		name     string
		fn       func() []byte
		expected []byte
	}{
		{
			name: "pebbleTxnKey",
			fn:   func() []byte { return pebbleTxnKey(12345) },
			expected: func() []byte {
				b := make([]byte, len(pebblePrefixTxn)+8)
				copy(b, pebblePrefixTxn)
				binary.BigEndian.PutUint64(b[len(pebblePrefixTxn):], 12345)
				return b
			}(),
		},
		{
			name: "pebbleTxnPendingKey",
			fn:   func() []byte { return pebbleTxnPendingKey(67890) },
			expected: func() []byte {
				b := make([]byte, len(pebblePrefixTxnPending)+8)
				copy(b, pebblePrefixTxnPending)
				binary.BigEndian.PutUint64(b[len(pebblePrefixTxnPending):], 67890)
				return b
			}(),
		},
		{
			name: "pebbleTxnSeqKey",
			fn:   func() []byte { return pebbleTxnSeqKey(111, 222) },
			expected: func() []byte {
				b := make([]byte, len(pebblePrefixTxnSeq)+16)
				copy(b, pebblePrefixTxnSeq)
				binary.BigEndian.PutUint64(b[len(pebblePrefixTxnSeq):], 111)
				binary.BigEndian.PutUint64(b[len(pebblePrefixTxnSeq)+8:], 222)
				return b
			}(),
		},
		{
			name: "pebbleCdcRawKey",
			fn:   func() []byte { return pebbleCdcRawKey(333, 444) },
			expected: func() []byte {
				b := make([]byte, len(pebblePrefixCDCRaw)+16)
				copy(b, pebblePrefixCDCRaw)
				binary.BigEndian.PutUint64(b[len(pebblePrefixCDCRaw):], 333)
				binary.BigEndian.PutUint64(b[len(pebblePrefixCDCRaw)+8:], 444)
				return b
			}(),
		},
		{
			name: "pebbleSchemaKey",
			fn:   func() []byte { return pebbleSchemaKey("testdb") },
			expected: func() []byte {
				b := make([]byte, len(pebblePrefixSchema)+len("testdb"))
				copy(b, pebblePrefixSchema)
				copy(b[len(pebblePrefixSchema):], "testdb")
				return b
			}(),
		},
		{
			name: "pebbleSeqKey",
			fn:   func() []byte { return pebbleSeqKey(555) },
			expected: func() []byte {
				b := make([]byte, len(pebblePrefixSeq)+8)
				copy(b, pebblePrefixSeq)
				binary.BigEndian.PutUint64(b[len(pebblePrefixSeq):], 555)
				return b
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fn()
			if len(got) != len(tt.expected) {
				t.Errorf("%s: length mismatch, expected %d, got %d", tt.name, len(tt.expected), len(got))
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("%s: byte mismatch at index %d, expected %d, got %d", tt.name, i, tt.expected[i], got[i])
				}
			}
		})
	}
}

func TestPebbleKeyEdgeCases(t *testing.T) {
	// Test edge cases
	t.Run("ZeroValues", func(t *testing.T) {
		key := buildKeyUint64("/prefix/", 0)
		decoded := binary.BigEndian.Uint64(key[len("/prefix/"):])
		if decoded != 0 {
			t.Errorf("Expected 0, got %d", decoded)
		}
	})

	t.Run("MaxUint64", func(t *testing.T) {
		maxID := ^uint64(0)
		key := buildKeyUint64("/prefix/", maxID)
		decoded := binary.BigEndian.Uint64(key[len("/prefix/"):])
		if decoded != maxID {
			t.Errorf("Expected max uint64 %d, got %d", maxID, decoded)
		}
	})

	t.Run("EmptyString", func(t *testing.T) {
		key := buildKeyString("/prefix/", "")
		if len(key) != len("/prefix/") {
			t.Errorf("Expected length %d, got %d", len("/prefix/"), len(key))
		}
		if string(key) != "/prefix/" {
			t.Errorf("Expected '%s', got '%s'", "/prefix/", string(key))
		}
	})

	t.Run("LexicographicOrdering", func(t *testing.T) {
		// Big-endian preserves lexicographic ordering for range scans
		key1 := buildKeyUint64("/prefix/", 100)
		key2 := buildKeyUint64("/prefix/", 200)
		key3 := buildKeyUint64("/prefix/", 300)

		// Verify byte-level ordering
		if string(key1) >= string(key2) {
			t.Error("key1 should be < key2 in lexicographic order")
		}
		if string(key2) >= string(key3) {
			t.Error("key2 should be < key3 in lexicographic order")
		}
	})
}
