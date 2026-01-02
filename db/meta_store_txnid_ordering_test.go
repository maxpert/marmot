//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog"
)

func init() {
	// Respect LOG_LEVEL environment variable for tests
	// Valid levels: trace, debug, info, warn, error, fatal, panic, disabled
	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "error" // Default to error for tests to avoid noise
	}

	lvl, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil {
		lvl = zerolog.ErrorLevel
	}
	zerolog.SetGlobalLevel(lvl)
}

// TestTxnIDOrdering_OutOfOrderCommits verifies that StreamCommittedTransactions
// returns transactions in TxnID order, not SeqNum (commit) order.
//
// This is the critical bug case:
// - TxnA starts first, gets higher TxnID (HLC timestamp)
// - TxnB starts second, gets lower TxnID (e.g., from different node with clock skew)
// - TxnB commits first (SeqNum=1)
// - TxnA commits second (SeqNum=2)
//
// Current bug: Iteration by SeqNum returns TxnB, TxnA - but filtering by TxnID
// causes issues on reconnect.
// Fix: Iteration should be by TxnID so order matches filter criteria.
func TestTxnIDOrdering_OutOfOrderCommits(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	// We'll manually set TxnIDs to simulate out-of-order scenario
	// TxnA has higher TxnID but commits second
	// TxnB has lower TxnID but commits first

	txnIDHigh := uint64(1000) // TxnA - higher ID, commits second
	txnIDLow := uint64(900)   // TxnB - lower ID, commits first

	clock := hlc.NewClock(1)

	// Begin both transactions
	startTSHigh := clock.Now()
	err := store.BeginTransaction(txnIDHigh, 1, startTSHigh)
	if err != nil {
		t.Fatalf("BeginTransaction for txnIDHigh failed: %v", err)
	}

	startTSLow := clock.Now()
	err = store.BeginTransaction(txnIDLow, 2, startTSLow)
	if err != nil {
		t.Fatalf("BeginTransaction for txnIDLow failed: %v", err)
	}

	// Commit in reverse TxnID order: low TxnID commits first (gets SeqNum=1)
	commitTSLow := clock.Now()
	err = store.CommitTransaction(txnIDLow, commitTSLow, nil, "testdb", "", 0, 0)
	if err != nil {
		t.Fatalf("CommitTransaction for txnIDLow failed: %v", err)
	}

	// High TxnID commits second (gets SeqNum=2)
	commitTSHigh := clock.Now()
	err = store.CommitTransaction(txnIDHigh, commitTSHigh, nil, "testdb", "", 0, 0)
	if err != nil {
		t.Fatalf("CommitTransaction for txnIDHigh failed: %v", err)
	}

	// Stream all transactions
	var streamed []uint64
	err = store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
		streamed = append(streamed, rec.TxnID)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamCommittedTransactions failed: %v", err)
	}

	// Verify we got both transactions
	if len(streamed) != 2 {
		t.Fatalf("Expected 2 transactions, got %d", len(streamed))
	}

	// CRITICAL TEST: Transactions should be returned in TxnID order (900, 1000)
	// NOT in SeqNum/commit order (which would be 900, 1000 anyway in this case)
	// The real test is the filter behavior in subsequent tests
	if streamed[0] != txnIDLow {
		t.Errorf("First transaction should have lower TxnID (%d), got %d", txnIDLow, streamed[0])
	}
	if streamed[1] != txnIDHigh {
		t.Errorf("Second transaction should have higher TxnID (%d), got %d", txnIDHigh, streamed[1])
	}
}

// TestTxnIDOrdering_ReconnectAfterOutOfOrder is THE critical bug test case.
//
// Scenario:
//  1. TxnA (TxnID=1000) commits first, gets SeqNum=1
//  2. TxnB (TxnID=900) commits second, gets SeqNum=2
//  3. Client streams from 0, receives both in some order
//  4. Client disconnects after receiving TxnB (last TxnID=900)
//  5. Client reconnects with fromTxnID=900
//  6. BUG: With SeqNum iteration, iterator starts at SeqNum=1, sees TxnA (1000 > 900 = include),
//     but TxnB (SeqNum=2) has TxnID=900 which is <= fromTxnID=900, so it's skipped.
//     This is correct behavior IF iteration were by TxnID.
//  7. BUT: If iteration is by SeqNum and TxnA has SeqNum=1 and commits AFTER TxnB's
//     SeqNum=2 was created... wait, that's not possible with current code.
//
// The ACTUAL bug is more subtle:
// - SeqNum iteration order: SeqNum=1 (TxnA=1000), SeqNum=2 (TxnB=900)
// - Filter: TxnID > 900
// - Result: TxnA (1000 > 900) is included, TxnB (900 <= 900) is skipped
// - Correct! But what if we reconnect at 1000?
//
// Let me re-read the bug description... The issue is:
// "When a transaction with lower TxnID commits after one with higher TxnID"
//
// So the scenario is:
// 1. TxnA (TxnID=1000) commits first -> SeqNum=1
// 2. TxnB (TxnID=900) commits second -> SeqNum=2
//
// When streaming from fromTxnID=0:
// - Iterate by SeqNum: first see SeqNum=1 (TxnID=1000), then SeqNum=2 (TxnID=900)
// - Filter: TxnID > 0
// - Both pass, returned in order: 1000, 900 (by SeqNum order)
//
// Client receives 1000 first, then 900. Client's last received is 900.
// Client reconnects with fromTxnID=900.
//
// When streaming from fromTxnID=900:
// - Iterate by SeqNum: first see SeqNum=1 (TxnID=1000)
// - Filter: TxnID > 900 -> 1000 > 900 = true, include
// - Then SeqNum=2 (TxnID=900)
// - Filter: TxnID > 900 -> 900 > 900 = false, skip
//
// Result: Returns TxnID=1000 again! Client already received it!
//
// The FIX should iterate by TxnID so:
// - fromTxnID=900 would start iteration at TxnID=901
// - Only TxnID=1000 would be returned, which client didn't receive yet...
// - Wait, client DID receive 1000 first, then 900.
//
// The issue is that with SeqNum ordering, the "last received" doesn't work
// because IDs aren't monotonic with iteration order.
//
// Correct behavior with TxnID ordering:
// - Stream from 0: returns 900, 1000 (TxnID order)
// - Client receives 900, then 1000. Last received = 1000.
// - Client reconnects with fromTxnID=1000
// - No more transactions > 1000, returns nothing. Correct!
func TestTxnIDOrdering_ReconnectAfterOutOfOrder(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// TxnA: Higher TxnID, commits FIRST (gets lower SeqNum)
	txnIDHigh := uint64(1000)
	startTSHigh := clock.Now()
	store.BeginTransaction(txnIDHigh, 1, startTSHigh)
	store.CommitTransaction(txnIDHigh, clock.Now(), nil, "testdb", "", 0, 0)

	// TxnB: Lower TxnID, commits SECOND (gets higher SeqNum)
	txnIDLow := uint64(900)
	startTSLow := clock.Now()
	store.BeginTransaction(txnIDLow, 2, startTSLow)
	store.CommitTransaction(txnIDLow, clock.Now(), nil, "testdb", "", 0, 0)

	// First stream: Get all transactions from 0
	var firstStream []uint64
	err := store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
		firstStream = append(firstStream, rec.TxnID)
		return nil
	})
	if err != nil {
		t.Fatalf("First StreamCommittedTransactions failed: %v", err)
	}

	if len(firstStream) != 2 {
		t.Fatalf("Expected 2 transactions in first stream, got %d", len(firstStream))
	}

	// EXPECTED with TxnID ordering: [900, 1000] - ascending TxnID order
	// BUGGY with SeqNum ordering: [1000, 900] - SeqNum order (1000 committed first)
	expectedOrder := []uint64{txnIDLow, txnIDHigh} // [900, 1000]

	t.Run("FirstStreamOrder", func(t *testing.T) {
		for i, expected := range expectedOrder {
			if firstStream[i] != expected {
				t.Errorf("First stream index %d: expected TxnID %d, got %d", i, expected, firstStream[i])
			}
		}
	})

	// Simulate reconnect: last received was the highest TxnID in the stream
	// With correct TxnID ordering, last received should be 1000
	// With buggy SeqNum ordering, last received would be 900 (last in iteration)
	t.Run("ReconnectFromLastReceived", func(t *testing.T) {
		// With TxnID ordering: last received = 1000, reconnect with fromTxnID=1000
		// Should return nothing (no TxnID > 1000)
		var reconnectStream []uint64
		err := store.StreamCommittedTransactions(txnIDHigh, func(rec *TransactionRecord) error {
			reconnectStream = append(reconnectStream, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("Reconnect StreamCommittedTransactions failed: %v", err)
		}

		if len(reconnectStream) != 0 {
			t.Errorf("Expected 0 transactions after reconnect from TxnID=%d, got %d: %v",
				txnIDHigh, len(reconnectStream), reconnectStream)
		}
	})

	t.Run("ReconnectFromMiddle", func(t *testing.T) {
		// Reconnect from TxnID=900 should return only TxnID=1000
		var reconnectStream []uint64
		err := store.StreamCommittedTransactions(txnIDLow, func(rec *TransactionRecord) error {
			reconnectStream = append(reconnectStream, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("Reconnect StreamCommittedTransactions failed: %v", err)
		}

		if len(reconnectStream) != 1 {
			t.Fatalf("Expected 1 transaction after reconnect from TxnID=%d, got %d: %v",
				txnIDLow, len(reconnectStream), reconnectStream)
		}

		if reconnectStream[0] != txnIDHigh {
			t.Errorf("Expected TxnID=%d after reconnect, got %d", txnIDHigh, reconnectStream[0])
		}
	})
}

// TestTxnIDOrdering_FilterCorrectness verifies fromTxnID filter works correctly
// with multiple transactions.
func TestTxnIDOrdering_FilterCorrectness(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create transactions with specific TxnIDs (100, 200, 300, 400, 500)
	// Commit them in random order to test that iteration is by TxnID
	txnIDs := []uint64{300, 100, 500, 200, 400} // Commit order
	sortedTxnIDs := []uint64{100, 200, 300, 400, 500}

	for _, txnID := range txnIDs {
		startTS := clock.Now()
		err := store.BeginTransaction(txnID, 1, startTS)
		if err != nil {
			t.Fatalf("BeginTransaction for txnID=%d failed: %v", txnID, err)
		}
		err = store.CommitTransaction(txnID, clock.Now(), nil, "testdb", "", 0, 0)
		if err != nil {
			t.Fatalf("CommitTransaction for txnID=%d failed: %v", txnID, err)
		}
	}

	t.Run("FromZero_ReturnsAll", func(t *testing.T) {
		var streamed []uint64
		err := store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
			streamed = append(streamed, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("StreamCommittedTransactions failed: %v", err)
		}

		if len(streamed) != 5 {
			t.Fatalf("Expected 5 transactions, got %d", len(streamed))
		}

		// Should be in TxnID order
		for i, expected := range sortedTxnIDs {
			if streamed[i] != expected {
				t.Errorf("Index %d: expected TxnID %d, got %d", i, expected, streamed[i])
			}
		}
	})

	t.Run("FromMiddle_ReturnsSubset", func(t *testing.T) {
		// fromTxnID=200 should return 300, 400, 500
		var streamed []uint64
		err := store.StreamCommittedTransactions(200, func(rec *TransactionRecord) error {
			streamed = append(streamed, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("StreamCommittedTransactions failed: %v", err)
		}

		expected := []uint64{300, 400, 500}
		if len(streamed) != len(expected) {
			t.Fatalf("Expected %d transactions, got %d: %v", len(expected), len(streamed), streamed)
		}

		for i, exp := range expected {
			if streamed[i] != exp {
				t.Errorf("Index %d: expected TxnID %d, got %d", i, exp, streamed[i])
			}
		}
	})

	t.Run("FromLast_ReturnsNothing", func(t *testing.T) {
		// fromTxnID=500 should return nothing
		var streamed []uint64
		err := store.StreamCommittedTransactions(500, func(rec *TransactionRecord) error {
			streamed = append(streamed, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("StreamCommittedTransactions failed: %v", err)
		}

		if len(streamed) != 0 {
			t.Errorf("Expected 0 transactions from TxnID=500, got %d: %v", len(streamed), streamed)
		}
	})

	t.Run("FromBeyondLast_ReturnsNothing", func(t *testing.T) {
		// fromTxnID=1000 should return nothing
		var streamed []uint64
		err := store.StreamCommittedTransactions(1000, func(rec *TransactionRecord) error {
			streamed = append(streamed, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("StreamCommittedTransactions failed: %v", err)
		}

		if len(streamed) != 0 {
			t.Errorf("Expected 0 transactions from TxnID=1000, got %d: %v", len(streamed), streamed)
		}
	})
}

// TestTxnIDOrdering_ConcurrentCoordinators simulates the real-world scenario
// where different nodes (coordinators) have clock skew, causing TxnIDs to be
// generated out of order relative to commit order.
func TestTxnIDOrdering_ConcurrentCoordinators(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Simulate scenario:
	// - Node1 coordinator with "ahead" clock generates higher TxnIDs
	// - Node2 coordinator with "behind" clock generates lower TxnIDs
	// - Commits happen in arbitrary order

	type txn struct {
		txnID  uint64
		nodeID uint64
	}

	// Commit order: 1000, 800, 1200, 850
	// TxnID order should be: 800, 850, 1000, 1200
	transactions := []txn{
		{txnID: 1000, nodeID: 1}, // Node1, ahead clock
		{txnID: 800, nodeID: 2},  // Node2, behind clock
		{txnID: 1200, nodeID: 1}, // Node1, ahead clock
		{txnID: 850, nodeID: 2},  // Node2, behind clock
	}

	// Begin and commit in order
	for _, tx := range transactions {
		startTS := clock.Now()
		err := store.BeginTransaction(tx.txnID, tx.nodeID, startTS)
		if err != nil {
			t.Fatalf("BeginTransaction for txnID=%d failed: %v", tx.txnID, err)
		}
		err = store.CommitTransaction(tx.txnID, clock.Now(), nil, "testdb", "", 0, 0)
		if err != nil {
			t.Fatalf("CommitTransaction for txnID=%d failed: %v", tx.txnID, err)
		}
	}

	t.Run("IterationOrder", func(t *testing.T) {
		var streamed []uint64
		err := store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
			streamed = append(streamed, rec.TxnID)
			return nil
		})
		if err != nil {
			t.Fatalf("StreamCommittedTransactions failed: %v", err)
		}

		// Should be in TxnID order: 800, 850, 1000, 1200
		expected := []uint64{800, 850, 1000, 1200}
		if len(streamed) != len(expected) {
			t.Fatalf("Expected %d transactions, got %d: %v", len(expected), len(streamed), streamed)
		}

		for i, exp := range expected {
			if streamed[i] != exp {
				t.Errorf("Index %d: expected TxnID %d, got %d", i, exp, streamed[i])
			}
		}
	})

	t.Run("ReconnectScenarios", func(t *testing.T) {
		testCases := []struct {
			fromTxnID uint64
			expected  []uint64
		}{
			{fromTxnID: 0, expected: []uint64{800, 850, 1000, 1200}},
			{fromTxnID: 800, expected: []uint64{850, 1000, 1200}},
			{fromTxnID: 850, expected: []uint64{1000, 1200}},
			{fromTxnID: 1000, expected: []uint64{1200}},
			{fromTxnID: 1200, expected: []uint64{}},
			{fromTxnID: 1100, expected: []uint64{1200}},                // Between existing TxnIDs
			{fromTxnID: 700, expected: []uint64{800, 850, 1000, 1200}}, // Before all
		}

		for _, tc := range testCases {
			t.Run("", func(t *testing.T) {
				var streamed []uint64
				err := store.StreamCommittedTransactions(tc.fromTxnID, func(rec *TransactionRecord) error {
					streamed = append(streamed, rec.TxnID)
					return nil
				})
				if err != nil {
					t.Fatalf("StreamCommittedTransactions(fromTxnID=%d) failed: %v", tc.fromTxnID, err)
				}

				if len(streamed) != len(tc.expected) {
					t.Fatalf("fromTxnID=%d: expected %d transactions, got %d: %v",
						tc.fromTxnID, len(tc.expected), len(streamed), streamed)
				}

				for i, exp := range tc.expected {
					if streamed[i] != exp {
						t.Errorf("fromTxnID=%d, index %d: expected TxnID %d, got %d",
							tc.fromTxnID, i, exp, streamed[i])
					}
				}
			})
		}
	})
}

// TestTxnIDOrdering_StressOutOfOrder creates many transactions with intentionally
// out-of-order TxnIDs and verifies correct ordering.
func TestTxnIDOrdering_StressOutOfOrder(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create 50 transactions with intentionally non-sequential TxnIDs
	// Pattern alternates between high and low values to ensure out-of-order commits
	var txnIDs []uint64
	for i := 0; i < 50; i++ {
		// Generate TxnIDs that are intentionally non-sequential
		// Even indices: 10000, 10100, 10200, ...
		// Odd indices: 5000, 5100, 5200, ...
		var txnID uint64
		if i%2 == 0 {
			txnID = uint64(10000 + (i/2)*100)
		} else {
			txnID = uint64(5000 + (i/2)*100)
		}
		txnIDs = append(txnIDs, txnID)
	}

	// Begin all transactions first
	for _, txnID := range txnIDs {
		startTS := clock.Now()
		err := store.BeginTransaction(txnID, 1, startTS)
		if err != nil {
			t.Fatalf("BeginTransaction for txnID=%d failed: %v", txnID, err)
		}
	}

	// Commit in the order they were created (which is non-TxnID order)
	for _, txnID := range txnIDs {
		err := store.CommitTransaction(txnID, clock.Now(), nil, "testdb", "", 0, 0)
		if err != nil {
			t.Fatalf("CommitTransaction for txnID=%d failed: %v", txnID, err)
		}
	}

	// Stream and verify TxnID ordering
	var streamed []uint64
	err := store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
		streamed = append(streamed, rec.TxnID)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamCommittedTransactions failed: %v", err)
	}

	if len(streamed) != len(txnIDs) {
		t.Fatalf("Expected %d transactions, got %d", len(txnIDs), len(streamed))
	}

	// Verify ascending TxnID order
	sortedTxnIDs := make([]uint64, len(txnIDs))
	copy(sortedTxnIDs, txnIDs)
	sort.Slice(sortedTxnIDs, func(i, j int) bool { return sortedTxnIDs[i] < sortedTxnIDs[j] })

	for i, expected := range sortedTxnIDs {
		if streamed[i] != expected {
			t.Errorf("Index %d: expected TxnID %d, got %d", i, expected, streamed[i])
			if i > 5 {
				t.Log("... (truncating further errors)")
				break
			}
		}
	}
}

// TestTxnIDOrdering_ScanTransactionsDescending verifies that descending iteration
// also uses TxnID order.
func TestTxnIDOrdering_ScanTransactionsDescending(t *testing.T) {
	store, cleanup := createTestPebbleMetaStore(t)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Commit order: 300, 100, 500, 200, 400
	txnIDs := []uint64{300, 100, 500, 200, 400}

	for _, txnID := range txnIDs {
		startTS := clock.Now()
		store.BeginTransaction(txnID, 1, startTS)
		store.CommitTransaction(txnID, clock.Now(), nil, "testdb", "", 0, 0)
	}

	t.Run("DescendingOrder", func(t *testing.T) {
		var streamed []uint64
		err := store.ScanTransactions(0, true, func(rec *TransactionRecord) error {
			if rec.Status == TxnStatusCommitted {
				streamed = append(streamed, rec.TxnID)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("ScanTransactions descending failed: %v", err)
		}

		// Descending TxnID order: 500, 400, 300, 200, 100
		expected := []uint64{500, 400, 300, 200, 100}
		if len(streamed) != len(expected) {
			t.Fatalf("Expected %d transactions, got %d: %v", len(expected), len(streamed), streamed)
		}

		for i, exp := range expected {
			if streamed[i] != exp {
				t.Errorf("Index %d: expected TxnID %d, got %d", i, exp, streamed[i])
			}
		}
	})

	t.Run("DescendingWithFromTxnID", func(t *testing.T) {
		// fromTxnID=400 in descending should return 300, 200, 100 (TxnID < 400)
		var streamed []uint64
		err := store.ScanTransactions(400, true, func(rec *TransactionRecord) error {
			if rec.Status == TxnStatusCommitted {
				streamed = append(streamed, rec.TxnID)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("ScanTransactions descending failed: %v", err)
		}

		expected := []uint64{300, 200, 100}
		if len(streamed) != len(expected) {
			t.Fatalf("Expected %d transactions, got %d: %v", len(expected), len(streamed), streamed)
		}

		for i, exp := range expected {
			if streamed[i] != exp {
				t.Errorf("Index %d: expected TxnID %d, got %d", i, exp, streamed[i])
			}
		}
	})
}

// createTxnIDOrderingTestStore is a helper to create store for these tests.
func createTxnIDOrderingTestStore(t *testing.T) (*PebbleMetaStore, func()) {
	tmpDir, err := os.MkdirTemp("", "txnid_ordering_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	metaPath := filepath.Join(tmpDir, "test_meta.pebble")
	store, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
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

// createBenchmarkPebbleMetaStore creates a PebbleMetaStore for benchmarks.
func createBenchmarkPebbleMetaStore(b *testing.B) (*PebbleMetaStore, func()) {
	tmpDir, err := os.MkdirTemp("", "benchmark_pebble")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	metaPath := filepath.Join(tmpDir, "bench_meta.pebble")
	store, err := NewPebbleMetaStore(metaPath, PebbleMetaStoreOptions{
		CacheSizeMB:    32,
		MemTableSizeMB: 8,
		MemTableCount:  2,
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to create pebble meta store: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

// BenchmarkCommitTransaction measures the overhead of dual-index writes.
// Uses a fixed batch size to avoid b.N scaling issues with setup.
func BenchmarkCommitTransaction(b *testing.B) {
	const batchSize = 1000

	store, cleanup := createBenchmarkPebbleMetaStore(b)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Pre-create all transactions we'll need (outside timing)
	for i := 0; i < batchSize; i++ {
		txnID := uint64(i + 1)
		startTS := clock.Now()
		if err := store.BeginTransaction(txnID, 1, startTS); err != nil {
			b.Fatalf("BeginTransaction failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Measure commit performance
	for i := 0; i < b.N; i++ {
		txnID := uint64((i % batchSize) + 1)
		commitTS := clock.Now()
		// Note: Re-committing same txnID is idempotent in Pebble (overwrites)
		if err := store.CommitTransaction(txnID, commitTS, nil, "testdb", "", 0, 0); err != nil {
			b.Fatalf("CommitTransaction failed: %v", err)
		}
	}
}

// BenchmarkScanTransactions measures TxnID-ordered iteration performance.
func BenchmarkScanTransactions(b *testing.B) {
	store, cleanup := createBenchmarkPebbleMetaStore(b)
	defer cleanup()

	clock := hlc.NewClock(1)

	// Create 1000 transactions with varying TxnIDs (simulating clock skew)
	for i := 0; i < 1000; i++ {
		var txnID uint64
		if i%2 == 0 {
			txnID = uint64(10000 + i*10)
		} else {
			txnID = uint64(5000 + i*10)
		}
		startTS := clock.Now()
		store.BeginTransaction(txnID, 1, startTS)
		store.CommitTransaction(txnID, clock.Now(), nil, "testdb", "", 0, 0)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count := 0
		store.StreamCommittedTransactions(0, func(rec *TransactionRecord) error {
			count++
			return nil
		})
		_ = count
	}
}
