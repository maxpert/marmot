//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// staleGCRaceTxns is how many prepared transactions the concurrent race test
// sets the stale-transaction GC and the local commit path loose on.
const staleGCRaceTxns = 200

// TestStaleGCAndLocalCommitNeverCommitAnEmptyTransaction runs the
// stale-transaction GC and the log puller's local commit path
// (DatabaseManager.CommitLocallyPrepared) concurrently over many prepared
// transactions, every one of them stale. Each transaction must end either
// COMMITTED with its row applied, or not committed with its row absent -
// never COMMITTED without its row, which the cursor would then pass.
//
// Mutation (both halves of the fix): take the commit guard out of
// TransactionManager.cleanupStaleTransactions and make verifyPreparedPayload
// return nil. "committed without its row" fires.
func TestStaleGCAndLocalCommitNeverCommitAnEmptyTransaction(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
	tm := mdb.GetTransactionManager()
	tm.StopGarbageCollection()
	tm.heartbeatTimeout = 0

	const firstTxn = 20000
	for i := 0; i < staleGCRaceTxns; i++ {
		txnID := uint64(firstTxn + i)
		prep := engine.Prepare(context.Background(), &PrepareRequest{
			TxnID:      txnID,
			NodeID:     2,
			StartTS:    hlc.Timestamp{WallTime: int64(txnID), NodeID: 2},
			Database:   "testdb",
			Statements: []protocol.Statement{dmlLogInsertStatement("testdb", fmt.Sprintf("log:%d", txnID), int64(txnID), "race")},
		})
		require.True(t, prep.Success, "prepare %d: %s", txnID, prep.Error)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < staleGCRaceTxns; i++ {
			_, _ = tm.cleanupStaleTransactions()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < staleGCRaceTxns; i++ {
			_ = dm.CommitLocallyPrepared("testdb", uint64(firstTxn+i))
		}
	}()
	wg.Wait()

	for i := 0; i < staleGCRaceTxns; i++ {
		txnID := uint64(firstTxn + i)
		rec, err := mdb.GetMetaStore().GetTransaction(txnID)
		require.NoError(t, err)
		var v string
		rowErr := mdb.GetReadDB().QueryRow("SELECT v FROM log WHERE id = ?", int64(txnID)).Scan(&v)
		committed := rec != nil && rec.Status == TxnStatusCommitted
		if committed {
			require.NoError(t, rowErr, "txn %d committed without its row", txnID)
		} else {
			require.ErrorIs(t, rowErr, sql.ErrNoRows, "txn %d not committed but its row was applied", txnID)
		}
	}
}

// TestCommitRPCRefusesATransactionWhosePreparedPayloadIsGone pins the COMMIT
// RPC half of the prepared-payload check: a COMMIT carrying no statements
// (as the log puller's local commit, or a coordinator's minimal COMMIT,
// does) for a transaction whose captured rows, or DDL intent, were deleted
// after its durable prepare must be refused, not ACKed with nothing applied.
//
// Mutation: make verifyPreparedPayload return nil. Both COMMITs are ACKed.
func TestCommitRPCRefusesATransactionWhosePreparedPayloadIsGone(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
	mdb.GetTransactionManager().StopGarbageCollection()
	ms := mdb.GetMetaStore()

	const dmlTxn, ddlTxn = 21000, 21001
	prep := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID: dmlTxn, NodeID: 2, StartTS: hlc.Timestamp{WallTime: dmlTxn, NodeID: 2}, Database: "testdb",
		Statements: []protocol.Statement{dmlLogInsertStatement("testdb", "log:1", 1, "gone")},
	})
	require.True(t, prep.Success, prep.Error)
	require.NoError(t, ms.DeleteIntentsByTxn(dmlTxn))
	require.NoError(t, ms.DeleteIntentEntries(dmlTxn))

	res := engine.Commit(context.Background(), &CommitRequest{TxnID: dmlTxn, Database: "testdb"})
	require.False(t, res.Success, "a COMMIT whose prepared rows are gone was ACKed")
	require.Contains(t, res.Error, ErrPreparedRowsMissing.Error())

	prep = engine.Prepare(context.Background(), &PrepareRequest{
		TxnID: ddlTxn, NodeID: 2, StartTS: hlc.Timestamp{WallTime: ddlTxn, NodeID: 2}, Database: "testdb",
		Statements: []protocol.Statement{{Type: protocol.StatementDDL, Database: "testdb", TableName: "extra",
			SQL: "CREATE TABLE extra (id INTEGER PRIMARY KEY)"}},
	})
	require.True(t, prep.Success, prep.Error)
	require.NoError(t, ms.DeleteIntentsByTxn(ddlTxn))

	res = engine.Commit(context.Background(), &CommitRequest{TxnID: ddlTxn, Database: "testdb"})
	require.False(t, res.Success, "a COMMIT whose prepared DDL intent is gone was ACKed")
	require.Contains(t, res.Error, ErrPreparedRowsMissing.Error())

	for _, txnID := range []uint64{dmlTxn, ddlTxn} {
		rec, err := ms.GetTransaction(txnID)
		require.NoError(t, err)
		require.NotNil(t, rec)
		require.Equal(t, TxnStatusPending, rec.Status, "a refused COMMIT must leave txn %d PENDING", txnID)
	}
}
