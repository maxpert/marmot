//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// A participant that restarts while it holds a prepared transaction gets the
// transaction's row locks and CDC rows back from its meta store. The tests
// below pin that such a transaction stays resolvable: its COMMIT applies the
// rows it prepared, its ABORT ends it for good, and if no decision ever
// arrives the stale-transaction GC ends it. Each runs under both prepare sync
// modes, because each recovers the transaction from a different place: strict
// sync from the segment log's prepare record, grouped sync from the Pebble
// manifest key.

var prepareSyncModes = []struct {
	name string
	env  string
}{
	{name: "grouped", env: ""},
	{name: "strict", env: "strict"},
}

func prepareLogRow(engine *ReplicationEngine, txnID uint64, v string) *PrepareResult {
	return engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      txnID,
		NodeID:     1,
		StartTS:    hlc.Timestamp{WallTime: int64(txnID)},
		Database:   "testdb",
		Statements: []protocol.Statement{dmlLogInsertStatement("testdb", "log:1", 1, v)},
	})
}

func commitLogRow(engine *ReplicationEngine, txnID uint64) *CommitResult {
	return engine.Commit(context.Background(), &CommitRequest{
		TxnID:      txnID,
		Database:   "testdb",
		Statements: []protocol.Statement{dmlLogCommitStatement("testdb", "log:1")},
	})
}

// restartNode closes dm and opens a new DatabaseManager and engine on the same
// data directory, as a node restart does: every meta store is reopened through
// NewMetaStore, which runs Pebble recovery and ReconstructFromPebble.
func restartNode(t *testing.T, dm *DatabaseManager) (*ReplicationEngine, *DatabaseManager) {
	t.Helper()
	dir := dm.GetDataDir()
	require.NoError(t, dm.Close())
	clock := hlc.NewClock(1)
	restarted, err := NewDatabaseManager(dir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close() })
	return NewReplicationEngine(1, restarted, clock), restarted
}

// restartedLogDB prepares txnID on log:1 and then restarts the node. The
// returned database's GC goroutine is stopped so a test drives passes itself.
func restartedLogDB(t *testing.T, syncEnv string, txnID uint64, v string) (*ReplicationEngine, *DatabaseManager, *ReplicatedDatabase) {
	t.Helper()
	t.Setenv("MARMOT_CDC_PREPARE_SYNC", syncEnv)
	engine, dm, cleanup := setupTestReplicationEngine(t)
	t.Cleanup(cleanup)
	markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")

	prep := prepareLogRow(engine, txnID, v)
	require.True(t, prep.Success, "prepare: %s", prep.Error)

	engine, dm = restartNode(t, dm)
	mdb, err := dm.GetDatabase("testdb")
	require.NoError(t, err)
	mdb.GetTransactionManager().StopGarbageCollection()
	return engine, dm, mdb
}

// TestRecoveredPreparedRowIsFreedByTheStaleTransactionGC: a node restarts
// holding a prepared DML transaction whose coordinator never returns. Recovery
// re-locks its row, so a second writer is refused until the stale-transaction
// GC aborts it - which the GC can only do if recovery registered it where the
// GC looks. The GC judges the transaction by a heartbeat of the restart time,
// so the row is freed at most heartbeat_timeout + gc_interval after the
// restart. The passes are driven directly so the test does not race the GC
// goroutine.
//
// Mutation: skip the registration in ReconstructFromPebble. "the
// stale-transaction GC never saw the recovered prepared transaction" fires.
func TestRecoveredPreparedRowIsFreedByTheStaleTransactionGC(t *testing.T) {
	for _, mode := range prepareSyncModes {
		t.Run(mode.name, func(t *testing.T) {
			const dead, b1, b2 = 9700, 9701, 9702
			engine, _, mdb := restartedLogDB(t, mode.env, dead, "dead")
			restarted := time.Now()
			tm := mdb.GetTransactionManager()
			const heartbeatTimeout = 200 * time.Millisecond
			tm.heartbeatTimeout = heartbeatTimeout

			require.False(t, prepareLogRow(engine, b1, "b").Success,
				"the restart dropped the prepared transaction's row lock")

			cleaned, err := tm.cleanupStaleTransactions()
			require.NoError(t, err)
			require.Zero(t, cleaned, "the GC aborted a recovered transaction younger than heartbeat_timeout")

			time.Sleep(heartbeatTimeout - time.Since(restarted) + 50*time.Millisecond)
			cleaned, err = tm.cleanupStaleTransactions()
			require.NoError(t, err)
			require.Equal(t, 1, cleaned, "the stale-transaction GC never saw the recovered prepared transaction")

			prepB := prepareLogRow(engine, b2, "b")
			require.True(t, prepB.Success, "the GC abort did not free the recovered row: %s", prepB.Error)
			t.Logf("row freed %v after the restart (heartbeat_timeout %v, GC pass driven directly)",
				time.Since(restarted).Round(time.Millisecond), heartbeatTimeout)

			require.False(t, commitLogRow(engine, dead).Success, "a GC-aborted recovered transaction was committed")
		})
	}
}

// TestRecoveredPreparedRowIsFreedWithinTheGCBound measures the liveness bound
// with the GC goroutine itself rather than driven passes: a writer blocked by
// a recovered transaction whose decision never arrives gets the row no later
// than heartbeat_timeout + gc_interval after the restart (plus the time a pass
// and a PREPARE take), because the GC's first tick past heartbeat_timeout
// aborts it.
func TestRecoveredPreparedRowIsFreedWithinTheGCBound(t *testing.T) {
	const dead = 9705
	engine, _, mdb := restartedLogDB(t, "", dead, "dead")
	restarted := time.Now()
	tm := mdb.GetTransactionManager()
	const heartbeatTimeout, gcInterval, slack = 200 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond
	tm.heartbeatTimeout = heartbeatTimeout
	tm.gcInterval = gcInterval
	tm.StartGarbageCollection()
	defer tm.StopGarbageCollection()

	var freedAfter time.Duration
	for txnID := uint64(dead + 1); time.Since(restarted) < 10*time.Second; txnID++ {
		if prepareLogRow(engine, txnID, "b").Success {
			freedAfter = time.Since(restarted)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NotZero(t, freedAfter, "the recovered row was never freed")
	t.Logf("row freed %v after the restart by the GC goroutine (heartbeat_timeout %v, gc_interval %v)",
		freedAfter.Round(time.Millisecond), heartbeatTimeout, gcInterval)
	require.Less(t, freedAfter, heartbeatTimeout+gcInterval+slack, "the recovered row outlived heartbeat_timeout + gc_interval")
}

// TestRecoveredPreparedTransactionCommitsItsRows: the COMMIT of a transaction
// prepared before a restart applies the row it prepared. ReconstructFromPebble
// used to treat every transaction without a commit record as an orphan and
// delete its CDC rows, so this COMMIT found nothing, committed nothing, and
// ACKed.
//
// Mutation: in ReconstructFromPebble, delete the rows of recovered
// transactions too. "the COMMIT of a recovered transaction was refused"
// fires, because COMMIT refuses a DML transaction whose rows are gone; with
// that refusal also removed, "the recovered transaction's row was not
// applied" fires.
func TestRecoveredPreparedTransactionCommitsItsRows(t *testing.T) {
	for _, mode := range prepareSyncModes {
		t.Run(mode.name, func(t *testing.T) {
			const txnID = 9710
			engine, _, mdb := restartedLogDB(t, mode.env, txnID, "prepared")

			res := commitLogRow(engine, txnID)
			require.True(t, res.Success, "the COMMIT of a recovered transaction was refused: %s", res.Error)

			var v string
			err := mdb.GetReadDB().QueryRow("SELECT v FROM log WHERE id = 1").Scan(&v)
			require.NoError(t, err, "the recovered transaction's row was not applied")
			require.Equal(t, "prepared", v)

			require.True(t, prepareLogRow(engine, txnID+1, "next").Success,
				"the committed recovered transaction kept its row lock")
		})
	}
}

// TestRecoveredPreparedTransactionEndsOnItsAbort: the coordinator's ABORT
// for a transaction recovered at restart finds it pending, and ending it frees
// its row at once, without waiting for the stale-transaction GC.
//
// Mutation: make ReplicationEngine.Abort skip TransactionManager.AbortTransaction
// for a user database. The recovered transaction stays pending and "the ABORT
// of a recovered transaction left its row locked" fires. (Dropping only the
// abort's DeleteIntentsByTxn does not strand the row: once the record is
// gone, the next writer's conflict resolution overwrites the intent.)
func TestRecoveredPreparedTransactionEndsOnItsAbort(t *testing.T) {
	for _, mode := range prepareSyncModes {
		t.Run(mode.name, func(t *testing.T) {
			const txnID = 9715
			engine, _, _ := restartedLogDB(t, mode.env, txnID, "aborted")

			require.True(t, engine.Abort(context.Background(), &AbortRequest{TxnID: txnID, Database: "testdb"}).Success)
			prep := prepareLogRow(engine, txnID+1, "next")
			require.True(t, prep.Success, "the ABORT of a recovered transaction left its row locked: %s", prep.Error)
			require.False(t, commitLogRow(engine, txnID).Success, "an aborted recovered transaction was committed")
		})
	}
}

// TestPreparedTransactionSurvivesADetachedRestore: a snapshot restore detaches
// the database while a transaction is prepared on it. A COMMIT that arrives
// in the window is refused, never ACKed against the file being replaced; the
// meta store stays open across the window, so after the reattach the
// transaction is still prepared and its COMMIT applies its row to the file
// now in place.
//
// Mutation: make DetachDatabase leave the database in service. "a COMMIT was
// ACKed while its database was detached" fires.
func TestPreparedTransactionSurvivesADetachedRestore(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")

	const txnID = 9725
	require.True(t, prepareLogRow(engine, txnID, "kept").Success)
	require.NoError(t, dm.DetachDatabase(context.Background(), "testdb"))
	require.False(t, commitLogRow(engine, txnID).Success, "a COMMIT was ACKed while its database was detached")
	require.NoError(t, dm.AttachDatabase("testdb"))

	res := commitLogRow(engine, txnID)
	require.True(t, res.Success, "the COMMIT after the reattach was refused: %s", res.Error)
	mdb, err := dm.GetDatabase("testdb")
	require.NoError(t, err)
	var v string
	require.NoError(t, mdb.GetReadDB().QueryRow("SELECT v FROM log WHERE id = 1").Scan(&v))
	require.Equal(t, "kept", v)
}

// TestAbortedPreparedTransactionStaysAbortedAcrossARestart: under strict sync
// the segment prepare record outlives an ABORT, and recovery reads a prepare
// record whose Pebble status is gone as a prepare whose unsynced keys were
// lost. The abort therefore leaves an ABORTED status behind; without it, every
// restart would bring the aborted transaction and its row locks back.
//
// Mutation: delete the status key on abort instead of writing ABORTED. "an
// aborted transaction's row lock came back after the restart" fires (strict).
func TestAbortedPreparedTransactionStaysAbortedAcrossARestart(t *testing.T) {
	for _, mode := range prepareSyncModes {
		t.Run(mode.name, func(t *testing.T) {
			t.Setenv("MARMOT_CDC_PREPARE_SYNC", mode.env)
			engine, dm, cleanup := setupTestReplicationEngine(t)
			defer cleanup()
			markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")

			const aborted, next = 9720, 9721
			require.True(t, prepareLogRow(engine, aborted, "aborted").Success)
			require.True(t, engine.Abort(context.Background(), &AbortRequest{TxnID: aborted, Database: "testdb"}).Success)

			engine, dm = restartNode(t, dm)
			mdb, err := dm.GetDatabase("testdb")
			require.NoError(t, err)
			mdb.GetTransactionManager().StopGarbageCollection()

			prep := prepareLogRow(engine, next, "next")
			require.True(t, prep.Success, "an aborted transaction's row lock came back after the restart: %s", prep.Error)
			require.False(t, commitLogRow(engine, aborted).Success, "an aborted transaction was committed after a restart")
		})
	}
}

// TestPreparedTransactionWhoseRowsWereLostIsAbortedAtRecovery: under grouped
// sync, PREPARE can ACK before its CDC rows are on disk, so an OS crash can
// leave the Pebble manifest without the rows it points at. Recovery cannot
// keep that transaction's promise; it aborts it and releases its locks, so a
// COMMIT is refused instead of ACKed having applied nothing, and the node
// still opens.
//
// Mutation: in recoverSealedPreparedTransactions, skip the transaction on a
// row read error instead of aborting it. "a transaction whose prepared rows
// were lost is still pending" fires.
func TestPreparedTransactionWhoseRowsWereLostIsAbortedAtRecovery(t *testing.T) {
	t.Setenv("MARMOT_CDC_PREPARE_SYNC", "")
	dir := t.TempDir()
	opts := PebbleMetaStoreOptions{CacheSizeMB: 8, MemTableSizeMB: 4, MemTableCount: 2}
	store, err := NewPebbleMetaStore(dir, opts)
	require.NoError(t, err)

	const txnID uint64 = 9730
	require.NoError(t, store.BeginTransaction(txnID, 1, hlc.Timestamp{WallTime: 1, NodeID: 1}))
	row, err := EncodeRow(&EncodedCapturedRow{
		Table:     "log",
		Op:        uint8(OpTypeInsert),
		IntentKey: []byte("log:1"),
		NewValues: map[string][]byte{"id": []byte("1")},
	})
	require.NoError(t, err)
	require.NoError(t, store.WriteCapturedRow(txnID, 1, row))
	require.NoError(t, store.DurablyPrepareTransaction(txnID))
	require.NoError(t, store.Close())

	// The OS crash: the segment's rows never reached the disk.
	segments, err := filepath.Glob(filepath.Join(dir, cdcSegmentDirName, "seg-*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	for _, seg := range segments {
		require.NoError(t, os.Truncate(seg, 0))
	}

	recovered, err := NewPebbleMetaStore(dir, opts)
	require.NoError(t, err, "a lost prepared row must not stop the node from opening")
	defer recovered.Close()

	rec, err := recovered.GetTransaction(txnID)
	require.NoError(t, err)
	require.Nil(t, rec, "a transaction whose prepared rows were lost is still pending")
	require.Empty(t, recovered.takeRecoveredPrepared())
	_, locked := recovered.rowLocks.CheckLock("", "log", "log:1")
	require.False(t, locked, "a transaction whose prepared rows were lost kept its row lock")
	_, closer, err := recovered.db.Get(pebbleCDCManifestKey(txnID))
	if err == nil {
		closer.Close()
	}
	require.ErrorIs(t, err, pebble.ErrNotFound, "the lost transaction's manifest was kept")
}
