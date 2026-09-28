package db

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// TestApplyNonDMLIntents_AtomicAcrossDDLFailure is the atomicity
// regression: a failure on the second statement of a non-DML commit must
// roll back the whole SQLite transaction, so the first (otherwise
// successful) DDL statement, the schema-version bump and the applied-txn
// marker are all-or-nothing together.
//
// Before this fix, DDL ran under autocommit per statement
// (TransactionManager.execDDL opened its own *sql.Conn per intent), so the
// first CREATE TABLE would have persisted even though the whole apply
// failed on the second statement.
func TestApplyNonDMLIntents_AtomicAcrossDDLFailure(t *testing.T) {
	testDB := setupTestDBWithMeta(t)
	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock, NewSchemaCache())
	tm.SetDatabaseName("testdb")
	_, err := ensureSchemaVersionTable(testDB.DB, "testdb", false, nil)
	require.NoError(t, err)

	intents := []*WriteIntentRecord{
		{IntentType: IntentTypeDDL, TableName: "t", NodeID: 1, SQLStatement: "CREATE TABLE t (id INTEGER PRIMARY KEY)", CreatedAt: 1},
		// Second statement fails: no such table "nope".
		{IntentType: IntentTypeDDL, TableName: "nope", NodeID: 1, SQLStatement: "ALTER TABLE nope ADD COLUMN x INTEGER", CreatedAt: 2},
	}

	err = tm.applyNonDMLIntents(99, hlc.Timestamp{WallTime: 1, NodeID: 1}, intents)
	require.Error(t, err, "the second DDL statement must fail")

	var name string
	err = testDB.DB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='t'").Scan(&name)
	require.ErrorIs(t, err, sql.ErrNoRows, "the first DDL must be rolled back together with the failed second one")

	var one int
	err = testDB.DB.QueryRow("SELECT 1 FROM __marmot_applied_txn WHERE txn_id = ?", 99).Scan(&one)
	require.ErrorIs(t, err, sql.ErrNoRows, "no marker must be left behind by a failed apply")

	var version uint64
	require.NoError(t, testDB.DB.QueryRow("SELECT version FROM __marmot_schema_version WHERE id = 1").Scan(&version))
	require.Equal(t, uint64(0), version, "the schema version must not have bumped")
}

// TestApplyNonDMLIntents_ClaimOnlyWritesNoMarkerAndNoSQLiteTx pins the
// deliberate deviation documented on applyNonDMLIntents: a non-DML commit
// with no DDL, LOAD DATA or vector-control intent at all (the shape of an
// AUTO_INCREMENT claim-only transaction, whose IntentTypeAutoIDClaim intent
// is filtered out entirely) must NOT open a SQLite tx or write a marker.
// Writing one here would need the user database's single writer
// (_txlock=immediate) even when a concurrent pinned session already holds
// it for the whole surrounding LLDAP-shaped transaction - see
// TestClaimCompletesWhilePinnedSessionOpen (db/autoinc_hookdb_test.go),
// which this behavior exists to keep passing.
func TestApplyNonDMLIntents_ClaimOnlyWritesNoMarkerAndNoSQLiteTx(t *testing.T) {
	testDB := setupTestDBWithMeta(t)
	clock := hlc.NewClock(1)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, clock, NewSchemaCache())
	tm.SetDatabaseName("testdb")
	_, err := ensureSchemaVersionTable(testDB.DB, "testdb", false, nil)
	require.NoError(t, err)

	require.NoError(t, tm.applyNonDMLIntents(7, hlc.Timestamp{WallTime: 5, NodeID: 1}, nil))

	err = testDB.DB.QueryRow("SELECT 1 FROM __marmot_applied_txn WHERE txn_id = ?", 7).Scan(new(int))
	require.ErrorIs(t, err, sql.ErrNoRows, "a claim-only commit must not write a marker")

	var version uint64
	require.NoError(t, testDB.DB.QueryRow("SELECT version FROM __marmot_schema_version WHERE id = 1").Scan(&version))
	require.Equal(t, uint64(0), version, "a claim-only commit must not bump the schema version")
}

// TestApplyNonDMLIntents_2PCDDLCommitBumpsVersionExactlyOnceAndReplayIsNoOp
// is the end-to-end regression for exactly-once DDL: after a multi-statement
// 2PC DDL transaction (ALTER TABLE ADD COLUMN, then CREATE INDEX) commits
// through TransactionManager.CommitTransaction, the marker exists and the
// schema version is exactly +1 (one bump per committed txn, not per
// statement); replaying the very same transaction through ApplyReplayedTxn
// then applies nothing at all, leaving the marker and the version unchanged,
// where idempotent-rewritten DDL would otherwise silently re-apply and bump
// the version twice.
func TestApplyNonDMLIntents_2PCDDLCommitBumpsVersionExactlyOnceAndReplayIsNoOp(t *testing.T) {
	tmpDir := t.TempDir()
	dbMgr, err := NewDatabaseManager(tmpDir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dbMgr.Close()
	require.NoError(t, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(t, err)

	_, err = mdb.GetWriteDB().Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())

	txnMgr := mdb.GetTransactionManager()
	txn, err := txnMgr.BeginTransaction(1)
	require.NoError(t, err)

	stmts := []struct{ sql, table string }{
		{"ALTER TABLE t ADD COLUMN name TEXT", "t"},
		{"CREATE INDEX idx_name ON t(name)", "t"},
	}
	for i, s := range stmts {
		stmt := protocol.Statement{Type: protocol.StatementDDL, SQL: s.sql, TableName: s.table, Database: "app"}
		snap, err := SerializeData(DDLSnapshot{Type: int(stmt.Type), SQL: s.sql, TableName: s.table})
		require.NoError(t, err)
		require.NoError(t, txnMgr.WriteIntent(txn, IntentTypeDDL, s.table, fmt.Sprintf("ddl:%d", i), stmt, snap))
	}
	require.NoError(t, txnMgr.CommitTransaction(txn))

	require.Equal(t, uint64(1), mdb.SchemaVersion(), "a two-statement DDL txn must bump the version exactly once")

	var one int
	require.NoError(t, mdb.GetWriteDB().QueryRow("SELECT 1 FROM __marmot_applied_txn WHERE txn_id = ?", txn.ID).Scan(&one))

	// Reconstruct the replayed form of the same transaction from what
	// writeNonDMLToCDC captured during the commit above, exactly as a peer
	// pulling this txn from this node's log would receive it.
	cursor, err := mdb.GetMetaStore().IterateCapturedRows(txn.ID)
	require.NoError(t, err)
	var rows []*EncodedCapturedRow
	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		require.NoError(t, err)
		rows = append(rows, row)
	}
	require.NoError(t, cursor.Err())
	require.NoError(t, cursor.Close())
	require.Len(t, rows, 2)

	applied, err := mdb.ApplyReplayedTxn(context.Background(), &ReplayTxn{
		TxnID:        txn.ID,
		OriginNodeID: 1,
		CommitTS:     txn.CommitTS,
		Rows:         rows,
	}, false)
	require.NoError(t, err)
	require.False(t, applied, "an already-committed txn must not be replayed again")
	require.Equal(t, uint64(1), mdb.SchemaVersion(), "replaying an already-applied DDL must not bump the version again")
}

// TestAbortTransaction_RefusesCommittedReplayAndKeepsCapturedRows: a late
// local PREPARE's abort can hit a txn
// id a peer's replay already logged as COMMITTED (MetaStore.AbortTransaction
// then refuses with ErrAbortCommitted). TransactionManager.AbortTransaction
// must still release the PREPARE's own write intents but must NOT delete the
// committed entry's CDC intent entries or captured rows - a peer's next pull,
// or this node's own restore re-apply, still needs to read them back.
func TestAbortTransaction_RefusesCommittedReplayAndKeepsCapturedRows(t *testing.T) {
	testDB := setupTestDBWithMeta(t)
	tm := NewTransactionManager(testDB.DB, testDB.MetaStore, hlc.NewClock(1), NewSchemaCache())
	tm.SetDatabaseName("testdb")

	const txnID = uint64(9001)
	require.NoError(t, testDB.MetaStore.WriteCapturedRow(txnID, 1, []byte("row-data")))
	require.NoError(t, testDB.MetaStore.SealCapturedRows(txnID))
	require.NoError(t, testDB.MetaStore.StoreReplayedTransaction(txnID, 2, hlc.Timestamp{WallTime: 1, NodeID: 2}, "testdb", 1, 0))

	rec, err := testDB.MetaStore.GetTransaction(txnID)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, TxnStatusCommitted, rec.Status)

	// The *Transaction object a late local PREPARE's handler would still be
	// holding, believing txnID PENDING (tm.GetTransaction would already
	// refuse to reconstruct one for a COMMITTED id, so this is built the way
	// BeginTransactionWithID's own caller would have kept it).
	lateTxn := &Transaction{ID: txnID, Status: TxnStatusPending}

	err = tm.AbortTransaction(lateTxn)
	require.ErrorIs(t, err, ErrAbortCommitted)

	cursor, err := testDB.MetaStore.IterateCapturedRows(txnID)
	require.NoError(t, err)
	count := 0
	for cursor.Next() {
		count++
	}
	require.NoError(t, cursor.Err())
	require.NoError(t, cursor.Close())
	require.Equal(t, 1, count, "the committed log entry's captured rows must survive the refused abort")

	entries, _, _, err := testDB.MetaStore.ListCommittedLog(LogPosition{}, 10)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		found = found || e.TxnID == txnID
	}
	require.True(t, found, "the committed log entry must still be listed")
}

// TestDDLCommitCrashAfterMarkerIsRepairedIntoTheLog: a crash after a 2PC
// DDL's SQLite commit (DDL + marker) and before its commit record leaves the
// DDL applied but outside this node's log. The captured rows are written
// before that SQLite commit, so the next open repairs the commit record from
// them and the DDL enters the log peers pull from.
//
// Mutation: write the captured rows after applyNonDMLIntents again. "the
// applied DDL never entered this node's log" fires.
func TestDDLCommitCrashAfterMarkerIsRepairedIntoTheLog(t *testing.T) {
	tmpDir := t.TempDir()
	dbMgr, err := NewDatabaseManager(tmpDir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	require.NoError(t, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(t, err)
	txnMgr := mdb.GetTransactionManager()

	txn, err := txnMgr.BeginTransaction(1)
	require.NoError(t, err)
	ddl := "CREATE TABLE crashed (id INTEGER PRIMARY KEY)"
	stmt := protocol.Statement{Type: protocol.StatementDDL, SQL: ddl, TableName: "crashed", Database: "app"}
	snap, err := SerializeData(DDLSnapshot{Type: int(stmt.Type), SQL: ddl, TableName: "crashed"})
	require.NoError(t, err)
	require.NoError(t, txnMgr.WriteIntent(txn, IntentTypeDDL, "crashed", "ddl:0", stmt, snap))
	require.NoError(t, mdb.GetMetaStore().DurablyPrepareTransaction(txn.ID), "PREPARE's durable fence")
	txnMgr.failBeforeFinalizeForTest = true
	require.Error(t, txnMgr.CommitTransaction(txn), "the injected crash must stop the commit")
	require.NoError(t, dbMgr.Close())

	dbMgr, err = NewDatabaseManager(tmpDir, 1, hlc.NewClock(1))
	require.NoError(t, err)
	defer dbMgr.Close()
	mdb, err = dbMgr.GetDatabase("app")
	require.NoError(t, err)
	rec, err := mdb.GetMetaStore().GetTransaction(txn.ID)
	require.NoError(t, err)
	require.True(t, rec != nil && rec.Status == TxnStatusCommitted, "the applied DDL never entered this node's log")
	entries, _, _, err := mdb.GetMetaStore().ListCommittedLog(LogPosition{}, 10)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		found = found || e.TxnID == txn.ID
	}
	require.True(t, found, "the repaired DDL is not listed in this node's log")
}
