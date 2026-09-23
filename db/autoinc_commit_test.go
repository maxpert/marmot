//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// commitStatement builds what a claim looks like on the COMMIT wire: the flag,
// the intent key, and no payload. buildCommitRequest strips the payload on
// purpose, so a COMMIT handler that reads its values from the wire instead of
// from the intent it prepared will fail against this shape rather than in
// production.
func commitStatement(database, table string) protocol.Statement {
	return protocol.Statement{
		Type:        protocol.StatementInsert,
		Database:    database,
		TableName:   table,
		IntentKey:   []byte(protocol.AutoIncClaimKey(database, table)),
		AutoIDClaim: true,
	}
}

// TestCommitAppliesClaimAndAdvancesBase is the COMMIT half of the protocol: the
// row only materialises here, so the whole uniqueness argument
// rests on this write happening and on it storing newBase+size.
//
// Mutation: store claim.NewBase instead of claim.NewBase+claim.Size. The base
// never advances, the next claimant proposes the same range, and the assertion
// on 1064 fires.
func TestCommitAppliesClaimAndAdvancesBase(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 1000, 1)

	const txnID = 7100
	prep := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      txnID,
		NodeID:     7,
		StartTS:    hlc.Timestamp{WallTime: 4242},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "users", 1000, 1000, 64)},
	})
	require.True(t, prep.Success, "PREPARE refused a well-formed claim: %s", prep.Error)

	res := engine.Commit(context.Background(), &CommitRequest{
		TxnID:      txnID,
		Database:   "testdb",
		Statements: []protocol.Statement{commitStatement("testdb", "users")},
	})
	require.True(t, res.Success, "COMMIT failed: %s", res.Error)

	base := readClaimBase(t, dm, "testdb", "users")
	if base != 1064 {
		t.Fatalf("stored base is %d, want 1064: the claimant owns 1001..1064, so the next prevBase is 1064", base)
	}

	// Owner and grant time come from the intent record, never from the payload,
	// so a claimant cannot attribute a range to a node that did not ask for it.
	// Mutation: write re.nodeID (1 here) as the owner instead of intent.NodeID.
	// Keyed by BOTH database and table: the system database's claim table now
	// serves every user database, so a query on table name alone would match
	// another database's row of the same name.
	var owner, grantedAt int64
	require.NoError(t, dm.GetSystemDatabase().GetReadDB().QueryRow(
		"SELECT owner, granted_at FROM "+AutoIncClaimTable+" WHERE db = ? AND tbl = ?", "testdb", "users").
		Scan(&owner, &grantedAt))
	if owner != 7 {
		t.Errorf("range owner recorded as %d, want the claimant 7", owner)
	}
	if grantedAt != 4242 {
		t.Errorf("granted_at recorded as %d, want the claim's own timestamp 4242", grantedAt)
	}
}

// TestCommitWithoutClaimWritesNothing pins the gate. An ordinary transaction
// carries no claim flag, so the commit path must not touch the claim table at
// all -- this is what keeps a Pebble intent scan off every write.
//
// Mutation: drop the statementsCarryAutoIDClaim guard and always call
// AutoIncClaimStore.ApplyClaims. The call finds no claim intent and writes nothing, so
// this test still passes; it is the benchmark, not this test, that holds that
// line. What this test does hold is that a claim-free commit still succeeds.
func TestCommitWithoutClaimWritesNothing(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 1000, 1)

	const txnID = 7200
	prep := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:    txnID,
		NodeID:   7,
		StartTS:  hlc.Timestamp{WallTime: 10},
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type: protocol.StatementDDL, Database: "testdb", TableName: "other",
			SQL: "CREATE TABLE other (id INTEGER PRIMARY KEY)",
		}},
	})
	require.True(t, prep.Success, "PREPARE failed: %s", prep.Error)

	res := engine.Commit(context.Background(), &CommitRequest{
		TxnID:    txnID,
		Database: "testdb",
		Statements: []protocol.Statement{{
			Type: protocol.StatementDDL, Database: "testdb", TableName: "other",
			SQL: "CREATE TABLE other (id INTEGER PRIMARY KEY)",
		}},
	})
	require.True(t, res.Success, "COMMIT failed: %s", res.Error)

	base := readClaimBase(t, dm, "testdb", "users")
	require.Equal(t, uint64(1000), base, "a claim-free commit moved the base")
}

// TestCommitRefusesWhenClaimCannotBeApplied is the claim's first invariant:
// a participant must never ACK COMMIT unless the claim row is durably in its
// own SQLite. It runs under both batch-commit settings. A claim-only
// transaction has no CDC entries, so CommitTransaction would take its
// statement branch under either setting, and the refusal happens before
// CommitTransaction is reached; the batch committer's blocking wait is pinned
// by TestParticipantWithholdsAckWhenTransactionCommitFailsAfterClaimApplies.
//
// The failure is injected with a BEFORE UPDATE trigger on the claim table, so
// the seeded row PREPARE reads is intact and only the apply's upsert aborts.
//
// Mutation: apply the claim AFTER txnMgr.CommitTransaction, or swallow the
// apply error and return Success: true. Either way the node reports a commit it
// did not durably make and both assertions below fire.
func TestCommitRefusesWhenClaimCannotBeApplied(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "batch_commit_disabled"
		if batch {
			name = "batch_commit_enabled"
		}
		t.Run(name, func(t *testing.T) {
			restore := cfg.Config.BatchCommit.Enabled
			cfg.Config.BatchCommit.Enabled = batch
			defer func() { cfg.Config.BatchCommit.Enabled = restore }()

			engine, dm, cleanup := setupTestReplicationEngine(t)
			defer cleanup()
			mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
			seedClaimBase(t, dm, "testdb", "users", 1000, 1)

			const txnID = 7300
			prep := engine.Prepare(context.Background(), &PrepareRequest{
				TxnID:      txnID,
				NodeID:     7,
				StartTS:    hlc.Timestamp{WallTime: 11},
				Database:   "testdb",
				Statements: []protocol.Statement{claimStatement(t, "testdb", "users", 1000, 1000, 64)},
			})
			require.True(t, prep.Success, "PREPARE refused a well-formed claim: %s", prep.Error)

			_, err := dm.GetSystemDatabase().GetWriteDB().Exec(
				"CREATE TRIGGER refuse_base_write BEFORE UPDATE ON " + AutoIncClaimTable +
					" BEGIN SELECT RAISE(ABORT, 'claim write refused'); END")
			require.NoError(t, err)

			res := engine.Commit(context.Background(), &CommitRequest{
				TxnID:      txnID,
				Database:   "testdb",
				Statements: []protocol.Statement{commitStatement("testdb", "users")},
			})
			if res.Success {
				t.Fatal("participant ACKed COMMIT although the claim row was not written")
			}
			// Pin the REASON, not just the refusal: a commit that failed
			// because the transaction was missing would satisfy every other
			// assertion here while proving nothing about the claim write.
			if !strings.Contains(res.Error, "claim write refused") {
				t.Fatalf("COMMIT failed for the wrong reason: %s", res.Error)
			}

			base := readClaimBase(t, dm, "testdb", "users")
			require.Equal(t, uint64(1000), base, "the base moved despite the refused write")

			// The transaction stays PENDING: a participant that cannot complete
			// withholds its ACK and leaves recovery to resolve the transaction,
			// rather than unilaterally discarding a transaction the rest of the
			// cluster may be committing.
			if txn := mdb.GetTransactionManager().GetTransaction(txnID); txn == nil {
				t.Error("the transaction is no longer PENDING, so the node decided it alone")
			}
		})
	}
}

// TestDroppedTableKeepsItsClaimRow holds the claim's second invariant, "the
// row is never deleted", against the one operation that most looks like it
// should delete it.
//
// This is a deliberate, named divergence from MySQL, where DROP TABLE discards
// the counter and a table recreated under the same name restarts at 1. Here the
// row outlives the table, so a recreated table continues above the dropped
// one's last id. The divergence is the safe direction: an absent row is a hard
// rejection at PREPARE, so any code path that removes rows opens a
// window in which a table re-mints ids it has already used -- recovery's
// pruning hazard. Ids restarting at 1 is cosmetic; ids repeating is corruption.
//
// The table is dropped through the replicated DDL path - PREPARE and COMMIT
// of a DDL statement - which is where DDL-time seeding runs
// (seedAutoIncBasesForDDL) and so the only place a DROP TABLE could reach the
// claim row.
//
// Mutation: in seedAutoIncBasesForDDL, delete the claim row of a DDL-touched
// table that no longer exists. "a DROP TABLE deleted the claim row" fires.
func TestDroppedTableKeepsItsClaimRow(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 1000, 1)

	drop := protocol.Statement{Type: protocol.StatementDDL, Database: "testdb", TableName: "users", SQL: "DROP TABLE users"}
	prep := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID: 7400, NodeID: 1, StartTS: hlc.Timestamp{WallTime: 13}, Database: "testdb",
		Statements: []protocol.Statement{drop},
	})
	require.True(t, prep.Success, "PREPARE of DROP TABLE: %s", prep.Error)
	res := engine.Commit(context.Background(), &CommitRequest{
		TxnID: 7400, Database: "testdb", Statements: []protocol.Statement{drop},
	})
	require.True(t, res.Success, "COMMIT of DROP TABLE: %s", res.Error)
	var tables int
	require.NoError(t, mdb.GetReadDB().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 'users'").Scan(&tables))
	require.Zero(t, tables, "the DDL did not drop the table, so this test proves nothing")

	base, err := autoIncClaimStoreForTest(dm).ReadBase("testdb", "users")
	require.NoError(t, err, "a DROP TABLE deleted the claim row")
	require.Equal(t, uint64(1000), base)
}

// dmlLogInsertStatement builds a real client DML statement (not a claim) that
// carries a CDC row image, exactly as PREPARE requires for the DML path
// (createDMLIntent, db/replication_engine.go). Riding alongside a claim
// statement in the same PREPARE request is what makes cdcEntries > 0 at
// COMMIT (db/transaction.go:269,277), which is the only thing that routes
// CommitTransaction into the batch committer at all.
func dmlLogInsertStatement(database, intentKey string, id int64, v string) protocol.Statement {
	return testProtocolDMLStatement(protocol.StatementInsert, database, "log", []byte(intentKey), nil,
		encodeTestValues(map[string]interface{}{"id": id, "v": v}))
}

// dmlLogCommitStatement is the COMMIT-side decision metadata for the same
// insert: type, table, and intent key only, no row image (COMMIT never
// carries a payload -- CommitTransaction rebuilds txn.Statements from the CDC
// entries persisted at PREPARE).
func dmlLogCommitStatement(database, intentKey string) protocol.Statement {
	return protocol.Statement{
		Type:      protocol.StatementInsert,
		Database:  database,
		TableName: "log",
		IntentKey: []byte(intentKey),
	}
}

// TestParticipantWithholdsAckWhenClaimRowIsNotDurable is the claim's first
// invariant, proved with a transaction that genuinely reaches CommitTransaction's
// DML branch: the PREPARE carries a claim statement AND an ordinary DML insert,
// so GetIntentEntries returns a non-empty slice and the len(cdcEntries) > 0
// branch at db/transaction.go:277 is the one taken, not the DDL/statement
// branch at :294 a claim-only transaction would take.
//
// The claim apply itself still fails and refuses the commit before
// CommitTransaction is ever called (db/replication_engine.go: the
// AutoIncClaimStore.ApplyClaims check at the statementsCarryAutoIDClaim gate returns
// early, above txnMgr.CommitTransaction). That ordering is itself part of the
// invariant: a claim that cannot be written must never let the surrounding
// transaction's DML commit either. Both assertions here hold in both batch
// configurations, and the log table row count pins that the DML never landed.
//
// Mutation: apply the claim AFTER txnMgr.CommitTransaction, or swallow the
// apply error and return Success: true. Either way the node reports a commit
// it did not durably make and both assertions below fire.
func TestParticipantWithholdsAckWhenClaimRowIsNotDurable(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "batch_commit_disabled"
		if batch {
			name = "batch_commit_enabled"
		}
		t.Run(name, func(t *testing.T) {
			restore := cfg.Config.BatchCommit.Enabled
			cfg.Config.BatchCommit.Enabled = batch
			defer func() { cfg.Config.BatchCommit.Enabled = restore }()

			engine, dm, cleanup := setupTestReplicationEngine(t)
			defer cleanup()
			mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
			seedClaimBase(t, dm, "testdb", "users", 1000, 1)
			_, err := mdb.GetWriteDB().Exec("CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
			require.NoError(t, err)

			const txnID = 7500
			prep := engine.Prepare(context.Background(), &PrepareRequest{
				TxnID:    txnID,
				NodeID:   7,
				StartTS:  hlc.Timestamp{WallTime: 12},
				Database: "testdb",
				Statements: []protocol.Statement{
					claimStatement(t, "testdb", "users", 1000, 1000, 64),
					dmlLogInsertStatement("testdb", "log:1", 1, "x"),
				},
			})
			require.True(t, prep.Success, "PREPARE refused claim+DML: %s", prep.Error)

			// Proves the precondition the whole test rests on: this is a real
			// DML transaction, not a claim-only one, so toggling
			// BatchCommit.Enabled actually selects between CommitTransaction's
			// two branches (db/transaction.go:277 vs :294) rather than being a
			// no-op for this commit.
			entries, err := mdb.GetMetaStore().GetIntentEntries(txnID)
			require.NoError(t, err)
			require.Len(t, entries, 1, "DML must produce a CDC entry or this test does not exercise the DML/batch branch")

			_, err = dm.GetSystemDatabase().GetWriteDB().Exec(
				"CREATE TRIGGER refuse_base_write BEFORE UPDATE ON " + AutoIncClaimTable +
					" BEGIN SELECT RAISE(ABORT, 'claim write refused'); END")
			require.NoError(t, err)

			res := engine.Commit(context.Background(), &CommitRequest{
				TxnID:    txnID,
				Database: "testdb",
				Statements: []protocol.Statement{
					commitStatement("testdb", "users"),
					dmlLogCommitStatement("testdb", "log:1"),
				},
			})
			if res.Success {
				t.Fatal("participant ACKed COMMIT although the claim row was not written")
			}
			if !strings.Contains(res.Error, "claim write refused") {
				t.Fatalf("COMMIT failed for the wrong reason: %s", res.Error)
			}

			base := readClaimBase(t, dm, "testdb", "users")
			require.Equal(t, uint64(1000), base, "the base moved despite the refused write")

			var logCount int
			require.NoError(t, mdb.GetReadDB().QueryRow("SELECT COUNT(*) FROM log").Scan(&logCount))
			require.Zero(t, logCount, "the DML riding with the failed claim must not land either")

			if txn := mdb.GetTransactionManager().GetTransaction(txnID); txn == nil {
				t.Error("the transaction is no longer PENDING, so the node decided it alone")
			}
		})
	}
}

// TestParticipantWithholdsAckWhenTransactionCommitFailsAfterClaimApplies is the
// complementary case: the claim itself commits durably (AutoIncClaimStore.ApplyClaims
// succeeds, so the base row is written and durable independent of anything
// that follows), but the rest of the transaction's own DML
// commit then fails. This is where the batch committer's blocking fut.Get()
// (db/transaction.go:282) is the only thing standing between a failed DML
// apply and a false ACK: Enqueue hands the work to the flush loop
// asynchronously, and only a synchronous wait on the future keeps Commit()
// from returning Success: true before the flush loop's outcome is known.
//
// The base having already moved while Success is false is not a bug this test
// hunts for -- it is the documented design (db/replication_engine.go's comment
// on the AutoIncClaimStore.ApplyClaims call): a participant that cannot complete
// withholds its ACK and leaves the transaction PENDING for recovery, rather
// than silently discarding a claim the rest of the cluster may already be
// counting as committed. The invariant under test is narrower and absolute:
// Success must be false whenever the surrounding commit did not complete, in
// both batch configurations.
//
// Mutation: return Success: true whenever AutoIncClaimStore.ApplyClaims succeeds, without
// checking txnMgr.CommitTransaction's error (or drop the batch committer's
// fut.Get() and treat Enqueue as fire-and-forget). Either way res.Success is
// true while the log table holds no row, and the assertions below fire.
func TestParticipantWithholdsAckWhenTransactionCommitFailsAfterClaimApplies(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "batch_commit_disabled"
		if batch {
			name = "batch_commit_enabled"
		}
		t.Run(name, func(t *testing.T) {
			restore := cfg.Config.BatchCommit.Enabled
			cfg.Config.BatchCommit.Enabled = batch
			defer func() { cfg.Config.BatchCommit.Enabled = restore }()

			engine, dm, cleanup := setupTestReplicationEngine(t)
			defer cleanup()
			mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
			seedClaimBase(t, dm, "testdb", "users", 1000, 1)
			_, err := mdb.GetWriteDB().Exec("CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
			require.NoError(t, err)

			const txnID = 7600
			prep := engine.Prepare(context.Background(), &PrepareRequest{
				TxnID:    txnID,
				NodeID:   7,
				StartTS:  hlc.Timestamp{WallTime: 13},
				Database: "testdb",
				Statements: []protocol.Statement{
					claimStatement(t, "testdb", "users", 1000, 1000, 64),
					dmlLogInsertStatement("testdb", "log:1", 1, "x"),
				},
			})
			require.True(t, prep.Success, "PREPARE refused claim+DML: %s", prep.Error)

			entries, err := mdb.GetMetaStore().GetIntentEntries(txnID)
			require.NoError(t, err)
			require.Len(t, entries, 1, "DML must produce a CDC entry or this test does not exercise the DML/batch branch")

			// The claim table is untouched: only the DML's own apply fails, so
			// AutoIncClaimStore.ApplyClaims succeeds and CommitTransaction is reached.
			_, err = mdb.GetWriteDB().Exec(
				"CREATE TRIGGER refuse_log_insert BEFORE INSERT ON log" +
					" BEGIN SELECT RAISE(ABORT, 'dml commit refused'); END")
			require.NoError(t, err)

			res := engine.Commit(context.Background(), &CommitRequest{
				TxnID:    txnID,
				Database: "testdb",
				Statements: []protocol.Statement{
					commitStatement("testdb", "users"),
					dmlLogCommitStatement("testdb", "log:1"),
				},
			})
			if res.Success {
				t.Fatal("participant ACKed COMMIT although the transaction's own DML commit failed")
			}
			if !strings.Contains(res.Error, "dml commit refused") {
				t.Fatalf("COMMIT failed for the wrong reason: %s", res.Error)
			}

			base := readClaimBase(t, dm, "testdb", "users")
			require.Equal(t, uint64(1064), base,
				"the claim apply is a separate, already-committed write; "+
					"a later failure in the same transaction must not un-commit it")

			var logCount int
			require.NoError(t, mdb.GetReadDB().QueryRow("SELECT COUNT(*) FROM log").Scan(&logCount))
			require.Zero(t, logCount, "the failed DML must not land despite the claim having committed")

			if txn := mdb.GetTransactionManager().GetTransaction(txnID); txn == nil {
				t.Error("the transaction is no longer PENDING, so the node decided it alone")
			}
		})
	}
}
