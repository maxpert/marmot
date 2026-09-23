//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// TestClaimCompletesWhilePinnedSessionOpen is the LLDAP-shape property, and it
// is the reason the claim row lives in the system database rather than in the
// user database it is about.
//
// The sea-orm/LLDAP shape is: BEGIN; INSERT INTO users ...; INSERT INTO
// groups ...; COMMIT. Marmot serves that shape with a pinned session
// (db/db_integration.go BeginPinnedSession), which takes hookDB's single
// connection (hookDB.SetMaxOpenConns(1)) and opens a SQLite transaction on it
// that stays open until Release. Both pools are built with _txlock=immediate
// (db/db_integration.go writeDSN, hookDSN), so that open transaction holds the
// USER database's one SQLite writer for its whole lifetime.
//
// The second INSERT in that shape is what needs a range, so its claim runs
// while the first INSERT's pinned session is still open. Two distinct ways to
// get this wrong; both block, and the bounded timeout below turns either into
// a failure (the second is the one run as a mutation):
//
//   - The claim takes a connection from hookDB (directly, or via
//     StartEphemeralSession). hookDB has one slot and the pinned session holds
//     it, so the claim blocks behind the transaction its own request is part
//     of: a self-deadlock, not a cross-request one.
//   - The claim writes the claim row into the USER database (the pre-relocation
//     design). Its BEGIN IMMEDIATE on GetWriteDB() waits on the same writer the
//     pinned session holds, and blocks just as hard.
//
// The claim instead writes the SYSTEM database (AutoIncClaimStore, a separate
// SQLite file with its own writer), so it completes promptly. The claim below
// is driven through the real PREPARE and COMMIT handlers on the SAME
// ReplicatedDatabase whose writer the pinned session holds, under a bounded
// timeout far below pinnedSessionTimeout's default of 50s
// (coordinator/handler.go), so a regression fails by name instead of hanging
// the suite.
//
// Mutation: point AutoIncClaimStore at the user database instead of the system
// database (NewAutoIncClaimStore(replicatedDB) in
// ReplicationEngine.autoIncClaimStore). The COMMIT apply's BEGIN blocks on the
// pinned session's writer and the select's timeout branch fires the t.Fatal
// below.
func TestClaimCompletesWhilePinnedSessionOpen(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	_, err := mdb.GetWriteDB().Exec("CREATE TABLE groups (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())
	seedClaimBase(t, dm, "testdb", "groups", 3000, 1)

	// Statement one of the shape: a real pinned session on the user database,
	// with a real INSERT already executed on it, held open for the rest of the
	// test. Per BeginPinnedSession's doc comment pinnedCtx must stay alive
	// until Release is called.
	pinnedCtx, pinnedCancel := context.WithCancel(context.Background())
	defer pinnedCancel()
	pinned, err := mdb.BeginPinnedSession(pinnedCtx, 9500)
	require.NoError(t, err, "failed to open pinned session")
	defer func() { _ = pinned.Release() }()

	_, _, err = pinned.ExecuteStatement(pinnedCtx,
		"INSERT INTO users (id, v) VALUES (?, ?)", []interface{}{int64(1), "ada"})
	require.NoError(t, err, "the pinned session's first INSERT failed")

	// Build the claim statements up front so the goroutine below touches no
	// *testing.T: calling require/t.Fatal from a non-test goroutine is unsafe,
	// and a failure inside the goroutine must instead surface as a value on
	// the result channel that the test goroutine asserts on.
	claimStmt := claimStatement(t, "testdb", "groups", 3000, 3000, 16)
	commitStmt := commitStatement("testdb", "groups")

	type claimOutcome struct{ err error }
	done := make(chan claimOutcome, 1)
	go func() {
		const txnID = 9501
		prep := engine.Prepare(context.Background(), &PrepareRequest{
			TxnID:      txnID,
			NodeID:     9,
			StartTS:    hlc.Timestamp{WallTime: 30},
			Database:   "testdb",
			Statements: []protocol.Statement{claimStmt},
		})
		if !prep.Success {
			done <- claimOutcome{err: fmt.Errorf("PREPARE refused a well-formed claim: %s", prep.Error)}
			return
		}

		res := engine.Commit(context.Background(), &CommitRequest{
			TxnID:      txnID,
			Database:   "testdb",
			Statements: []protocol.Statement{commitStmt},
		})
		if !res.Success {
			done <- claimOutcome{err: fmt.Errorf("COMMIT failed: %s", res.Error)}
			return
		}
		done <- claimOutcome{}
	}()

	select {
	case outcome := <-done:
		require.NoError(t, outcome.err, "claim through PREPARE/COMMIT did not complete cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("claim through PREPARE/COMMIT blocked while a pinned session held the user " +
			"database's writer and hookDB's single connection: the claim must write the system " +
			"database and must never contend for either")
	}

	// The claim committed durably, not merely promptly.
	require.Equal(t, uint64(3016), readClaimBase(t, dm, "testdb", "groups"),
		"the claim did not durably commit")

	// The pinned session is still usable, so the claim neither stole its
	// connection nor rolled its transaction back underneath it.
	_, _, err = pinned.ExecuteStatement(pinnedCtx,
		"INSERT INTO groups (id, v) VALUES (?, ?)", []interface{}{int64(3001), "admins"})
	require.NoError(t, err, "the pinned session was disturbed by the concurrent claim")
}
