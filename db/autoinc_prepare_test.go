//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// testClaimMembership is the cluster size every claim in these tests counts,
// and the view markedTableDB installs on the participant.
const testClaimMembership = 1

// claimStatement builds the wire shape a range claim takes: an existing DML
// statement type, a non-empty intent key, the payload, and no row image.
func claimStatement(t *testing.T, database, table string, prevBase, newBase, size uint64) protocol.Statement {
	t.Helper()
	payload, err := protocol.EncodeAutoIncClaim(AutoIncClaim{
		Table: table, PrevBase: prevBase, NewBase: newBase, Size: size, Membership: testClaimMembership,
	})
	require.NoError(t, err)
	return protocol.Statement{
		Type:               protocol.StatementInsert,
		Database:           database,
		TableName:          table,
		IntentKey:          []byte(protocol.AutoIncClaimKey(database, table)),
		AutoIDClaim:        true,
		AutoIDClaimPayload: payload,
	}
}

// markedTableDB sets up a database holding one marked AUTO_INCREMENT table,
// on a participant whose view of the cluster is testClaimMembership nodes.
func markedTableDB(t *testing.T, engine *ReplicationEngine, dm *DatabaseManager, ddl string) *ReplicatedDatabase {
	t.Helper()
	dm.SetClusterMembership(func() int { return testClaimMembership })
	require.NoError(t, dm.CreateDatabase("testdb"))
	mdb, err := dm.GetDatabase("testdb")
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())
	return mdb
}

// autoIncClaimStoreForTest wraps dm's system database as an AutoIncClaimStore,
// the same store DatabaseManager.wireGCCoordination injects into every
// TransactionManager in production (db/database_manager.go). Tests drive
// claim seeding and readback through this instead of a method on the user
// database, since the claim table lives in the system database now.
func autoIncClaimStoreForTest(dm *DatabaseManager) *AutoIncClaimStore {
	return NewAutoIncClaimStore(dm.GetSystemDatabase())
}

// seedClaimBase seeds a (database, table) claim row directly through the
// system-database store, standing in for what seedAutoIncBasesForDDL does at
// DDL time.
func seedClaimBase(t *testing.T, dm *DatabaseManager, database, table string, base, owner uint64) {
	t.Helper()
	require.NoError(t, autoIncClaimStoreForTest(dm).Seed(database, table, base, owner))
}

// readClaimBase reads a (database, table) claim row's base directly through
// the system-database store.
func readClaimBase(t *testing.T, dm *DatabaseManager, database, table string) uint64 {
	t.Helper()
	base, err := autoIncClaimStoreForTest(dm).ReadBase(database, table)
	require.NoError(t, err)
	return base
}

// claimRequest wraps one claim statement in a PREPARE request.
func claimRequest(txnID uint64, stmt protocol.Statement) *PrepareRequest {
	return &PrepareRequest{
		TxnID:      txnID,
		NodeID:     1,
		StartTS:    hlc.Timestamp{WallTime: int64(txnID)},
		Database:   "testdb",
		Statements: []protocol.Statement{stmt},
	}
}

// TestPrepareClaimBackfillsAnAbsentBaseFromTheTable pins the absent-row rule.
// "Absent means 0 means yes" is the mistake the protocol exists to prevent: a
// table whose ids reach 50 must not grant a range starting at 0. A node whose
// votes are not held backfills the base the DDL-time seed would have written
// - MAX(id) - and votes on that.
//
// Mutation: treat ErrAutoIncBaseAbsent as base 0 and fall through to the
// comparison. The claim is accepted and this fires.
func TestPrepareClaimBackfillsAnAbsentBaseFromTheTable(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	for id := 1; id <= 50; id++ {
		_, err := mdb.GetWriteDB().Exec("INSERT INTO users (id, v) VALUES (?, 'x')", id)
		require.NoError(t, err)
	}

	// No claim row has ever been written for this table.
	result := engine.Prepare(context.Background(), claimRequest(5001, claimStatement(t, "testdb", "users", 0, 0, 64)))
	if result.Success {
		t.Fatal("a participant with no stored base ACKed a range starting at 0 over ids up to 50; absent must never read as 0")
	}
	if !result.Rejected || result.AutoIDStoredBase != 50 {
		t.Fatalf("rejection = {rejected:%v base:%d}, want a rejection carrying the backfilled base 50", result.Rejected, result.AutoIDStoredBase)
	}
	if base := readClaimBase(t, dm, "testdb", "users"); base != 50 {
		t.Fatalf("stored base after backfill = %d, want 50", base)
	}

	result = engine.Prepare(context.Background(), claimRequest(5002, claimStatement(t, "testdb", "users", 50, 50, 64)))
	require.True(t, result.Success, "a claim above the backfilled base must be accepted: %s", result.Error)
}

// TestPrepareClaimRefusesWhenThereIsNothingToBackfill pins that the backfill
// applies only to a narrow auto-increment column: anything else stays absent
// and refused.
//
// Mutation: backfill every absent row with 0. The claim is accepted and this
// fires.
func TestPrepareClaimRefusesWhenThereIsNothingToBackfill(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")

	result := engine.Prepare(context.Background(), claimRequest(5001, claimStatement(t, "testdb", "ghost", 0, 0, 64)))
	if result.Success {
		t.Fatal("a claim for a table this node does not have was ACKed")
	}
	if !result.Rejected || !strings.Contains(result.Error, "ghost") {
		t.Fatalf("refusal = {rejected:%v error:%q}, want a rejection naming the table", result.Rejected, result.Error)
	}
	if _, err := autoIncClaimStoreForTest(dm).ReadBase("testdb", "ghost"); !errors.Is(err, ErrAutoIncBaseAbsent) {
		t.Fatalf("ReadBase after the refusal = %v, want the row still absent", err)
	}
}

// TestPrepareClaimDeclinesWhileVotesAreHeld pins the vote hold: a node whose
// bases may be missing claims it or a majority granted declines every claim,
// without a verdict and without backfilling, until it has merged bases.
//
// Mutation: ignore the hold in prepareAutoIncClaim. The claim is accepted
// and this fires.
func TestPrepareClaimDeclinesWhileVotesAreHeld(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 0, 1)
	_, err := dm.GetSystemDatabase().GetWriteDB().Exec(holdVotesSQL, 1)
	require.NoError(t, err)

	result := engine.Prepare(context.Background(), claimRequest(5001, claimStatement(t, "testdb", "users", 0, 0, 64)))
	if result.Success {
		t.Fatal("a node whose claim votes are held ACKed a claim")
	}
	if result.Rejected {
		t.Fatal("a held node cast a verdict; it must only decline, so the claim is decided by the rest of the cluster")
	}

	require.NoError(t, dm.MergeAutoIncBasesAndReleaseVotes(nil))
	result = engine.Prepare(context.Background(), claimRequest(5002, claimStatement(t, "testdb", "users", 0, 0, 64)))
	require.True(t, result.Success, "after the merge released the hold the claim must be accepted: %s", result.Error)
}

// TestPrepareClaimRejectsAnotherMembershipCount pins the membership check: a
// claim is voted on only by a node that counts the cluster the way the
// claimant did, since majorities of two memberships need not intersect. A
// claimant that predates the field sends 0 and is refused, not read as a size.
//
// Mutation: drop the membership comparison. The mismatched claim is accepted
// and this fires.
func TestPrepareClaimRejectsAnotherMembershipCount(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 30, 1)

	for i, membership := range []uint32{testClaimMembership + 2, 0} {
		stmt := claimStatement(t, "testdb", "users", 30, 30, 64)
		payload, err := protocol.EncodeAutoIncClaim(AutoIncClaim{Table: "users", PrevBase: 30, NewBase: 30, Size: 64, Membership: membership})
		require.NoError(t, err)
		stmt.AutoIDClaimPayload = payload

		result := engine.Prepare(context.Background(), claimRequest(uint64(5001+i), stmt))
		if result.Success {
			t.Fatalf("a claim counting %d members was ACKed by a node counting %d", membership, testClaimMembership)
		}
		if !result.Rejected || result.AutoIDStoredBase != 30 {
			t.Fatalf("membership %d: refusal = {rejected:%v base:%d}, want a rejection carrying base 30", membership, result.Rejected, result.AutoIDStoredBase)
		}
	}
}

// TestPrepareClaimCondition walks the three comparisons the condition makes.
func TestPrepareClaimCondition(t *testing.T) {
	cases := []struct {
		name             string
		storedBase       uint64
		prevBase         uint64
		newBase          uint64
		size             uint64
		wantAccept       bool
		wantReturnedBase uint64
	}{
		{
			name:       "fresh claim on a seeded table is accepted",
			storedBase: 0, prevBase: 0, newBase: 0, size: 64, wantAccept: true,
		},
		{
			name:       "a claim proposing this node's own base is accepted",
			storedBase: 1000, prevBase: 1000, newBase: 1000, size: 64, wantAccept: true,
		},
		{
			// A participant that missed an earlier commit round holds a base
			// BELOW the claimant's. It must still accept: the commit will carry
			// it forward. Mutation: tighten storedBase > prevBase to !=.
			name:       "a claim from ahead of this node's base is accepted",
			storedBase: 500, prevBase: 1000, newBase: 1000, size: 64, wantAccept: true,
		},
		{
			// The soundness case: this participant already committed a higher
			// claim, so the proposer lost the race and must be told the real base.
			// Mutation: drop the storedBase > prevBase comparison.
			name:       "stale claim is rejected and returns this node's base",
			storedBase: 5000, prevBase: 1000, newBase: 1000, size: 64,
			wantAccept: false, wantReturnedBase: 5000,
		},
		{
			// Allocation is lowest-free: a claimant proposes its OWN view of
			// the base and nothing else. Both directions are refused, so the
			// term is == and not >=.
			// Mutation: relax newBase != prevBase to newBase < prevBase.
			name:       "a claim below its own view of the base is rejected",
			storedBase: 1000, prevBase: 1000, newBase: 999, size: 64,
			wantAccept: false, wantReturnedBase: 1000,
		},
		{
			// Mutation: relax newBase != prevBase to newBase > prevBase.
			name:       "a claim above its own view of the base is rejected",
			storedBase: 1000, prevBase: 1000, newBase: 1500, size: 64,
			wantAccept: false, wantReturnedBase: 1000,
		},
		{
			// A zero-size claim would leave the committed base where it is, so
			// the same range could be handed out twice.
			// Mutation: drop the size == 0 comparison.
			name:       "a zero-size claim is rejected",
			storedBase: 1000, prevBase: 1000, newBase: 1000, size: 0,
			wantAccept: false, wantReturnedBase: 1000,
		},
		{
			// Mutation: drop the widthMax term. The column is INT, so its
			// ceiling is 2147483647; a range ending above it would mint ids the
			// column cannot hold.
			name:       "a claim past the column's ceiling is rejected",
			storedBase: 2147483000, prevBase: 2147483000, newBase: 2147483000, size: 10000,
			wantAccept: false, wantReturnedBase: 2147483000,
		},
		{
			// 2^64 - 2147483000: the additive form newBase+size wraps to exactly
			// 0, which compares as comfortably inside a signed INT's ceiling, so
			// a claim for the whole key space would be accepted.
			// Mutation: write the ceiling arm as claim.NewBase+claim.Size > widthMax.
			name:       "a claim whose size overflows uint64 is rejected",
			storedBase: 2147483000, prevBase: 2147483000, newBase: 2147483000,
			size:       18446744071562068616,
			wantAccept: false, wantReturnedBase: 2147483000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine, dm, cleanup := setupTestReplicationEngine(t)
			defer cleanup()
			markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
			seedClaimBase(t, dm, "testdb", "users", tc.storedBase, 1)

			result := engine.Prepare(context.Background(), &PrepareRequest{
				TxnID:      6001,
				NodeID:     1,
				StartTS:    hlc.Timestamp{WallTime: 1},
				Database:   "testdb",
				Statements: []protocol.Statement{claimStatement(t, "testdb", "users", tc.prevBase, tc.newBase, tc.size)},
			})

			if tc.wantAccept {
				if !result.Success {
					t.Fatalf("claim rejected, want accepted: %s", result.Error)
				}
				return
			}
			if result.Success {
				t.Fatal("claim accepted, want rejected")
			}
			// The claimant retries with the maximum base returned, so a
			// rejection that carries nothing makes it spin.
			// Mutation: leave AutoIDStoredBase unset on the rejection paths.
			if result.AutoIDStoredBase != tc.wantReturnedBase {
				t.Errorf("rejection returned base %d, want %d", result.AutoIDStoredBase, tc.wantReturnedBase)
			}
		})
	}
}

// TestPrepareClaimDerivesWidthFromItsOwnSchema pins that the ceiling comes from
// this node's own sqlite_master and not from the claimant. A claim carries no
// width at all, so a node whose column is narrower rejects a range a wider
// node would have accepted.
//
// Mutation: take a width from the claim payload. The SMALLINT node accepts a
// range only an INT column could hold.
func TestPrepareClaimDerivesWidthFromItsOwnSchema(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	// SMALLINT: ceiling 32767.
	markedTableDB(t, engine, dm, "CREATE TABLE small (id INTEGER /*M:16a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "small", 32000, 1)

	result := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      7001,
		NodeID:     1,
		StartTS:    hlc.Timestamp{WallTime: 1},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "small", 32000, 32000, 1000)},
	})

	if result.Success {
		t.Fatal("a SMALLINT column accepted a range ending at 33000, past its 32767 ceiling")
	}
	if !strings.Contains(result.Error, "exhausts") {
		t.Errorf("rejection %q does not name exhaustion", result.Error)
	}
}

// TestPrepareClaimTakesIntentLockBeforeReadingBase pins the ORDER of the two
// halves of a participant's vote, which is the whole of the protocol's
// soundness argument under concurrency.
//
// A participant's vote is "my stored base is not above your prevBase". Reading
// the base and writing the intent are each individually synchronised, so -race
// sees nothing wrong; what matters is that they cannot straddle another
// claimant's commit. If the base is read BEFORE the intent's row lock is
// taken, this interleaving grants the same ids twice:
//
//	B reads base 0 -> A commits, base becomes 100, A's lock is released ->
//	B's WriteIntent now succeeds -> B is accepted for [0,100) as well.
//
// The fix is to take the lock first, so the read happens inside it. The
// observable consequence, and what this test asserts, is that a claim whose
// lock is already held by a live transaction fails WITHOUT CASTING A VOTE: it
// comes back as a plain missing ACK (Rejected false, no base attached), not as
// a stale-base rejection, even though its base genuinely is stale. A node that
// answers "your base is stale, mine is 500" has by definition read the base
// without the lock.
//
// Mutation: move the AddStatement/WriteIntent pair back below the condition
// switch in prepareAutoIncClaim (db/replication_engine.go). The stale claim is
// then judged before the lock is attempted, the result comes back
// Rejected=true with AutoIDStoredBase=500, and the three assertions below fire
// by name.
func TestPrepareClaimTakesIntentLockBeforeReadingBase(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	// Claimant A prepares and does not commit, so its intent - and the claim
	// key's row lock - stays held for the rest of the test.
	prepA := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      8100,
		NodeID:     1,
		StartTS:    hlc.Timestamp{WallTime: 100},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "users", 500, 500, 10)},
	})
	require.True(t, prepA.Success, "PREPARE refused a well-formed claim: %s", prepA.Error)

	// Claimant B is genuinely stale: this node holds 500 and B proposes 0.
	prepB := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      8101,
		NodeID:     2,
		StartTS:    hlc.Timestamp{WallTime: 101},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "users", 0, 0, 10)},
	})
	require.False(t, prepB.Success, "a second claim was accepted while another holds the claim key's lock")
	require.False(t, prepB.Rejected,
		"the node cast a verdict on a claim whose lock it never held: the base was read outside the lock")
	require.Zero(t, prepB.AutoIDStoredBase,
		"the node returned its stored base for a claim it never got the lock to judge")
	require.Contains(t, prepB.Error, "write-write conflict",
		"expected the lost race for the claim key's row lock, got: %s", prepB.Error)

	// The base is untouched by either attempt: only COMMIT advances it.
	require.Equal(t, uint64(500), readClaimBase(t, dm, "testdb", "users"))
}

// TestPrepareClaimRejectsOnceTheWinnerHasCommitted is the other side of
// TestPrepareClaimTakesIntentLockBeforeReadingBase: once the lock IS free, the
// base the loser reads is the winner's committed one, so it rejects with a
// base its coordinator can retry above instead of being granted the same ids.
//
// The winner commits before the loser prepares, so this does not exercise
// the read-under-lock ordering (TestPrepareClaimTakesIntentLockBeforeReadingBase
// does); it pins the verdict and the retry base once the lock is free.
//
// Mutation: drop the "storedBase > prevBase" rejection from
// prepareAutoIncClaim and "the winner's committed range was handed out a
// second time" fires; or omit AutoIDStoredBase from that rejection and "the
// rejection must carry this node's base" fires.
func TestPrepareClaimRejectsOnceTheWinnerHasCommitted(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const winner = 8200
	prep := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      winner,
		NodeID:     1,
		StartTS:    hlc.Timestamp{WallTime: 200},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "users", 500, 500, 10)},
	})
	require.True(t, prep.Success, "PREPARE refused a well-formed claim: %s", prep.Error)
	res := engine.Commit(context.Background(), &CommitRequest{
		TxnID:      winner,
		Database:   "testdb",
		Statements: []protocol.Statement{commitStatement("testdb", "users")},
	})
	require.True(t, res.Success, "COMMIT failed: %s", res.Error)
	require.Equal(t, uint64(510), readClaimBase(t, dm, "testdb", "users"))

	loser := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      8201,
		NodeID:     2,
		StartTS:    hlc.Timestamp{WallTime: 201},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "users", 500, 500, 10)},
	})
	require.False(t, loser.Success, "the winner's committed range was handed out a second time")
	require.True(t, loser.Rejected, "a stale claim on a free lock must be a verdict, not a missing ACK")
	require.Equal(t, uint64(510), loser.AutoIDStoredBase,
		"the rejection must carry this node's base so the claimant retries above it")
}

// TestPrepareResultConversionKeepsTheStoredBase: the coordinator's own node
// answers PREPARE through LocalReplicator, which returns
// PrepareResult.ToCoordinatorResponse. A rejected claim's stored base must
// survive that conversion, or the coordinator never learns the base to retry
// above from its own vote.
//
// Mutation: drop AutoIDStoredBase from PrepareResult.ToCoordinatorResponse.
// "the local PREPARE path dropped a rejected claim's stored base" fires.
func TestPrepareResultConversionKeepsTheStoredBase(t *testing.T) {
	res := (&PrepareResult{Rejected: true, AutoIDStoredBase: 510}).ToCoordinatorResponse()
	require.Equal(t, uint64(510), res.AutoIDStoredBase, "the local PREPARE path dropped a rejected claim's stored base")
}
