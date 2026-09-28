//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// A prepared transaction's row locks are what make a participant's vote
// binding: for an AUTO_INCREMENT claim, the claim key's lock is what makes the
// read of the stored base and the vote on it atomic. The tests below pin that
// no other writer can take a lock from a transaction that has not ended, and
// that a claim COMMIT is refused whenever this node did not hold the claim
// from its vote to its commit.

// ageHeartbeat makes txnID look abandoned: participants never refresh a
// heartbeat, so any prepared transaction whose coordinator is slower than
// heartbeat_timeout_seconds looks like this.
func ageHeartbeat(t *testing.T, mdb *ReplicatedDatabase, txnID uint64) {
	t.Helper()
	mem, ok := mdb.GetMetaStore().(*MemoryMetaStore)
	require.True(t, ok, "test expects the production MemoryMetaStore")
	mem.txnStore.UpdateHeartbeat(txnID, time.Now().Add(-time.Hour).UnixNano())
}

func prepareClaim(engine *ReplicationEngine, t *testing.T, txnID, nodeID uint64, prevBase, size uint64) *PrepareResult {
	return engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      txnID,
		NodeID:     nodeID,
		StartTS:    hlc.Timestamp{WallTime: int64(txnID)},
		Database:   "testdb",
		Statements: []protocol.Statement{claimStatement(t, "testdb", "users", prevBase, prevBase, size)},
	})
}

func commitClaim(engine *ReplicationEngine, txnID uint64) *CommitResult {
	return engine.Commit(context.Background(), &CommitRequest{
		TxnID:      txnID,
		Database:   "testdb",
		Statements: []protocol.Statement{commitStatement("testdb", "users")},
	})
}

// TestPreparedClaimIsNotEvictedByAStaleHeartbeat: claimant A prepares, its
// coordinator stalls past the heartbeat timeout, and claimant B proposes the
// same range. B used to evict A's lock, pass PREPARE on the base A had also
// read, and commit; A's COMMIT then found no claim intent and ACKed anyway, so
// both were granted ids 501..510 on this node.
//
// Mutation: in resolveIntentConflictPebble, let a PENDING holder yield once its
// heartbeat is older than heartbeat_timeout_seconds. "a second claimant was
// granted the claim key while a prepared claim holds it" fires.
func TestPreparedClaimIsNotEvictedByAStaleHeartbeat(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const a, b, retry = 9100, 9101, 9102
	prepA := prepareClaim(engine, t, a, 1, 500, 10)
	require.True(t, prepA.Success, "A prepare: %s", prepA.Error)
	ageHeartbeat(t, mdb, a)

	prepB := prepareClaim(engine, t, b, 2, 500, 20)
	require.False(t, prepB.Success, "a second claimant was granted the claim key while a prepared claim holds it")
	require.False(t, prepB.Rejected, "the node cast a verdict on a claim whose lock it never held")
	require.Contains(t, prepB.Error, "write-write conflict")

	commitA := commitClaim(engine, a)
	require.True(t, commitA.Success, "A's COMMIT: %s", commitA.Error)
	require.Equal(t, uint64(510), readClaimBase(t, dm, "testdb", "users"))

	// B's retry from its stale view is now rejected with the base to retry above.
	prepRetry := prepareClaim(engine, t, retry, 2, 500, 20)
	require.True(t, prepRetry.Rejected, "a stale proposal was accepted after A committed")
	require.Equal(t, uint64(510), prepRetry.AutoIDStoredBase)
}

// TestPreparedDMLIsNotEvictedByAStaleHeartbeat is the same property for an
// ordinary write. B used to evict A's row lock and both COMMITs were ACKed, so
// the write-write conflict the lock exists to report was never reported.
//
// Mutation: the one named on TestPreparedClaimIsNotEvictedByAStaleHeartbeat.
// "a second writer was granted a row a prepared transaction holds" fires.
func TestPreparedDMLIsNotEvictedByAStaleHeartbeat(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")

	prepareRow := func(txnID uint64, v string) *PrepareResult {
		return engine.Prepare(context.Background(), &PrepareRequest{
			TxnID:      txnID,
			NodeID:     1,
			StartTS:    hlc.Timestamp{WallTime: int64(txnID)},
			Database:   "testdb",
			Statements: []protocol.Statement{dmlLogInsertStatement("testdb", "log:1", 1, v)},
		})
	}

	const a, b = 9200, 9201
	prepA := prepareRow(a, "a")
	require.True(t, prepA.Success, "A prepare: %s", prepA.Error)
	ageHeartbeat(t, mdb, a)

	prepB := prepareRow(b, "b")
	require.False(t, prepB.Success, "a second writer was granted a row a prepared transaction holds")
	require.Contains(t, prepB.Error, "write-write conflict")

	commitA := engine.Commit(context.Background(), &CommitRequest{
		TxnID:      a,
		Database:   "testdb",
		Statements: []protocol.Statement{dmlLogCommitStatement("testdb", "log:1")},
	})
	require.True(t, commitA.Success, "A's COMMIT: %s", commitA.Error)
	var v string
	require.NoError(t, mdb.GetReadDB().QueryRow("SELECT v FROM log WHERE id = 1").Scan(&v))
	require.Equal(t, "a", v)
}

// TestAbortedClaimLeavesNothingThatGrantsTheKeyTwice: A claims and aborts, B
// claims and holds, and C must then lose the race for the claim key rather
// than be granted the range B holds.
//
// Mutation: in resolveIntentConflictPebble, let a PENDING holder yield. "a
// second claimant was granted a range while another holds the claim key"
// fires.
func TestAbortedClaimLeavesNothingThatGrantsTheKeyTwice(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	require.True(t, prepareClaim(engine, t, 8300, 1, 500, 10).Success)
	abort := engine.Abort(context.Background(), &AbortRequest{TxnID: 8300, Database: "testdb"})
	require.True(t, abort.Success, "abort failed: %s", abort.Error)

	prepB := prepareClaim(engine, t, 8301, 2, 500, 10)
	require.True(t, prepB.Success, "PREPARE refused a well-formed claim: %s", prepB.Error)

	prepC := prepareClaim(engine, t, 8302, 3, 500, 10)
	require.False(t, prepC.Success, "a second claimant was granted a range while another holds the claim key")
	require.False(t, prepC.Rejected, "the node cast a verdict on a claim whose lock it never held")
	require.Contains(t, prepC.Error, "write-write conflict")

	res := commitClaim(engine, 8301)
	require.True(t, res.Success, "COMMIT failed: %s", res.Error)
	require.Equal(t, uint64(510), readClaimBase(t, dm, "testdb", "users"))
}

// TestCommitRefusesAClaimWhoseIntentIsGone: a COMMIT carrying the claim flag
// on a node that holds no claim intent for the transaction must not be ACKed.
// Nothing was applied, so an ACK would count this node toward a commit quorum
// for a range it never recorded.
//
// Mutation: make ApplyClaims return nil when the transaction holds no claim
// intent. "a claim COMMIT was ACKed with no claim intent to apply" fires.
func TestCommitRefusesAClaimWhoseIntentIsGone(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const a = 9300
	require.True(t, prepareClaim(engine, t, a, 1, 500, 10).Success)
	// The intent is gone while the transaction record stays pending.
	require.NoError(t, mdb.GetMetaStore().DeleteIntentsByTxn(a))

	res := commitClaim(engine, a)
	require.False(t, res.Success, "a claim COMMIT was ACKed with no claim intent to apply")
	require.Contains(t, res.Error, ErrAutoIncClaimNotApplicable.Error())
	require.True(t, res.ToCoordinatorResponse().ClaimNotApplicable, "the refusal reached the coordinator unclassified")
	require.Equal(t, uint64(500), readClaimBase(t, dm, "testdb", "users"))
}

// TestCommitRefusesAClaimOnceTheBaseMovedPastItsPrevBase: a DDL seed raised
// the base between the claim's PREPARE and its COMMIT. Writing the claim's
// newBase+size would lower the base, and the range it hands out sits below the
// new floor, so the COMMIT must be refused and the base left where the seed
// put it.
//
// Mutation: drop "AND seed <= ?" from applyClaimTx's UPDATE. The COMMIT is
// ACKed and "a claim below the raised floor was ACKed" fires.
func TestCommitRefusesAClaimOnceTheBaseMovedPastItsPrevBase(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const a = 9400
	require.True(t, prepareClaim(engine, t, a, 1, 500, 10).Success)
	seedClaimBase(t, dm, "testdb", "users", 800, 2)

	res := commitClaim(engine, a)
	require.False(t, res.Success, "a claim below the raised floor was ACKed")
	require.Equal(t, uint64(800), readClaimBase(t, dm, "testdb", "users"), "the claim lowered a base a seed had raised")
	require.Contains(t, res.Error, ErrAutoIncClaimNotApplicable.Error())
	require.True(t, res.ClaimNotApplicable, "the refusal was not classified as a claim that cannot be applied")
}

// intentsFrom is a MetaStore whose GetIntentsByTxn replays intents captured
// earlier, standing in for a COMMIT that read its claim intent just before the
// intent was released.
type intentsFrom struct {
	MetaStore
	intents []*WriteIntentRecord
}

func (m intentsFrom) GetIntentsByTxn(uint64) ([]*WriteIntentRecord, error) {
	return m.intents, nil
}

// TestApplyClaimsACKsAtMostOneOfTwoOverlappingClaims is the per-node half of
// the argument that two overlapping ranges can never both commit: A read its
// claim intent at COMMIT, then lost its lock (a stale-transaction GC abort
// racing the COMMIT), and B prepared and committed the same range. A's apply
// must now be refused, so this node ACKs one of the two and never both; two
// majorities that each ACKed one would have to share a node that ACKed both.
//
// Mutation: drop "AND committed <= ?" from applyClaimTx's UPDATE. A's apply
// succeeds and "this node applied two overlapping claims" fires.
func TestApplyClaimsACKsAtMostOneOfTwoOverlappingClaims(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const a, b = 9500, 9501
	require.True(t, prepareClaim(engine, t, a, 1, 500, 10).Success)
	captured, err := mdb.GetMetaStore().GetIntentsByTxn(a)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	abort := engine.Abort(context.Background(), &AbortRequest{TxnID: a, Database: "testdb"})
	require.True(t, abort.Success, "abort failed: %s", abort.Error)
	require.True(t, prepareClaim(engine, t, b, 2, 500, 20).Success)
	require.True(t, commitClaim(engine, b).Success)

	err = autoIncClaimStoreForTest(dm).ApplyClaims("testdb", a, intentsFrom{MetaStore: mdb.GetMetaStore(), intents: captured})
	require.True(t, errors.Is(err, ErrAutoIncClaimNotApplicable), "this node applied two overlapping claims: err=%v", err)
	require.Equal(t, uint64(520), readClaimBase(t, dm, "testdb", "users"))
}

// TestCommitOfAClaimAppliedBeforeAFailedCommitIsACKed: a COMMIT applied the
// claim in the system database and then failed before the user-database
// commit (its writer busy, or a crash), leaving the transaction PENDING. The
// transaction was decided COMMITTED, so committing it again - a retried
// COMMIT, or the log puller committing it because a peer's log holds it -
// must succeed rather than be refused for the floor its own claim raised.
//
// Mutation: drop claimAlreadyApplied from applyClaimTx. The second commit is
// refused and "a claim this node already applied was refused" fires.
func TestCommitOfAClaimAppliedBeforeAFailedCommitIsACKed(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const a = 9560
	require.True(t, prepareClaim(engine, t, a, 1, 500, 10).Success)
	require.NoError(t, autoIncClaimStoreForTest(dm).ApplyClaims("testdb", a, mdb.GetMetaStore()))

	res := commitClaim(engine, a)
	require.True(t, res.Success, "a claim this node already applied was refused: %s", res.Error)
	require.Equal(t, uint64(510), readClaimBase(t, dm, "testdb", "users"))
}

// TestApplyClaimsIsConditionedOnTheRangeItWrites: the COMMIT's conditional
// write guards the range it hands out, newBase+1..newBase+size, so it must not
// depend on PREPARE having admitted only newBase == prevBase. Here the intent
// the COMMIT reads carries a newBase below the stored base (500): applying it
// would lower the base to 110 and re-issue ids 101..110.
//
// Mutation: condition the UPDATE on the claim's prevBase instead of its
// newBase. The apply succeeds and "a claim whose range starts below the stored
// base was applied" fires.
func TestApplyClaimsIsConditionedOnTheRangeItWrites(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	seedClaimBase(t, dm, "testdb", "users", 500, 1)

	const a = 9550
	require.True(t, prepareClaim(engine, t, a, 1, 500, 10).Success)
	captured, err := mdb.GetMetaStore().GetIntentsByTxn(a)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	low := *captured[0]
	low.DataSnapshot, err = protocol.EncodeAutoIncClaim(protocol.AutoIncClaim{Table: "users", PrevBase: 500, NewBase: 100, Size: 10})
	require.NoError(t, err)

	err = autoIncClaimStoreForTest(dm).ApplyClaims("testdb", a, intentsFrom{MetaStore: mdb.GetMetaStore(), intents: []*WriteIntentRecord{&low}})
	require.True(t, errors.Is(err, ErrAutoIncClaimNotApplicable), "a claim whose range starts below the stored base was applied: err=%v", err)
	require.Equal(t, uint64(500), readClaimBase(t, dm, "testdb", "users"))
}

// TestDeadCoordinatorsPreparedRowIsFreedByTheStaleTransactionGC is the
// liveness side of TestPreparedDMLIsNotEvictedByAStaleHeartbeat. A second
// writer no longer evicts a prepared transaction whose coordinator has gone
// quiet; it waits for the stale-transaction GC to abort it. The GC judges
// staleness by the transaction's LastHeartbeat, which is set at BEGIN and
// never refreshed for a participant, so a prepared transaction is aborted by
// the first GC pass that runs once it is heartbeat_timeout old - at most
// heartbeat_timeout + gc_interval after its PREPARE. The pass is driven
// directly here so the test does not race the GC goroutine.
//
// Mutation: make MemoryMetaStore.isStale never treat a
// pending transaction as stale. "the GC did not abort a prepared transaction
// past heartbeat_timeout" fires. (Dropping only its DeleteIntentsByTxn does
// not strand the row: once the record is aborted, the next writer's conflict
// resolution overwrites the intent.)
func TestDeadCoordinatorsPreparedRowIsFreedByTheStaleTransactionGC(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
	tm := mdb.GetTransactionManager()
	tm.StopGarbageCollection()
	const heartbeatTimeout = 200 * time.Millisecond
	tm.heartbeatTimeout = heartbeatTimeout

	prepareRow := func(txnID uint64, v string) *PrepareResult {
		return engine.Prepare(context.Background(), &PrepareRequest{
			TxnID:      txnID,
			NodeID:     1,
			StartTS:    hlc.Timestamp{WallTime: int64(txnID)},
			Database:   "testdb",
			Statements: []protocol.Statement{dmlLogInsertStatement("testdb", "log:1", 1, v)},
		})
	}

	const dead, b1, b2 = 9600, 9601, 9602
	prepared := time.Now()
	require.True(t, prepareRow(dead, "dead").Success)
	require.False(t, prepareRow(b1, "b").Success, "a live prepared row was taken")

	// A pass before the timeout leaves the prepared transaction alone.
	cleaned, err := tm.cleanupStaleTransactions()
	require.NoError(t, err)
	require.Zero(t, cleaned, "the GC aborted a prepared transaction younger than heartbeat_timeout")

	time.Sleep(heartbeatTimeout - time.Since(prepared) + 50*time.Millisecond)
	cleaned, err = tm.cleanupStaleTransactions()
	require.NoError(t, err)
	require.Equal(t, 1, cleaned, "the GC did not abort a prepared transaction past heartbeat_timeout")

	prepB := prepareRow(b2, "b")
	require.True(t, prepB.Success, "the GC abort did not free the row: %s", prepB.Error)
	t.Logf("row freed %v after the dead coordinator's PREPARE (heartbeat_timeout %v, GC pass driven directly)",
		time.Since(prepared).Round(time.Millisecond), heartbeatTimeout)

	// The aborted transaction's COMMIT, should its coordinator return, is refused.
	res := engine.Commit(context.Background(), &CommitRequest{
		TxnID:      dead,
		Database:   "testdb",
		Statements: []protocol.Statement{dmlLogCommitStatement("testdb", "log:1")},
	})
	require.False(t, res.Success, "a GC-aborted transaction was committed")
}

// TestCommitRefusesADMLTransactionWhoseRowsTheGCDeleted replays a COMMIT
// racing the stale-transaction GC on one node. The COMMIT has loaded the
// transaction while it was still pending (engine.Commit ->
// TransactionManager.GetTransaction); the GC then deletes the transaction's
// intents and captured rows (MemoryMetaStore.AbortStaleTransaction, in its
// own order), and the COMMIT runs before the GC's abort reaches the record.
// The COMMIT used to find no rows, take the statement branch, write a commit
// record and ACK a write it never applied.
//
// Mutation: drop the statementsNamePreparedRows refusal in CommitTransaction.
// "a COMMIT whose prepared rows the GC deleted was ACKed" fires.
func TestCommitRefusesADMLTransactionWhoseRowsTheGCDeleted(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE log (id INTEGER PRIMARY KEY, v TEXT)")
	tm := mdb.GetTransactionManager()
	tm.StopGarbageCollection()

	const txnID = 9740
	prep := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID:      txnID,
		NodeID:     1,
		StartTS:    hlc.Timestamp{WallTime: txnID},
		Database:   "testdb",
		Statements: []protocol.Statement{dmlLogInsertStatement("testdb", "log:1", 1, "gc")},
	})
	require.True(t, prep.Success, "prepare: %s", prep.Error)

	txn := tm.GetTransaction(txnID)
	require.NotNil(t, txn)
	txn.Statements = []protocol.Statement{dmlLogCommitStatement("testdb", "log:1")}

	ms := mdb.GetMetaStore()
	require.NoError(t, ms.DeleteIntentsByTxn(txnID))
	require.NoError(t, ms.DeleteIntentEntries(txnID))

	err := tm.CommitTransaction(txn)
	require.ErrorIs(t, err, ErrPreparedRowsMissing, "a COMMIT whose prepared rows the GC deleted was ACKed")

	var n int
	require.NoError(t, mdb.GetReadDB().QueryRow("SELECT COUNT(*) FROM log").Scan(&n))
	require.Zero(t, n)
}
