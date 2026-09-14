//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// ensureDBReplicator wraps a countingReplicator-shaped counter with a record
// of how many PREPARE calls carried a StatementCreateDatabase statement. It
// plays the role of the REMOTE replicator (peer node 2) in a two-node
// cluster, so that unlike a single-node harness (where the remote replicator
// is never dialed at all - see write_coordinator.go's otherNodes handling)
// "prepares" is a real signal of 2PC activity rather than a value that is
// always zero regardless of behavior.
type ensureDBReplicator struct {
	prepares            int
	commits             int
	createDatabaseCalls int
}

func (e *ensureDBReplicator) ReplicateTransaction(
	ctx context.Context,
	nodeID uint64,
	req *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	switch req.Phase {
	case coordinator.PhasePrep:
		e.prepares++
		for _, stmt := range req.Statements {
			if stmt.Type == protocol.StatementCreateDatabase {
				e.createDatabaseCalls++
			}
		}
	case coordinator.PhaseCommit:
		e.commits++
	}
	return &coordinator.ReplicationResponse{Success: true}, nil
}

// alwaysConflictingReplicator reports ConflictDetected on every PREPARE, so
// the underlying CREATE DATABASE 2PC round never reaches quorum and
// DatabaseExists never becomes true. It plays the role of a peer node whose
// concurrent auto-create of the same database keeps colliding with this
// node's own attempt - the persistent-conflict scenario EnsureDatabase's
// bounded retry exists to survive without looping forever.
type alwaysConflictingReplicator struct {
	prepares int
}

func (a *alwaysConflictingReplicator) ReplicateTransaction(
	ctx context.Context,
	nodeID uint64,
	req *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	if req.Phase == coordinator.PhasePrep {
		a.prepares++
		return &coordinator.ReplicationResponse{
			Success:          false,
			ConflictDetected: true,
			ConflictDetails:  "simulated persistent write-write conflict",
		}, nil
	}
	return &coordinator.ReplicationResponse{Success: true}, nil
}

// ensureDBSetup builds a real two-node-cluster-shaped CoordinatorHandler: node
// 1 (self) runs a real db.DatabaseManager so DatabaseExists/CreateDatabase and
// the full 2PC apply path run for real, node 2 is a mock remote peer served by
// ensureDBReplicator. Two nodes (rather than noop_dml_test.go's single-node
// harness) are required so a real PREPARE round trip happens and
// "prepares"/"createDatabaseCalls" are meaningful counters instead of values
// that are always zero.
type ensureDBSetup struct {
	handler    *coordinator.CoordinatorHandler
	dbMgr      *db.DatabaseManager
	replicator *ensureDBReplicator
}

// newEnsureDBHandler builds a real two-node-cluster-shaped CoordinatorHandler
// (node 1 = self with a real db.DatabaseManager, node 2 = the given mock
// remote replicator) - the wiring shared by every EnsureDatabase test in this
// file, parameterized on the remote replicator so different tests can drive
// different PREPARE outcomes (a normal accepting peer, or one that always
// reports a conflict).
func newEnsureDBHandler(t *testing.T, replicator coordinator.Replicator) (*coordinator.CoordinatorHandler, *db.DatabaseManager) {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1, 2})

	writeCoord := coordinator.NewWriteCoordinator(
		1,
		nodeProvider,
		replicator,
		db.NewLocalReplicator(1, dbMgr, clock),
		10*time.Second,
		clock,
	)
	readCoord := coordinator.NewReadCoordinator(
		1,
		nodeProvider,
		db.NewLocalReader(dbMgr),
		10*time.Second,
	)

	handler := coordinator.NewCoordinatorHandler(
		1,
		writeCoord,
		readCoord,
		clock,
		dbMgr,
		coordinator.NewDDLLockManager(30*time.Second),
		schemaVersionMgr,
		noopNodeRegistry{},
	)

	return handler, dbMgr
}

func setupEnsureDB(t *testing.T) *ensureDBSetup {
	t.Helper()

	replicator := &ensureDBReplicator{}
	handler, dbMgr := newEnsureDBHandler(t, replicator)

	return &ensureDBSetup{handler: handler, dbMgr: dbMgr, replicator: replicator}
}

// withAutoCreateDatabase sets cfg.Config.MySQL.AutoCreateDatabase for one test
// and restores it after. Not run in parallel (mirrors withDDLImplicitCommit /
// noop_dml_test.go's convention in this package): cfg.Config is global mutable
// state, and no test in this package marks itself t.Parallel() while touching it.
func withAutoCreateDatabase(t *testing.T, enabled bool) {
	t.Helper()
	prev := cfg.Config.MySQL.AutoCreateDatabase
	cfg.Config.MySQL.AutoCreateDatabase = enabled
	t.Cleanup(func() { cfg.Config.MySQL.AutoCreateDatabase = prev })
}

// withDDLValidationTimeoutMS overrides the DDL validation timeout
// EnsureDatabase's retry deadline derives from (coordinator.getDDLValidationTimeout),
// so a bounded-retry test does not have to pay the full production default
// (60s) to observe the deadline actually being enforced. Same non-parallel
// convention as withAutoCreateDatabase.
func withDDLValidationTimeoutMS(t *testing.T, ms int) {
	t.Helper()
	prev := cfg.Config.Replication.DDLValidationTimeoutMS
	cfg.Config.Replication.DDLValidationTimeoutMS = ms
	t.Cleanup(func() { cfg.Config.Replication.DDLValidationTimeoutMS = prev })
}

func newSession(connID uint64, currentDB string) *protocol.ConnectionSession {
	return &protocol.ConnectionSession{
		ConnID:               connID,
		CurrentDatabase:      currentDB,
		TranspilationEnabled: true,
	}
}

// Arm 1: EnsureDatabase on an existing database is a no-op and never touches
// replication.
func TestEnsureDatabase_ExistingDatabase_NoReplication(t *testing.T) {
	withAutoCreateDatabase(t, true)
	s := setupEnsureDB(t)
	require.NoError(t, s.dbMgr.CreateDatabase("existingdb"))
	before := s.replicator.prepares

	session := newSession(1, "")
	err := s.handler.EnsureDatabase(session, "existingdb")

	require.NoError(t, err)
	require.Equal(t, before, s.replicator.prepares, "existing database must not trigger any 2PC")
}

// Arm 2: EnsureDatabase on a missing database with AutoCreateDatabase=true
// creates it through the real replicated DDL path - exactly one PREPARE
// call, and that call carries a StatementCreateDatabase. This is the
// recursion safety proof: EnsureDatabase's own call into HandleQuery for the
// CREATE DATABASE statement must not re-enter EnsureDatabase and must not
// duplicate the create.
func TestEnsureDatabase_MissingDatabase_AutoCreateOn(t *testing.T) {
	withAutoCreateDatabase(t, true)
	s := setupEnsureDB(t)
	require.False(t, s.dbMgr.DatabaseExists("newdb"))

	session := newSession(1, "")
	err := s.handler.EnsureDatabase(session, "newdb")

	require.NoError(t, err)
	require.True(t, s.dbMgr.DatabaseExists("newdb"))
	require.Equal(t, 1, s.replicator.prepares, "exactly one 2PC round for the auto-create")
	require.Equal(t, 1, s.replicator.createDatabaseCalls, "exactly one CREATE DATABASE statement replicated, proving no recursive duplicate create")
}

// Arm 3: EnsureDatabase on a missing database with AutoCreateDatabase=false
// fails fast with 1049 before ever touching replication.
func TestEnsureDatabase_MissingDatabase_AutoCreateOff(t *testing.T) {
	withAutoCreateDatabase(t, false)
	s := setupEnsureDB(t)
	before := s.replicator.prepares

	session := newSession(1, "")
	err := s.handler.EnsureDatabase(session, "newdb")

	require.Error(t, err)
	var mysqlErr *protocol.MySQLError
	require.True(t, errors.As(err, &mysqlErr), "error must be a *protocol.MySQLError")
	require.Equal(t, protocol.ErrCodeBadDB, mysqlErr.Code)
	require.False(t, s.dbMgr.DatabaseExists("newdb"))
	require.Equal(t, before, s.replicator.prepares, "auto-create disabled must fail before touching 2PC")
}

// Arm 4: a reserved system schema name is virtual - EnsureDatabase accepts it
// (nil, not 1049) without ever creating it, even with AutoCreateDatabase=true.
// This is R2's ruling: real MySQL servers always have information_schema, and
// clients/tools select or query it regardless of what real databases exist, so
// the reserved-name guard must short-circuit before (and independent of) the
// auto-create knob. Includes the mixed-case arm (LOW-2) and the safety count
// (zero create requests replicated) required by R2.
func TestEnsureDatabase_ReservedSchemaName_NeverAutoCreated(t *testing.T) {
	withAutoCreateDatabase(t, true)
	s := setupEnsureDB(t)
	before := s.replicator.prepares

	session := newSession(1, "")

	err := s.handler.EnsureDatabase(session, "information_schema")
	require.NoError(t, err)
	require.False(t, s.dbMgr.DatabaseExists("information_schema"))

	// Mixed-case arm (LOW-2): the reserved-schema comparison is case-insensitive.
	err = s.handler.EnsureDatabase(session, "Information_Schema")
	require.NoError(t, err)
	require.False(t, s.dbMgr.DatabaseExists("Information_Schema"))
	require.False(t, s.dbMgr.DatabaseExists("information_schema"))

	require.Equal(t, before, s.replicator.prepares, "reserved schema names must never trigger a create attempt (safety)")
}

// R2: EnsureDatabase("mysql") - the handshake `-D mysql` / COM_INIT_DB shape -
// succeeds without creating anything, in BOTH auto_create_database knob
// positions, with zero create requests replicated in either (safety).
func TestEnsureDatabase_Mysql_SucceedsBothKnobPositions_NeverCreated(t *testing.T) {
	for _, knob := range []bool{true, false} {
		t.Run(fmt.Sprintf("autoCreateDatabase=%v", knob), func(t *testing.T) {
			withAutoCreateDatabase(t, knob)
			s := setupEnsureDB(t)
			before := s.replicator.prepares

			session := newSession(1, "")
			err := s.handler.EnsureDatabase(session, "mysql")

			require.NoError(t, err)
			require.False(t, s.dbMgr.DatabaseExists("mysql"))
			require.Equal(t, before, s.replicator.prepares, "reserved schema must never trigger 2PC (safety)")
		})
	}
}

// R2: a JOIN across two information_schema tables must not be rejected with
// 1049. isInformationSchemaQuery (protocol/parser_vitess.go) only inspects
// top-level *AliasedTableExpr entries in sel.From, so a JOIN is typed a plain
// StatementSelect rather than StatementInformationSchema, while
// extractDatabaseFromTableExpr (which sets stmt.Database) DOES recurse into
// *JoinTableExpr - so this shape reaches HandleQuery's fail-fast existence
// check carrying Database="information_schema", which DatabaseExists never
// reports true for (reserved schemas are never created). Without the
// protocol.IsReservedSystemSchema exemption this always 1049s; with it, the
// query falls through to the same read path it reached before the fail-fast
// check existed at all. The actual resulting code is measured, not assumed:
// protocol.ConvertToMySQLError is the same conversion protocol/server.go
// applies to any non-*MySQLError before writing it to the wire.
func TestHandleQuery_ReservedSchemaJoin_NotRejectedAsBadDB(t *testing.T) {
	withAutoCreateDatabase(t, false)
	s := setupEnsureDB(t)
	session := newSession(1, "")

	sql := "SELECT t.table_name, c.column_name FROM information_schema.tables t " +
		"JOIN information_schema.columns c ON t.table_name = c.table_name"
	_, err := s.handler.HandleQuery(session, sql, nil)

	require.Error(t, err, "information_schema is never created, so the underlying read still fails")
	mysqlErr := protocol.ConvertToMySQLError(err)
	require.NotEqual(t, protocol.ErrCodeBadDB, mysqlErr.Code, "reserved-schema JOIN must not be rejected with 1049 - that would refuse every deployment permanently")
	require.Equal(t, protocol.ErrCodeUnknown, mysqlErr.Code, "measured: falls through to the pre-existing read path, which reports a generic 1105 (ER_UNKNOWN_ERROR), same as before the fail-fast check existed")
}

// R2: a plain SELECT with session.CurrentDatabase set to a reserved schema
// (the "mysql -D mysql" shape after a successful handshake) must behave the
// same way - not 1049 - since HandleQuery folds session.CurrentDatabase into
// stmt.Database before the fail-fast check runs.
func TestHandleQuery_SelectOne_ReservedCurrentDatabase_NotRejectedAsBadDB(t *testing.T) {
	withAutoCreateDatabase(t, false)
	s := setupEnsureDB(t)
	session := newSession(1, "mysql")

	_, err := s.handler.HandleQuery(session, "SELECT 1", nil)

	require.Error(t, err)
	mysqlErr := protocol.ConvertToMySQLError(err)
	require.NotEqual(t, protocol.ErrCodeBadDB, mysqlErr.Code, "SELECT against CurrentDatabase=mysql must not be rejected with 1049")
	require.Equal(t, protocol.ErrCodeUnknown, mysqlErr.Code, "measured: same fall-through path as the JOIN arm above")
}

// Arm 5 (true): USE against a missing database auto-creates it and switches
// the session's current database, when routed through the SQL-level
// HandleQuery("USE ...") entrypoint the wire protocol also drives.
func TestHandleUseDatabase_AutoCreateOn_CreatesAndSwitches(t *testing.T) {
	withAutoCreateDatabase(t, true)
	s := setupEnsureDB(t)
	session := newSession(1, "")

	_, err := s.handler.HandleQuery(session, "USE somedb", nil)

	require.NoError(t, err)
	require.Equal(t, "somedb", session.CurrentDatabase)
	require.True(t, s.dbMgr.DatabaseExists("somedb"))
}

// Arm 5 (false): USE against a missing database fails with 1049 and leaves
// the session's current database untouched, when AutoCreateDatabase is off.
func TestHandleUseDatabase_AutoCreateOff_Fails(t *testing.T) {
	withAutoCreateDatabase(t, false)
	s := setupEnsureDB(t)
	session := newSession(1, "originaldb")

	_, err := s.handler.HandleQuery(session, "USE somedb", nil)

	require.Error(t, err)
	var mysqlErr *protocol.MySQLError
	require.True(t, errors.As(err, &mysqlErr), "error must be a *protocol.MySQLError")
	require.Equal(t, protocol.ErrCodeBadDB, mysqlErr.Code)
	require.Equal(t, "originaldb", session.CurrentDatabase, "failed USE must not change the session's current database")
}

// Arm 6: the HandleQuery fail-fast check rejects DDL against a database that
// was never created, before touching 2PC at all - the actual bug-report
// scenario (a client mutating a database name that was never selected via a
// successful USE/handshake).
func TestHandleQuery_FailFast_UnknownDatabaseDDL(t *testing.T) {
	withAutoCreateDatabase(t, false)
	s := setupEnsureDB(t)
	before := s.replicator.prepares

	session := newSession(1, "nosuchdb")
	_, err := s.handler.HandleQuery(session, "CREATE TABLE t (id INTEGER)", nil)

	require.Error(t, err)
	var mysqlErr *protocol.MySQLError
	require.True(t, errors.As(err, &mysqlErr), "error must be a *protocol.MySQLError")
	require.Equal(t, protocol.ErrCodeBadDB, mysqlErr.Code)
	require.Equal(t, before, s.replicator.prepares, "fail-fast rejection must happen before touching 2PC")
}

// Arm 7: recursion end-to-end through the real SQL-level HandleQuery path.
// session.CurrentDatabase names a database that does not exist yet;
// HandleQuery("USE ...") drives handleUseDatabase -> EnsureDatabase ->
// HandleQuery(CREATE DATABASE ...) recursively. This must terminate (not
// hang/infinite-loop) and must replicate exactly one CREATE DATABASE PREPARE
// - the strongest form of the recursion safety proof.
func TestHandleQuery_UseDatabase_RecursionTerminates(t *testing.T) {
	withAutoCreateDatabase(t, true)
	s := setupEnsureDB(t)

	session := newSession(1, "")
	done := make(chan error, 1)
	go func() {
		_, err := s.handler.HandleQuery(session, "USE thatname", nil)
		done <- err
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("HandleQuery(USE ...) did not terminate - possible EnsureDatabase recursion")
	}

	require.True(t, s.dbMgr.DatabaseExists("thatname"))
	require.Equal(t, 1, s.replicator.createDatabaseCalls, "exactly one CREATE DATABASE must be replicated despite the recursive EnsureDatabase->HandleQuery call")
}

// Arm 8 / R1(e): when the underlying CREATE DATABASE keeps hitting a real
// write-write conflict - simulated here by a remote peer that reports
// ConflictDetected on every PREPARE, so quorum never forms and
// DatabaseExists never becomes true - EnsureDatabase must not retry
// unboundedly. The bound is now a DEADLINE (getDDLValidationTimeout,
// overridden here so the test is fast), not an attempt count, so this test
// asserts the deadline is honored AND - the MEDIUM-3 discriminator -
// require.Greater(prepares, 1): an EnsureDatabase that gives up on the first
// attempt without retrying at all must NOT be able to pass this test. See the
// deliverable for the arm V rerun proving this fires.
func TestEnsureDatabase_PersistentConflict_BoundedRetry(t *testing.T) {
	withAutoCreateDatabase(t, true)
	withDDLValidationTimeoutMS(t, 150)
	replicator := &alwaysConflictingReplicator{}
	handler, dbMgr := newEnsureDBHandler(t, replicator)

	session := newSession(1, "")
	start := time.Now()
	err := handler.EnsureDatabase(session, "neverexistsdb")
	elapsed := time.Since(start)

	require.Error(t, err, "EnsureDatabase must give up, not hang, when the create never succeeds")
	require.False(t, dbMgr.DatabaseExists("neverexistsdb"))
	require.Less(t, elapsed, 2*time.Second, "EnsureDatabase must return within a small bounded time after the deadline, not loop forever")
	require.Greater(t, replicator.prepares, 1, "must have actually retried past the first attempt before giving up")
}

// quorumFailReplicator reports a transport failure (not a conflict) on every
// PREPARE, as if the remote peer is unreachable, so quorum (2 of 2 in this
// harness) can never form. It never conflicts and never holds a lock, so
// EnsureDatabase's classification must treat it as non-retryable.
type quorumFailReplicator struct {
	mu       sync.Mutex
	prepares int
}

func (q *quorumFailReplicator) ReplicateTransaction(
	ctx context.Context,
	nodeID uint64,
	req *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	if req.Phase == coordinator.PhasePrep {
		q.mu.Lock()
		q.prepares++
		q.mu.Unlock()
		return nil, errors.New("simulated: remote node unreachable")
	}
	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (q *quorumFailReplicator) prepareCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.prepares
}

// R1(a): a quorum failure (a participant unreachable, not a write-write
// conflict) must fail EnsureDatabase fast on the first attempt with the real
// quorum error unchanged - never retried.
func TestEnsureDatabase_QuorumError_FailsFastNoRetry(t *testing.T) {
	withAutoCreateDatabase(t, true)
	replicator := &quorumFailReplicator{}
	handler, dbMgr := newEnsureDBHandler(t, replicator)

	session := newSession(1, "")
	start := time.Now()
	err := handler.EnsureDatabase(session, "quorumdb")
	elapsed := time.Since(start)

	require.Error(t, err)
	var quorumErr *coordinator.QuorumNotAchievedError
	require.True(t, errors.As(err, &quorumErr), "the error returned to the client must be the real quorum error, not a generic retry-exhausted error")
	require.False(t, dbMgr.DatabaseExists("quorumdb"))
	require.Equal(t, 1, replicator.prepareCount(), "a non-conflict, non-lock error must not be retried")
	require.Less(t, elapsed, 100*time.Millisecond, "must fail fast on attempt 1, not pay any backoff")
}

// R1(b/c): an unclassified *protocol.MySQLError distinct from the 1213
// conflict code must fail fast too. protocol.ErrReadOnly (1290) and a raw
// context.Canceled/DeadlineExceeded are not reachable through
// CoordinatorHandler's own code paths today - ErrReadOnly is only returned by
// replica.ReadOnlyHandler (a different package), and no path here surfaces a
// bare context error as WriteTransaction's top-level error rather than
// folding it into QuorumNotAchievedError (see the quorum arm above, which
// covers a remote context.Canceled/DeadlineExceeded's actual observable
// shape in this package) - so those two are proven directly against the
// classifier in TestIsRetryableEnsureDatabaseError
// (ensure_database_classify_test.go, package coordinator, white-box). This
// arm exercises the same "any unclassified MySQLError
// fails fast" property through the one such error CoordinatorHandler DOES
// return on its own: ErrServerShutdown (1053) from the draining gate, which
// HandleQuery checks before ever touching replication.
func TestEnsureDatabase_Draining_FailsFastNoRetry(t *testing.T) {
	withAutoCreateDatabase(t, true)
	s := setupEnsureDB(t)
	s.handler.SetDraining(true)
	t.Cleanup(func() { s.handler.SetDraining(false) })
	before := s.replicator.prepares

	session := newSession(1, "")
	start := time.Now()
	err := s.handler.EnsureDatabase(session, "drainingdb")
	elapsed := time.Since(start)

	require.Error(t, err)
	var mysqlErr *protocol.MySQLError
	require.True(t, errors.As(err, &mysqlErr), "error must be a *protocol.MySQLError")
	require.Equal(t, protocol.ErrCodeServerShutdown, mysqlErr.Code)
	require.False(t, s.dbMgr.DatabaseExists("drainingdb"))
	require.Equal(t, before, s.replicator.prepares, "draining rejection happens before 2PC and must not be retried")
	require.Less(t, elapsed, 100*time.Millisecond, "must fail fast on attempt 1")
}

// conflictThenSucceedReplicator reports a conflict on the first PREPARE it
// sees (simulating a concurrent winner mid-commit), then succeeds on every
// subsequent PREPARE - simulating that winner having finished and this
// retry now winning the race for real.
type conflictThenSucceedReplicator struct {
	mu       sync.Mutex
	prepares int
}

func (c *conflictThenSucceedReplicator) ReplicateTransaction(
	ctx context.Context,
	nodeID uint64,
	req *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	if req.Phase == coordinator.PhasePrep {
		c.mu.Lock()
		c.prepares++
		n := c.prepares
		c.mu.Unlock()
		if n == 1 {
			return &coordinator.ReplicationResponse{
				Success:          false,
				ConflictDetected: true,
				ConflictDetails:  "simulated: another auto-create is committing concurrently",
			}, nil
		}
	}
	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (c *conflictThenSucceedReplicator) prepareCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepares
}

// R1(d): the first CREATE DATABASE attempt hits a real write-write conflict
// (1213), the retry's second attempt succeeds for real. EnsureDatabase must
// return nil, the database must exist, and there must be EXACTLY two PREPARE
// attempts - not fewer (the conflict was real) and not more (recovery is
// immediate).
func TestEnsureDatabase_ConflictThenSucceeds_ExactlyTwoAttempts(t *testing.T) {
	withAutoCreateDatabase(t, true)
	replicator := &conflictThenSucceedReplicator{}
	handler, dbMgr := newEnsureDBHandler(t, replicator)

	session := newSession(1, "")
	err := handler.EnsureDatabase(session, "racedb")

	require.NoError(t, err)
	require.True(t, dbMgr.DatabaseExists("racedb"))
	require.Equal(t, 2, replicator.prepareCount(), "exactly one retry after the conflict, no more")
}

// slowCommitReplicator counts PREPARE calls and, on PhaseCommit, sleeps for
// commitDelay before acknowledging - simulating a remote COMMIT round trip
// slow enough to outlast a losing local caller's first retry backoff.
type slowCommitReplicator struct {
	commitDelay time.Duration

	mu       sync.Mutex
	prepares int
}

func (s *slowCommitReplicator) ReplicateTransaction(
	ctx context.Context,
	nodeID uint64,
	req *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	switch req.Phase {
	case coordinator.PhasePrep:
		s.mu.Lock()
		s.prepares++
		s.mu.Unlock()
	case coordinator.PhaseCommit:
		time.Sleep(s.commitDelay)
	}
	return &coordinator.ReplicationResponse{Success: true}, nil
}

func (s *slowCommitReplicator) prepareCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prepares
}

// R1(f): two goroutines race to auto-create the SAME missing database on ONE
// node (one shared *CoordinatorHandler). The remote replicator's COMMIT
// phase is slow enough (120ms) to outlast the first retry backoff (~20ms),
// so whichever goroutine loses the local DDL lock race gets the now-typed
// ErrDDLLockHeld while the winner is still mid-commit, and must retry rather
// than fail. Both goroutines must return nil, exactly ONE create request must
// have been replicated (safety - counted through the replicator's PREPARE
// seam, never by timing), and the database must exist afterward.
func TestEnsureDatabase_SameNodeRace_ExactlyOneCreate(t *testing.T) {
	withAutoCreateDatabase(t, true)
	withDDLValidationTimeoutMS(t, 5000)
	replicator := &slowCommitReplicator{commitDelay: 120 * time.Millisecond}
	handler, dbMgr := newEnsureDBHandler(t, replicator)

	const name = "racedbsamenode"
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			session := newSession(uint64(idx+1), "")
			errs[idx] = handler.EnsureDatabase(session, name)
		}(i)
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.True(t, dbMgr.DatabaseExists(name))
	require.Equal(t, 1, replicator.prepareCount(), "exactly one create must be replicated despite two concurrent local callers")
}
