//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

// Regression test for handleCommit's empty-transaction fast path silently
// masquerading a lost pinned transaction as a successful empty COMMIT.
//
// TakeAndReleasePinnedStateForTest (coordinator/vec_testexport_test.go)
// simulates the pinned state being taken and released by something other
// than this COMMIT - e.g. a concurrent forward-session eviction calling
// CoordinatorHandler.CloseSession - which is exactly the class of loss the
// grpc.closeRemovedForwardSession execMu fix now prevents in production,
// but which this fast path must also refuse to paper over as defense in
// depth.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

type lostPinnedStateReplicator struct{}

func (*lostPinnedStateReplicator) ReplicateTransaction(
	_ context.Context,
	_ uint64,
	_ *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	return &coordinator.ReplicationResponse{Success: true}, nil
}

type lostPinnedStateSetup struct {
	handler *coordinator.CoordinatorHandler
	session *protocol.ConnectionSession
	conn    *sql.DB
}

func setupLostPinnedState(t *testing.T) *lostPinnedStateSetup {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })

	const dbName = "lostpinned"
	require.NoError(t, dbMgr.CreateDatabase(dbName))

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	writeCoord := coordinator.NewWriteCoordinator(
		1,
		nodeProvider,
		&lostPinnedStateReplicator{},
		db.NewLocalReplicator(1, dbMgr, clock),
		10*time.Second,
		clock,
	)
	readCoord := coordinator.NewReadCoordinator(1, nodeProvider, db.NewLocalReader(dbMgr), 10*time.Second)

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

	session := &protocol.ConnectionSession{
		ConnID:               1,
		CurrentDatabase:      dbName,
		TranspilationEnabled: true,
	}

	_, err = handler.HandleQuery(session, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", nil)
	require.NoError(t, err)

	conn, err := dbMgr.GetDatabaseConnection(dbName)
	require.NoError(t, err)

	return &lostPinnedStateSetup{handler: handler, session: session, conn: conn}
}

// TestCommitFailsLoudWhenPinnedStateLost pins the fix: if eager DML pinned
// state for this transaction but that state is gone by the time COMMIT
// reads it - with no buffered statements and no pinned state left, exactly
// what a legitimately empty transaction looks like - COMMIT must return an
// error, never a silent OK, because the write may have already executed and
// be unrecoverably lost.
func TestCommitFailsLoudWhenPinnedStateLost(t *testing.T) {
	s := setupLostPinnedState(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	_, err = s.handler.HandleQuery(s.session, "INSERT INTO t (name) VALUES ('lost')", nil)
	require.NoError(t, err)

	// Simulate a concurrent eviction taking and discarding the pinned state
	// out from under this transaction, without going through this session's
	// own COMMIT/ROLLBACK - the same effect grpc/forward_session.go's old,
	// unsynchronized closeRemovedForwardSession had on an in-flight COMMIT.
	took := s.handler.TakeAndReleasePinnedStateForTest(s.session.ConnID)
	require.True(t, took, "INSERT must have pinned transaction state")

	require.True(t, s.session.InTransaction(), "the race leaves COMMIT still seeing an open transaction")

	_, err = s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.Error(t, err, "COMMIT must fail loud when its pinned state vanished instead of silently reporting OK")

	require.False(t, s.session.InTransaction(), "COMMIT must still end the session's transaction even when it errors")

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE name = 'lost'").Scan(&count))
	require.Equal(t, 0, count, "the discarded write must not have been applied")
}

// TestCommitEmptyTransactionStillNoop guards that a transaction which never
// pinned any state (BEGIN immediately followed by COMMIT, or one that only
// ran no-op DML) keeps working exactly as before: COMMIT is a real no-op,
// not an error.
func TestCommitEmptyTransactionStillNoop(t *testing.T) {
	s := setupLostPinnedState(t)

	_, err := s.handler.HandleQuery(s.session, "BEGIN", nil)
	require.NoError(t, err)

	res, err := s.handler.HandleQuery(s.session, "COMMIT", nil)
	require.NoError(t, err, "a transaction that never pinned any state must commit as a plain no-op")
	require.Nil(t, res)
	require.False(t, s.session.InTransaction())
}
