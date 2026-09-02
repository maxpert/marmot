//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

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

// countingReplicator records how many transactions reached the 2PC path so a
// no-op DML can be proven not to replicate anything.
type countingReplicator struct {
	prepares int
	commits  int
}

func (c *countingReplicator) ReplicateTransaction(
	ctx context.Context,
	nodeID uint64,
	req *coordinator.ReplicationRequest,
) (*coordinator.ReplicationResponse, error) {
	switch req.Phase {
	case coordinator.PhasePrep:
		c.prepares++
	case coordinator.PhaseCommit:
		c.commits++
	}
	return &coordinator.ReplicationResponse{Success: true}, nil
}

// noopNodeRegistry satisfies coordinator.NodeRegistry for a single-node handler.
type noopNodeRegistry struct{}

func (noopNodeRegistry) UpdateSchemaVersions(map[string]uint64) {}
func (noopNodeRegistry) CountAlive() int                        { return 1 }
func (noopNodeRegistry) GetAll() []any                          { return nil }
func (noopNodeRegistry) IsLeaving(uint64) bool                  { return false }
func (noopNodeRegistry) GetLocalNodeID() uint64                 { return 1 }

// noopDMLSetup builds a single-node handler over a real DatabaseManager so the
// CDC preupdate hook runs for real.
type noopDMLSetup struct {
	handler    *coordinator.CoordinatorHandler
	session    *protocol.ConnectionSession
	replicator *countingReplicator
	conn       *sql.DB
}

func setupNoopDML(t *testing.T) *noopDMLSetup {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })

	const dbName = "noopdml"
	require.NoError(t, dbMgr.CreateDatabase(dbName))

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	schemaVersionMgr := db.NewSchemaVersionManager(systemDB.GetMetaStore())

	replicator := &countingReplicator{}
	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})

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

	// Real connections enable transpilation (protocol/server.go), and without it
	// BEGIN does not parse as transaction control, so a test meaning to exercise
	// the explicit-transaction path would silently run in autocommit.
	session := &protocol.ConnectionSession{
		ConnID:               1,
		CurrentDatabase:      dbName,
		TranspilationEnabled: true,
	}

	_, err = handler.HandleQuery(session, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", nil)
	require.NoError(t, err)

	res, err := handler.HandleQuery(session, "INSERT INTO t (id, name) VALUES (1, 'a')", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsAffected)

	conn, err := dbMgr.GetDatabaseConnection(dbName)
	require.NoError(t, err)

	return &noopDMLSetup{handler: handler, session: session, replicator: replicator, conn: conn}
}

// TestNoopDMLSucceeds pins that a DML matching zero rows is a successful no-op.
// It captures no CDC rows, and DML is never replicated as raw SQL, so the
// coordinator used to reject it with "DML statement missing encoded CDC row".
func TestNoopDMLSucceeds(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"delete matching nothing", "DELETE FROM t WHERE id = 999"},
		{"update matching nothing", "UPDATE t SET name = 'z' WHERE id = 999"},
		{"insert or ignore on existing key", "INSERT OR IGNORE INTO t (id, name) VALUES (1, 'a')"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := setupNoopDML(t)
			before := s.replicator.prepares

			res, err := s.handler.HandleQuery(s.session, tc.sql, nil)
			require.NoError(t, err, "no-op DML must not fail the transaction")
			require.NotNil(t, res)
			require.Equal(t, int64(0), res.RowsAffected, "no-op DML affects zero rows")
			require.Equal(t, before, s.replicator.prepares, "no-op DML must not run 2PC")
		})
	}
}

// TestNoopDMLLeavesDataIntact guards against the no-op path dropping a statement
// that actually changed rows.
func TestNoopDMLLeavesDataIntact(t *testing.T) {
	s := setupNoopDML(t)

	_, err := s.handler.HandleQuery(s.session, "DELETE FROM t WHERE id = 999", nil)
	require.NoError(t, err)

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t").Scan(&count))
	require.Equal(t, 1, count, "no-op DELETE must not remove the existing row")

	res, err := s.handler.HandleQuery(s.session, "DELETE FROM t WHERE id = 1", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.RowsAffected)

	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t").Scan(&count))
	require.Equal(t, 0, count, "matching DELETE must still remove the row")
}

// TestNoopDMLInExplicitTransaction covers the multi-statement path, where a
// no-op statement is dropped from the group but siblings still replicate.
func TestNoopDMLInExplicitTransaction(t *testing.T) {
	s := setupNoopDML(t)

	for _, q := range []string{
		"BEGIN",
		"DELETE FROM t WHERE id = 999",
		"INSERT INTO t (id, name) VALUES (2, 'b')",
		"COMMIT",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoError(t, err, "query %q", q)
	}

	var count int
	require.NoError(t, s.conn.QueryRow("SELECT COUNT(*) FROM t WHERE id = 2").Scan(&count))
	require.Equal(t, 1, count, "sibling INSERT must survive the dropped no-op")
}

// TestAllNoopTransactionSkips2PC pins that a transaction whose statements all
// collapse to no-ops commits without a replication round.
func TestAllNoopTransactionSkips2PC(t *testing.T) {
	s := setupNoopDML(t)
	before := s.replicator.prepares

	for _, q := range []string{
		"BEGIN",
		"DELETE FROM t WHERE id = 998",
		"DELETE FROM t WHERE id = 999",
		"COMMIT",
	} {
		_, err := s.handler.HandleQuery(s.session, q, nil)
		require.NoError(t, err, "query %q", q)
	}

	require.Equal(t, before, s.replicator.prepares, "all-no-op transaction must not run 2PC")
}

// TestUnknownDatabaseDMLReportsDatabase pins that a DML against a database the
// manager cannot open reports that, rather than a misleading CDC error.
func TestUnknownDatabaseDMLReportsDatabase(t *testing.T) {
	s := setupNoopDML(t)

	session := &protocol.ConnectionSession{ConnID: 2, CurrentDatabase: "nosuchdb", TranspilationEnabled: true}

	_, err := s.handler.HandleQuery(session, "DELETE FROM t WHERE id = 1", nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "missing encoded CDC row")
	require.Contains(t, err.Error(), "nosuchdb")
}
