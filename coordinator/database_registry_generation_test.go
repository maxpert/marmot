//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// setupDatabaseOpHandler builds a single-node CoordinatorHandler over a real
// DatabaseManager, so CREATE/DROP DATABASE runs the real stamping
// (handleMutation), PREPARE gate (ReplicationEngine.prepareDatabaseOperation)
// and COMMIT merge (DatabaseManager.ApplyDatabaseOp) - not a mock of any of
// them. The single node's own write goes through db.LocalReplicator, which
// calls the real ReplicationEngine in-process.
func setupDatabaseOpHandler(t testing.TB) (*coordinator.CoordinatorHandler, *db.DatabaseManager, *protocol.ConnectionSession) {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })
	require.NoError(t, dbMgr.MergeAutoIncBasesAndReleaseVotes(nil))

	_, err = dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	schemaVersionMgr := db.NewSchemaVersionManager(dbMgr)

	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	dbMgr.SetClusterMembership(nodeProvider.GetTotalMembershipSize)

	writeCoord := coordinator.NewWriteCoordinator(
		1,
		nodeProvider,
		&countingReplicator{},
		db.NewLocalReplicator(1, dbMgr, clock),
		10*time.Second,
		clock,
	)
	readCoord := coordinator.NewReadCoordinator(1, nodeProvider, db.NewLocalReader(dbMgr), 10*time.Second)

	handler := coordinator.NewCoordinatorHandler(
		1, writeCoord, readCoord, clock, dbMgr,
		coordinator.NewDDLLockManager(30*time.Second),
		schemaVersionMgr,
		noopNodeRegistry{},
	)

	session := &protocol.ConnectionSession{ConnID: 1, TranspilationEnabled: true}
	return handler, dbMgr, session
}

// TestCoordinatorStampsDatabaseGeneration_CreateAbsentIsGenerationOne pins
// that CREATE DATABASE on a name never seen stamps and commits generation 1,
// live.
func TestCoordinatorStampsDatabaseGeneration_CreateAbsentIsGenerationOne(t *testing.T) {
	handler, dbMgr, session := setupDatabaseOpHandler(t)

	_, err := handler.HandleQuery(session, "CREATE DATABASE gentest1", nil)
	require.NoError(t, err)

	key, err := dbMgr.RegistryKey("gentest1")
	require.NoError(t, err)
	require.Equal(t, db.DatabaseRegistryKey{Generation: 1, Dropped: false}, key)
}

// TestCoordinatorStampsDatabaseGeneration_CreateLiveIsNoOpGeneration pins that
// a second CREATE DATABASE on an already-live name stamps and commits the
// same generation (a no-op), never advancing it.
func TestCoordinatorStampsDatabaseGeneration_CreateLiveIsNoOpGeneration(t *testing.T) {
	handler, dbMgr, session := setupDatabaseOpHandler(t)

	_, err := handler.HandleQuery(session, "CREATE DATABASE gentest2", nil)
	require.NoError(t, err)

	_, err = handler.HandleQuery(session, "CREATE DATABASE gentest2", nil)
	require.NoError(t, err)

	key, err := dbMgr.RegistryKey("gentest2")
	require.NoError(t, err)
	require.Equal(t, db.DatabaseRegistryKey{Generation: 1, Dropped: false}, key)
}

// TestCoordinatorStampsDatabaseGeneration_DropIsCurrentGeneration pins that
// DROP DATABASE stamps and commits the database's current generation,
// tombstoned.
func TestCoordinatorStampsDatabaseGeneration_DropIsCurrentGeneration(t *testing.T) {
	handler, dbMgr, session := setupDatabaseOpHandler(t)

	_, err := handler.HandleQuery(session, "CREATE DATABASE gentest3", nil)
	require.NoError(t, err)

	_, err = handler.HandleQuery(session, "DROP DATABASE gentest3", nil)
	require.NoError(t, err)

	key, err := dbMgr.RegistryKey("gentest3")
	require.NoError(t, err)
	require.Equal(t, db.DatabaseRegistryKey{Generation: 1, Dropped: true}, key)
	require.False(t, dbMgr.DatabaseExists("gentest3"))
}

// TestCoordinatorStampsDatabaseGeneration_CreateTombstonedIsNextGeneration
// pins that CREATE DATABASE on a tombstoned name stamps and commits one
// generation above the tombstone, live - so the re-created database's
// generation always outranks the drop it followed.
func TestCoordinatorStampsDatabaseGeneration_CreateTombstonedIsNextGeneration(t *testing.T) {
	handler, dbMgr, session := setupDatabaseOpHandler(t)

	_, err := handler.HandleQuery(session, "CREATE DATABASE gentest4", nil)
	require.NoError(t, err)
	_, err = handler.HandleQuery(session, "DROP DATABASE gentest4", nil)
	require.NoError(t, err)

	_, err = handler.HandleQuery(session, "CREATE DATABASE gentest4", nil)
	require.NoError(t, err)

	key, err := dbMgr.RegistryKey("gentest4")
	require.NoError(t, err)
	require.Equal(t, db.DatabaseRegistryKey{Generation: 2, Dropped: false}, key)
	require.True(t, dbMgr.DatabaseExists("gentest4"))
}
