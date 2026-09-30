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
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/stretchr/testify/require"
)

// legacyAwareNodeRegistry satisfies coordinator.NodeRegistry with a
// configurable set of members that do not serve the commit-log pull protocol
// yet, for the rolling-upgrade DDL gate.
type legacyAwareNodeRegistry struct {
	legacy []uint64
}

func (legacyAwareNodeRegistry) UpdateSchemaVersions(map[string]uint64) {}
func (legacyAwareNodeRegistry) CountAlive() int                        { return 1 }
func (legacyAwareNodeRegistry) GetAll() []any                          { return nil }
func (legacyAwareNodeRegistry) IsLeaving(uint64) bool                  { return false }
func (legacyAwareNodeRegistry) GetLocalNodeID() uint64                 { return 1 }
func (r legacyAwareNodeRegistry) LegacyLogProtocolMembers() []uint64   { return r.legacy }

// setupUpgradeGateCoordinator builds a single-node handler over a real
// DatabaseManager, identical to setupNoopDML's construction, except the node
// registry's legacy members are configurable so both the refusal and
// acceptance paths of the rolling-upgrade DDL gate can be exercised end to
// end through CoordinatorHandler.HandleQuery.
func setupUpgradeGateCoordinator(t testing.TB, legacyMembers []uint64) (*coordinator.CoordinatorHandler, *protocol.ConnectionSession) {
	t.Helper()

	tmpDir := t.TempDir()
	clock := hlc.NewClock(1)

	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dbMgr.Close() })
	require.NoError(t, dbMgr.MergeAutoIncBasesAndReleaseVotes(nil))

	const dbName = "upgradegate"
	require.NoError(t, dbMgr.CreateDatabase(dbName))

	_, err = dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	schemaVersionMgr := db.NewSchemaVersionManager(dbMgr)

	replicator := &countingReplicator{}
	nodeProvider := coordinator.NewMockNodeProvider([]uint64{1})
	dbMgr.SetClusterMembership(nodeProvider.GetTotalMembershipSize)

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
		legacyAwareNodeRegistry{legacy: legacyMembers},
	)

	session := &protocol.ConnectionSession{
		ConnID:               1,
		CurrentDatabase:      dbName,
		TranspilationEnabled: true,
	}

	return handler, session
}

// TestCoordinator_RefusesDDLWhileALegacyMemberIsPresent pins that a
// coordinator refuses DDL cluster-wide, with the retryable 1213, while any
// member does not serve the commit-log pull protocol yet, naming those
// members: such a node counts a database's DDL history differently, and DDL
// run now would leave the schema versions of the two kinds of node apart for
// good.
func TestCoordinator_RefusesDDLWhileALegacyMemberIsPresent(t *testing.T) {
	handler, session := setupUpgradeGateCoordinator(t, []uint64{7, 3})

	_, err := handler.HandleQuery(session, "CREATE TABLE t (id INTEGER PRIMARY KEY)", nil)
	requireLegacyMembersRefusal(t, err, []uint64{7, 3})
}

// requireLegacyMembersRefusal asserts err is the rolling-upgrade DDL refusal
// naming legacy, carrying the retryable 1213.
func requireLegacyMembersRefusal(t *testing.T, err error, legacy []uint64) {
	t.Helper()
	require.ErrorIs(t, err, coordinator.ErrLegacyMembersPresent)
	var refusal *coordinator.LegacyMembersDDLRefusal
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, legacy, refusal.LegacyIDs, "the refusal must name the members still to upgrade")
	require.Equal(t, mysqlcode.ErrCodeDeadlock, protocol.ConvertToMySQLError(err).Code)
}

// TestCoordinator_RefusesCreateDatabaseWhileALegacyMemberIsPresent covers
// the other isDDL branch: CREATE/DROP DATABASE shares the same gate as DDL.
func TestCoordinator_RefusesCreateDatabaseWhileALegacyMemberIsPresent(t *testing.T) {
	handler, session := setupUpgradeGateCoordinator(t, []uint64{2})

	_, err := handler.HandleQuery(session, "CREATE DATABASE newdb", nil)
	requireLegacyMembersRefusal(t, err, []uint64{2})
}

// TestCoordinator_AllowsDDLWhenEveryMemberServesLogPull proves the gate does
// not misfire on a fully upgraded cluster: DDL proceeds normally when
// LegacyLogProtocolMembers reports no member.
func TestCoordinator_AllowsDDLWhenEveryMemberServesLogPull(t *testing.T) {
	handler, session := setupUpgradeGateCoordinator(t, nil)

	_, err := handler.HandleQuery(session, "CREATE TABLE t (id INTEGER PRIMARY KEY)", nil)
	require.NoError(t, err, "DDL must proceed once every member reports LogPullProtocolVersion")
}
