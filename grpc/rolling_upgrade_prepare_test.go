package grpc

import (
	"context"
	"os"
	"testing"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/stretchr/testify/require"
)

// TestHandlePrepare_RefusesDDLWhileALegacyMemberIsPresent proves the
// participant half of the rolling-upgrade DDL gate: a PREPARE carrying DDL
// (or CREATE/DROP DATABASE) must be refused, with the retryable 1213, while
// any cluster member does not serve the commit-log pull protocol yet. The
// coordinator-side gate alone would not stop a coordinator running an older
// release, which has no such gate.
func TestHandlePrepare_RefusesDDLWhileALegacyMemberIsPresent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_test_upgrade_prepare")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	defer dbMgr.Close()

	testDB := "test_db"
	require.NoError(t, dbMgr.CreateDatabase(testDB))

	schemaVersionMgr := db.NewSchemaVersionManager(dbMgr)
	handler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	registry := NewNodeRegistry(1, "localhost:8081")
	registry.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE, LogProtocolVersion: 0}) // an older release
	handler.SetRegistry(registry)

	req := &TransactionRequest{
		TxnId:        9,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_PREPARE,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_DDL,
				TableName: "t",
				Database:  testDB,
				Payload:   &Statement_DdlChange{DdlChange: &DDLChange{Sql: "CREATE TABLE t (id INTEGER PRIMARY KEY)"}},
			},
		},
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	require.NoError(t, err, "refusal is a response field, not a transport error")
	require.False(t, resp.Success, "PREPARE carrying DDL must be refused while a legacy member is present")
	require.True(t, resp.Rejected)
	require.Equal(t, uint32(mysqlcode.ErrCodeDeadlock), resp.ErrorCode)
	require.Equal(t, coordinator.NewLegacyMembersDDLRefusal([]uint64{2}).Error(), resp.ErrorMessage)
}

// TestHandlePrepare_AllowsDDLWhenEveryMemberServesLogPull proves the gate does not
// misfire once every member reports LogPullProtocolVersion.
func TestHandlePrepare_AllowsDDLWhenEveryMemberServesLogPull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_test_upgrade_prepare_ok")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	defer dbMgr.Close()

	testDB := "test_db"
	require.NoError(t, dbMgr.CreateDatabase(testDB))

	schemaVersionMgr := db.NewSchemaVersionManager(dbMgr)
	handler := NewReplicationHandler(1, dbMgr, clock, schemaVersionMgr)

	registry := NewNodeRegistry(1, "localhost:8081")
	registry.Add(&NodeState{NodeId: 2, Address: "localhost:8082", Status: NodeStatus_ALIVE, LogProtocolVersion: LogPullProtocolVersion})
	handler.SetRegistry(registry)

	req := &TransactionRequest{
		TxnId:        10,
		SourceNodeId: 2,
		Database:     testDB,
		Phase:        TransactionPhase_PREPARE,
		Timestamp: &HLC{
			WallTime: clock.Now().WallTime,
			Logical:  clock.Now().Logical,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_DDL,
				TableName: "t",
				Database:  testDB,
				Payload:   &Statement_DdlChange{DdlChange: &DDLChange{Sql: "CREATE TABLE t (id INTEGER PRIMARY KEY)"}},
			},
		},
	}

	resp, err := handler.HandleReplicateTransaction(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Success, "PREPARE carrying DDL must succeed once every member reports LogPullProtocolVersion: %s", resp.ErrorMessage)
}
