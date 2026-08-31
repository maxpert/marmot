package grpc

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// prepareOnlyServer exposes a real *ReplicationHandler over gRPC. It is
// intentionally minimal - only ReplicateTransaction is wired - to keep this
// test's server-side surface limited to what PREPARE actually needs, the
// same pattern trailerOnlyStreamSnapshotServer uses in catch_up_test.go.
type prepareOnlyServer struct {
	UnimplementedMarmotServiceServer
	handler *ReplicationHandler
}

func (s *prepareOnlyServer) ReplicateTransaction(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	return s.handler.HandleReplicateTransaction(ctx, req)
}

// TestGRPCReplicator_PrepareRejectionCarriesOverRealGRPCConnection is the
// wire-level regression test for the PREPARE Rejected verdict: a real
// ReplicationEngine.Prepare rejection (a duplicate-column DDL SQLite refuses)
// must survive real protobuf marshaling to a real TCP gRPC connection and
// come back out through GRPCReplicator.ReplicateTransaction as
// coordinator.ReplicationResponse.Rejected == true. A struct literal with
// Rejected set by hand proves nothing about the wire; this drives it through
// grpc.NewServer/grpc.NewClient exactly as production traffic does.
func TestGRPCReplicator_PrepareRejectionCarriesOverRealGRPCConnection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "marmot_test_prepare_rejection_wire")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)
	defer dbMgr.Close()

	const testDB = "test_prepare_rejection"
	require.NoError(t, dbMgr.CreateDatabase(testDB))

	dbInstance, err := dbMgr.GetDatabase(testDB)
	require.NoError(t, err)
	_, err = dbInstance.GetDB().Exec(`CREATE TABLE groups (group_id INTEGER PRIMARY KEY, creation_date datetime)`)
	require.NoError(t, err)

	systemDB, err := dbMgr.GetDatabase(db.SystemDatabaseName)
	require.NoError(t, err)
	handler := NewReplicationHandler(1, dbMgr, clock, db.NewSchemaVersionManager(systemDB.GetMetaStore()))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, &prepareOnlyServer{handler: handler})
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	const peerNodeID = 2
	client := NewClient(peerNodeID)
	require.NoError(t, client.Connect(peerNodeID, listener.Addr().String()))
	defer client.Close()

	replicator := NewGRPCReplicator(client)

	req := &coordinator.ReplicationRequest{
		TxnID:    500,
		NodeID:   peerNodeID,
		Database: testDB,
		Phase:    coordinator.PhasePrep,
		StartTS:  clock.Now(),
		Statements: []protocol.Statement{
			{
				Type:     protocol.StatementDDL,
				Database: testDB,
				SQL:      `ALTER TABLE groups ADD COLUMN creation_date datetime NOT NULL DEFAULT '2026-08-10 19:38:56.952152'`,
			},
		},
	}

	resp, err := replicator.ReplicateTransaction(context.Background(), peerNodeID, req)
	require.NoError(t, err)

	if resp.Success {
		t.Fatal("expected PREPARE to fail: ALTER TABLE ADD COLUMN duplicate column must be rejected")
	}
	if !resp.Rejected {
		t.Fatalf("expected Rejected=true to survive the real gRPC round trip, got false (error: %q)", resp.Error)
	}
	require.Contains(t, resp.Error, "duplicate column name: creation_date")

	// The connection carried a genuine sqlite3 error two hops (handler ->
	// wire -> adapter); verify the client-facing MySQL mapping end to end too.
	mysqlErr := protocol.ConvertToMySQLError(&coordinator.RemotePrepareRejectedError{NodeID: peerNodeID, Reason: resp.Error})
	if mysqlErr.Code != protocol.ErrCodeDupFieldName {
		t.Errorf("MySQL error code: got %d, want %d (ErrCodeDupFieldName)", mysqlErr.Code, protocol.ErrCodeDupFieldName)
	}
}
