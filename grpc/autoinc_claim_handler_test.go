package grpc

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/stretchr/testify/require"
)

// newClaimTestHandler builds a real ReplicationHandler over a DatabaseManager
// holding database "testdb" with one table, created from ddl.
func newClaimTestHandler(t *testing.T, ddl string) (*ReplicationHandler, *db.DatabaseManager) {
	t.Helper()
	clock := hlc.NewClock(1)
	dm, err := db.NewDatabaseManager(t.TempDir(), 1, clock)
	require.NoError(t, err)
	t.Cleanup(func() { dm.Close() })
	require.NoError(t, dm.CreateDatabase("testdb"))
	mdb, err := dm.GetDatabase("testdb")
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, mdb.ReloadSchema())
	return NewReplicationHandler(1, dm, clock, db.NewSchemaVersionManager(dm.GetSystemDatabase().GetMetaStore())), dm
}

// prepareOverWire sends statements through the wire conversion and the
// handler's PREPARE entry point, as a remote coordinator's PREPARE arrives.
func prepareOverWire(t *testing.T, rh *ReplicationHandler, txnID uint64, stmts []protocol.Statement, onWire func(*Statement)) *TransactionResponse {
	t.Helper()
	protoStmts, err := convertStatementsToProto(stmts, "testdb", txnID)
	require.NoError(t, err)
	for _, s := range protoStmts {
		onWire(s)
	}
	resp, err := rh.HandleReplicateTransaction(context.Background(), &TransactionRequest{
		TxnId:        txnID,
		SourceNodeId: 2,
		Database:     "testdb",
		Phase:        TransactionPhase_PREPARE,
		Statements:   protoStmts,
		Timestamp:    &HLC{WallTime: int64(txnID), NodeId: 2},
	})
	require.NoError(t, err)
	return resp
}

func claimStatementForWire(t *testing.T, prevBase, size uint64) protocol.Statement {
	t.Helper()
	payload, err := protocol.EncodeAutoIncClaim(protocol.AutoIncClaim{
		Table: "users", PrevBase: prevBase, NewBase: prevBase, Size: size,
	})
	require.NoError(t, err)
	return protocol.Statement{
		Type:               protocol.StatementInsert,
		Database:           "testdb",
		TableName:          "users",
		IntentKey:          []byte(protocol.AutoIncClaimKey("testdb", "users")),
		AutoIDClaim:        true,
		AutoIDClaimPayload: payload,
	}
}

func unchanged(*Statement) {}

// TestHandlePrepareReturnsTheStoredBaseOfARejectedClaim: a participant that
// rejects a stale claim returns its own stored base on the wire, so the
// claimant can retry above it instead of spinning to 1205.
//
// Mutation: drop AutoIdStoredBase from the TransactionResponse literal in
// ReplicationHandler.handlePrepare. "the rejecting participant's base did not
// reach the wire" fires.
func TestHandlePrepareReturnsTheStoredBaseOfARejectedClaim(t *testing.T) {
	rh, dm := newClaimTestHandler(t, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	require.NoError(t, db.NewAutoIncClaimStore(dm.GetSystemDatabase()).Seed("testdb", "users", 500, 1))

	resp := prepareOverWire(t, rh, 100, []protocol.Statement{claimStatementForWire(t, 100, 10)}, unchanged)
	require.False(t, resp.Success)
	require.True(t, resp.Rejected)
	require.Equal(t, uint64(500), resp.AutoIdStoredBase, "the rejecting participant's base did not reach the wire")
}

// TestHandlePrepareReturnsTheParticipantsErrorCode: a DDL this participant
// refuses with a MySQL-coded error (here the width ceiling, 1264) returns that
// code on the wire, so a client whose coordinator is another node sees 1264
// rather than 1105.
//
// Mutation: drop ErrorCode from the TransactionResponse literal in
// ReplicationHandler.handlePrepare. "the participant's MySQL code did not
// reach the wire" fires.
func TestHandlePrepareReturnsTheParticipantsErrorCode(t *testing.T) {
	rh, dm := newClaimTestHandler(t, "CREATE TABLE t (id INTEGER /*M:8a*/ PRIMARY KEY, v TEXT)")
	mdb, err := dm.GetDatabase("testdb")
	require.NoError(t, err)
	_, err = mdb.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (200, 'x')")
	require.NoError(t, err)

	resp := prepareOverWire(t, rh, 101, []protocol.Statement{{
		Type:      protocol.StatementDDL,
		Database:  "testdb",
		TableName: "t",
		SQL:       "ALTER TABLE t ADD COLUMN extra INTEGER",
	}}, unchanged)
	require.False(t, resp.Success)
	require.True(t, resp.Rejected)
	require.Equal(t, uint32(mysqlcode.ErrCodeDataOutOfRange), resp.GetErrorCode(),
		"the participant's MySQL code did not reach the wire")
}

// TestTransactionResponseConversionKeepsTheErrorCode is the coordinator-side
// half: the code read off the wire must reach coordinator.ReplicationResponse.
//
// Mutation: drop ErrorCode from convertTransactionResponse. "the error code
// was lost converting the wire response" fires.
func TestTransactionResponseConversionKeepsTheErrorCode(t *testing.T) {
	resp := convertTransactionResponse(&TransactionResponse{
		Rejected:  true,
		ErrorCode: uint32(mysqlcode.ErrCodeDataOutOfRange),
	})
	require.Equal(t, mysqlcode.ErrCodeDataOutOfRange, resp.ErrorCode, "the error code was lost converting the wire response")
}

// TestClaimIsRejectedByABinaryThatIgnoresTheFlag is the rolling-upgrade proof.
// Two binaries cannot run in the harness, so the old binary is simulated at
// the point it differs: it does not know wire fields 9 and 10, so it drops the
// claim flag and payload, and its PREPARE handler sees an ordinary INSERT. That
// statement must be refused at the engine's "DML prepare missing encoded CDC
// row" gate, never applied as a write.
//
// Mutation: make the converter copy the claim payload into the row image
// (convertStatementsToProto) - the old binary then finds a row image, passes
// the gate, and "the old binary did not refuse the claim at the CDC-row gate"
// fires. The fixture carries a real payload for exactly this reason.
func TestClaimIsRejectedByABinaryThatIgnoresTheFlag(t *testing.T) {
	rh, dm := newClaimTestHandler(t, "CREATE TABLE users (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)")
	require.NoError(t, db.NewAutoIncClaimStore(dm.GetSystemDatabase()).Seed("testdb", "users", 0, 1))

	oldBinary := func(s *Statement) {
		s.AutoIdClaim = false
		s.AutoIdClaimPayload = nil
	}
	resp := prepareOverWire(t, rh, 102, []protocol.Statement{claimStatementForWire(t, 0, 10)}, oldBinary)
	require.False(t, resp.Success, "the old binary accepted a claim as a write")
	require.Contains(t, resp.ErrorMessage, "DML prepare missing encoded CDC row",
		"the old binary did not refuse the claim at the CDC-row gate")
}
