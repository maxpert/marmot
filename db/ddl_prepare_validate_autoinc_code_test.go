//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/stretchr/testify/require"
)

// TestAutoIncWidthExceededError_ConvertsToDataOutOfRange pins that an
// *AutoIncWidthExceededError - the PREPARE-time refusal of DDL that would
// leave an AUTO_INCREMENT column unable to hold ids the table already
// contains - reaches a client-facing caller as MySQL's
// ER_WARN_DATA_OUT_OF_RANGE (1264, SQLSTATE 22003), through the exact same
// conversion protocol.ConvertToMySQLError performs for every other error
// surfaced to a client.
//
// Mutation: remove AutoIncWidthExceededError's coded-error exposure (Unwrap);
// ConvertToMySQLError falls through to the message-based default
// (ER_UNKNOWN_ERROR, HY000) and the Code assertion below fires.
func TestAutoIncWidthExceededError_ConvertsToDataOutOfRange(t *testing.T) {
	t.Parallel()

	widthErr := &AutoIncWidthExceededError{
		Table:    "narrow",
		Column:   "id",
		Max:      200,
		WidthMax: 127,
	}

	mysqlErr := protocol.ConvertToMySQLError(widthErr)
	require.Equal(t, mysqlcode.ErrCodeDataOutOfRange, mysqlErr.Code,
		"a width-ceiling rejection must reach the client as ER_WARN_DATA_OUT_OF_RANGE")
	require.Equal(t, mysqlcode.SQLStateDataOutOfRange, mysqlErr.SQLState)

	require.True(t, isDDLRejection(context.Background(), widthErr),
		"a width-ceiling violation must still be classified as a deterministic rejection")
}

// TestPrepareRejectionCarriesTheMySQLCode pins the hop the test above cannot
// see. A participant's rejection crosses to the coordinator as a STRING
// (PrepareResult.Error), so the typed error - and with it the code
// ConvertToMySQLError reads - is gone by the time anything maps the failure to
// a client error. The code therefore has to be read where the typed error
// still exists and carried alongside the message. Without this, the
// width-ceiling refusal that this file's first test proves is 1264 reached a
// real MySQL client as 1105 HY000
// (test/autoinc_claim_cluster_test.go TestAutoIncSeed_RefusesWhenExistingMaxExceedsWidth).
//
// Mutation: return 0 from mysqlCodeForError. The first assertion fires. The
// DDL rejection site itself is pinned by
// TestPrepareOfARefusedDDLCarriesTheMySQLCode.
func TestPrepareRejectionCarriesTheMySQLCode(t *testing.T) {
	t.Parallel()

	widthErr := &AutoIncWidthExceededError{Table: "narrow", Column: "id", Max: 200, WidthMax: 127}

	require.Equal(t, mysqlcode.ErrCodeDataOutOfRange, mysqlCodeForError(widthErr),
		"the rejection must carry the participant's own MySQL code, not 0")
	require.Zero(t, mysqlCodeForError(context.DeadlineExceeded),
		"an error naming no code must stay 0 so the coordinator keeps classifying the message")

	// The value survives the conversion the local participant path uses.
	// Mutation: drop ErrorCode from ToCoordinatorResponse.
	result := &PrepareResult{Success: false, Rejected: true, Error: widthErr.Error(),
		ErrorCode: mysqlCodeForError(widthErr)}
	require.Equal(t, mysqlcode.ErrCodeDataOutOfRange, result.ToCoordinatorResponse().ErrorCode,
		"the code must reach the coordinator with the response, not stop at the db boundary")
}

// TestPrepareOfARefusedDDLCarriesTheMySQLCode drives the DDL rejection site in
// ReplicationEngine.Prepare itself: a DDL that declares a TINYINT
// AUTO_INCREMENT column over a value (200) it cannot hold is refused, and the
// PrepareResult carries 1264 for the coordinator.
//
// Mutation: drop ErrorCode from the PrepareResult literal at the DDL rejection
// site in prepareRegularTransaction. "the DDL rejection lost its MySQL code"
// fires.
func TestPrepareOfARefusedDDLCarriesTheMySQLCode(t *testing.T) {
	engine, dm, cleanup := setupTestReplicationEngine(t)
	defer cleanup()
	mdb := markedTableDB(t, engine, dm, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	_, err := mdb.GetWriteDB().Exec("INSERT INTO t (id, v) VALUES (1, 'x')")
	require.NoError(t, err)

	result := engine.Prepare(context.Background(), &PrepareRequest{
		TxnID: 7600, NodeID: 1, StartTS: hlc.Timestamp{WallTime: 14}, Database: "testdb",
		Statements: []protocol.Statement{{Type: protocol.StatementDDL, Database: "testdb", TableName: "t",
			SQL: "ALTER TABLE t ADD COLUMN seq INTEGER /*M:8a*/ DEFAULT 200"}},
	})
	require.False(t, result.Success)
	require.True(t, result.Rejected)
	require.Equal(t, mysqlcode.ErrCodeDataOutOfRange, result.ErrorCode, "the DDL rejection lost its MySQL code")
}
