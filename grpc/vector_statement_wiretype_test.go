package grpc

import (
	"testing"

	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// callProtocolStatementFromProto invokes protocolStatementFromProto and proves
// the call never panics, regardless of what the result turns out to be. This
// is what makes TestProtocolStatementFromProto_UnknownWireType red if
// MustFromWireType (which panics instead of returning an error) is ever
// restored on this path: the recover fires and t.Fatalf runs before the
// normal error assertions are even reached.
func callProtocolStatementFromProto(t *testing.T, stmt *Statement) (protocol.Statement, error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("protocolStatementFromProto panicked: %v", r)
		}
	}()
	return protocolStatementFromProto(stmt)
}

// TestProtocolStatementFromProto_UnknownWireType asserts that an unknown wire
// StatementType produces a non-nil, descriptive error instead of a panic.
// Mutation caught: reverting appcommon.FromWireType back to
// appcommon.MustFromWireType makes the recover() in
// callProtocolStatementFromProto fire and fail the test at that Fatalf,
// before the require.Error assertion below is even reached.
func TestProtocolStatementFromProto_UnknownWireType(t *testing.T) {
	t.Parallel()

	stmt := &Statement{Type: pb.StatementType(999), TableName: "docs", Database: "app"}
	_, err := callProtocolStatementFromProto(t, stmt)

	// Mutation caught: dropping the `if !ok { return ..., err }` guard (i.e.
	// ignoring the FromWireType ok value) makes err nil and reddens this line.
	require.Error(t, err)
	require.Contains(t, err.Error(), "999")
}

// unknownStatementPayload is a oneof implementation this binary does not
// know about - standing in for a payload case added by a newer peer. It is
// defined in this package (not imported) because isStatement_Payload() is
// unexported and can only be implemented from within package grpc.
type unknownStatementPayload struct{}

func (unknownStatementPayload) isStatement_Payload() {}

// TestProtocolStatementFromProto_UnknownPayload asserts that a recognised
// payload case is required. Mutation caught: removing the `default:` arm
// from the switch in protocolStatementFromProto (or turning it into a no-op)
// makes err nil and reddens the require.Error assertion below - the
// statement would otherwise silently ACK with no SQL, no intent key and no
// row image (the original silent-data-loss defect).
func TestProtocolStatementFromProto_UnknownPayload(t *testing.T) {
	t.Parallel()

	stmt := &Statement{
		Type:      pb.StatementType_INSERT,
		TableName: "docs",
		Database:  "app",
		Payload:   unknownStatementPayload{},
	}
	_, err := callProtocolStatementFromProto(t, stmt)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unknownStatementPayload")
}

// TestProtocolStatementFromProto_KnownPayloads is a regression guard: every
// payload kind the wire format knows about today must keep converting with a
// nil error and the same field values it produces today.
func TestProtocolStatementFromProto_KnownPayloads(t *testing.T) {
	t.Parallel()

	vectorChange := &VectorIndexChange{
		Action:    VectorIndexAction_VECTOR_INDEX_ACTION_CREATE,
		Database:  "app",
		IndexName: "docs_embed_idx",
		TableName: "docs",
		Metric:    "cosine",
		Dim:       128,
	}

	tests := []struct {
		name  string
		stmt  *Statement
		check func(t *testing.T, got protocol.Statement)
	}{
		{
			name: "row_change",
			stmt: &Statement{
				Type:      pb.StatementType_INSERT,
				TableName: "docs",
				Database:  "app",
				Payload:   &Statement_RowChange{RowChange: testInsertRowChange("docs", []byte("pk:1"), map[string][]byte{"id": {1}})},
			},
			check: func(t *testing.T, got protocol.Statement) {
				t.Helper()
				// Mutation caught: dropping the *Statement_RowChange case (or its
				// decodeRowChange call) leaves IntentKey/NewValues zero, reddening
				// these two assertions.
				require.Equal(t, []byte("pk:1"), got.IntentKey)
				require.Equal(t, map[string][]byte{"id": {1}}, got.NewValues)
				// The encoded row image is what a peer actually applies, and
				// nothing else in this file asserted it - a mutation arm that
				// deleted these two assignments went green.
				// Mutation caught: dropping either assignment in the
				// *Statement_RowChange case.
				require.NotEmpty(t, got.EncodedRow)
				require.Equal(t, db.EncodedCapturedRowCodecMsgpack(), got.EncodedCodec)
			},
		},
		{
			name: "ddl_change",
			stmt: &Statement{
				Type:      pb.StatementType_DDL,
				TableName: "docs",
				Database:  "app",
				Payload:   &Statement_DdlChange{DdlChange: &DDLChange{Sql: "CREATE TABLE docs (id INT)"}},
			},
			check: func(t *testing.T, got protocol.Statement) {
				t.Helper()
				// Mutation caught: making the ddl_change case fall into `default`
				// (returning an error) turns this into a test failure via err below;
				// mutating stmt.GetSQL() away from the top-level assignment reddens
				// this SQL assertion specifically.
				require.Equal(t, "CREATE TABLE docs (id INT)", got.SQL)
			},
		},
		{
			name: "load_data_change",
			stmt: &Statement{
				Type:      pb.StatementType_LOAD_DATA,
				TableName: "docs",
				Database:  "app",
				Payload:   &Statement_LoadDataChange{LoadDataChange: &LoadDataChange{Sql: "LOAD DATA ...", Data: []byte("payload")}},
			},
			check: func(t *testing.T, got protocol.Statement) {
				t.Helper()
				// Mutation caught: dropping the *Statement_LoadDataChange case
				// leaves SQL/LoadDataPayload unset, reddening these two.
				require.Equal(t, "LOAD DATA ...", got.SQL)
				require.Equal(t, []byte("payload"), got.LoadDataPayload)
			},
		},
		{
			name: "vector_index_change",
			stmt: &Statement{
				Type:      pb.StatementType_VECTOR_INDEX,
				TableName: "docs",
				Database:  "app",
				Payload:   &Statement_VectorIndexChange{VectorIndexChange: vectorChange},
			},
			check: func(t *testing.T, got protocol.Statement) {
				t.Helper()
				// Mutation caught: dropping the *Statement_VectorIndexChange case
				// leaves Type as StatementInsert (from the wire type) instead of
				// StatementCreateVectorIndex, and VectorIndexName empty.
				require.Equal(t, protocol.StatementCreateVectorIndex, got.Type)
				require.Equal(t, "docs_embed_idx", got.VectorIndexName)
				require.NotNil(t, got.VectorIndexChange)
			},
		},
		{
			name: "dml_intent",
			stmt: &Statement{
				Type:      pb.StatementType_UPDATE,
				TableName: "docs",
				Database:  "app",
				Payload:   &Statement_DmlIntent{DmlIntent: &DMLIntent{IntentKey: []byte("pk:2")}},
			},
			check: func(t *testing.T, got protocol.Statement) {
				t.Helper()
				// Mutation caught: making the dml_intent case fall into `default`
				// turns this into an error via err below; mutating
				// stmt.GetIntentKey() away from the top-level assignment reddens
				// this IntentKey assertion specifically.
				require.Equal(t, []byte("pk:2"), got.IntentKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := protocolStatementFromProto(tt.stmt)
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}
