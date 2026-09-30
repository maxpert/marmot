package protocol

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// isAnswerHandler answers every query with a fixed INFORMATION_SCHEMA row set,
// as the metadata handler does, and records the params it was handed.
type isAnswerHandler struct {
	params []interface{}
}

func (h *isAnswerHandler) HandleQuery(_ *ConnectionSession, _ string, params []interface{}) (*ResultSet, error) {
	h.params = params
	return &ResultSet{
		Columns: InformationSchemaColumns(ISTableSchemata),
		Rows:    [][]interface{}{{"def", "marmot", "utf8mb4", "utf8mb4_general_ci", nil}},
	}, nil
}

// readAllPackets runs serve against one end of a pipe and returns every
// packet it wrote, in order.
func readAllPackets(t *testing.T, serve func(conn net.Conn)) [][]byte {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go func() {
		serve(serverConn)
		_ = serverConn.Close()
	}()

	var packets [][]byte
	for {
		header := make([]byte, 4)
		if _, err := io.ReadFull(clientConn, header); err != nil {
			return packets
		}
		size := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		payload := make([]byte, size)
		_, err := io.ReadFull(clientConn, payload)
		require.NoError(t, err)
		packets = append(packets, payload)
	}
}

// TestParseResolvesBoundInformationSchemaFilters pins F-P1's trigger: a bound
// table_schema/table_name must filter exactly as the literal does. Without it
// a prepared COLUMNS query takes the every-table branch and answers rows of
// tables the client never asked about.
//
// Mutation: filterValues.value ignores *sqlparser.Argument; the first row
// fails.
func TestParseResolvesBoundInformationSchemaFilters(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		bound      []interface{}
		wantType   InformationSchemaTableType
		wantSchema string
		wantTable  string
	}{
		{
			name:       "both bound",
			sql:        "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
			bound:      []interface{}{"marmot", "pn"},
			wantType:   ISTableColumns,
			wantSchema: "marmot",
			wantTable:  "pn",
		},
		{
			name:       "bound as bytes",
			sql:        "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
			bound:      []interface{}{[]byte("p2")},
			wantType:   ISTableTables,
			wantSchema: "p2",
		},
		{
			name:       "literal and bound mixed",
			sql:        "SELECT * FROM information_schema.columns WHERE table_schema = 'marmot' AND table_name = ?",
			bound:      []interface{}{"pk"},
			wantType:   ISTableColumns,
			wantSchema: "marmot",
			wantTable:  "pk",
		},
		{
			name:       "placeholder before the filter",
			sql:        "SELECT ?, table_name FROM information_schema.tables WHERE table_schema = ?",
			bound:      []interface{}{int64(1), "p2"},
			wantType:   ISTableTables,
			wantSchema: "p2",
		},
		{
			name:       "schemata",
			sql:        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?",
			bound:      []interface{}{"p2"},
			wantType:   ISTableSchemata,
			wantSchema: "p2",
		},
		{
			name:     "non-text bound value filters nothing",
			sql:      "SELECT * FROM information_schema.tables WHERE table_schema = ?",
			bound:    []interface{}{int64(7)},
			wantType: ISTableTables,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatementWithOptions(tt.sql, ParseOptions{BoundParams: tt.bound})
			require.Equal(t, StatementInformationSchema, stmt.Type)
			require.Equal(t, tt.wantType, stmt.ISTableType)
			require.Equal(t, tt.wantSchema, stmt.ISFilter.SchemaName, "schema filter")
			require.Equal(t, tt.wantTable, stmt.ISFilter.TableName, "table filter")
		})
	}
}

// TestPreparedInformationSchemaAnswersWithItsRows pins F-P2: a prepared
// INFORMATION_SCHEMA statement answers with the handler's result set, exactly
// as its text form does, never with an OK packet.
//
// Mutation: answersRows := stmt.OriginalType == StatementSelect; the column
// count assertion fires on an OK packet (0x00).
func TestPreparedInformationSchemaAnswersWithItsRows(t *testing.T) {
	handler := &isAnswerHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	stmt := &PreparedStatement{
		ID:           9,
		Query:        "SELECT * FROM information_schema.schemata WHERE schema_name = ?",
		ParamCount:   1,
		OriginalType: StatementInformationSchema,
	}
	session := &ConnectionSession{
		ConnID:        1,
		preparedStmts: map[uint32]*PreparedStatement{stmt.ID: stmt},
	}

	value := "marmot"
	execPayload := make([]byte, 14+len(value))
	binary.LittleEndian.PutUint32(execPayload[0:4], stmt.ID) // statement_id
	execPayload[4] = 0                                       // flags
	binary.LittleEndian.PutUint32(execPayload[5:9], 1)       // iteration_count
	execPayload[9] = 0x00                                    // NULL bitmap
	execPayload[10] = 0x01                                   // new_params_bound_flag
	execPayload[11] = 0xFD                                   // MYSQL_TYPE_VAR_STRING
	execPayload[12] = 0x00                                   // param flags
	execPayload[13] = byte(len(value))                       // lenenc length
	copy(execPayload[14:], value)

	packets := readAllPackets(t, func(conn net.Conn) { server.handleStmtExecute(conn, session, execPayload) })
	require.NotEmpty(t, packets)
	require.Equal(t, byte(len(isSchemataColumns)), packets[0][0],
		"a prepared INFORMATION_SCHEMA query must answer with its column count, not an OK packet")
	// Column count, 5 definitions, EOF, one row, EOF.
	require.Len(t, packets, 1+len(isSchemataColumns)+1+1+1)
	require.Equal(t, []interface{}{"marmot"}, handler.params)
}

// TestPrepareDescribesInformationSchemaColumns pins that COM_STMT_PREPARE of
// an INFORMATION_SCHEMA statement reports the columns its execution returns:
// clients index result columns by name from this response.
//
// Mutation: drop the StatementInformationSchema branch in handleStmtPrepare;
// the column count is 0.
func TestPrepareDescribesInformationSchemaColumns(t *testing.T) {
	server := NewMySQLServer("127.0.0.1:0", "", 0, &isAnswerHandler{})
	session := &ConnectionSession{ConnID: 1, preparedStmts: map[uint32]*PreparedStatement{}}

	packets := readAllPackets(t, func(conn net.Conn) {
		server.handleStmtPrepare(conn, session, "SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")
	})
	require.NotEmpty(t, packets)
	ok := packets[0]
	require.Equal(t, byte(0x00), ok[0], "PREPARE_OK")
	require.Equal(t, uint16(len(isTablesColumns)), binary.LittleEndian.Uint16(ok[5:7]), "column count")
	require.Equal(t, uint16(2), binary.LittleEndian.Uint16(ok[7:9]), "param count")
}

// TestInformationSchemaColumnsAreCopies pins that a caller cannot corrupt the
// shared column sets.
func TestInformationSchemaColumnsAreCopies(t *testing.T) {
	cols := InformationSchemaColumns(ISTableTables)
	cols[0].Name = "mutated"
	require.Equal(t, "TABLE_CATALOG", InformationSchemaColumns(ISTableTables)[0].Name)
	require.Nil(t, InformationSchemaColumns(ISTableUnknown))
}
