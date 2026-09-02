package protocol

import (
	"database/sql"
	"encoding/binary"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sqliteExecHandler routes HandleQuery through a real *sql.DB backed by the
// project's actual SQLite driver, so tests using it exercise the real
// database/sql parameter-binding path (including its rejection of uint64
// values with the high bit set) instead of a mock that merely records
// whatever value it was handed.
type sqliteExecHandler struct {
	db      *sql.DB
	queries []string
}

func newSQLiteExecHandler(t *testing.T, schema string) *sqliteExecHandler {
	t.Helper()
	db := openTestDB(t, ":memory:")
	_, err := db.Exec(schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return &sqliteExecHandler{db: db}
}

func (h *sqliteExecHandler) HandleQuery(session *ConnectionSession, sqlText string, params []interface{}) (*ResultSet, error) {
	h.queries = append(h.queries, sqlText)
	res, err := h.db.Exec(sqlText, params...)
	if err != nil {
		return nil, err
	}
	rowsAffected, _ := res.RowsAffected()
	lastInsertID, _ := res.LastInsertId()
	return &ResultSet{RowsAffected: rowsAffected, LastInsertId: lastInsertID}, nil
}

// buildSendLongDataPayload constructs a COM_STMT_SEND_LONG_DATA payload
// (without the leading 0x18 command byte, matching how the command
// dispatcher in handleConnection slices it before calling the handler).
func buildSendLongDataPayload(stmtID uint32, paramID uint16, data string) []byte {
	payload := make([]byte, 6+len(data))
	binary.LittleEndian.PutUint32(payload[0:4], stmtID)
	binary.LittleEndian.PutUint16(payload[4:6], paramID)
	copy(payload[6:], data)
	return payload
}

// TestStmtSendLongData_NoResponseAccumulatesAndExecuteUsesIt exercises the
// full COM_STMT_SEND_LONG_DATA -> COM_STMT_EXECUTE lifecycle at the packet
// level: repeated SEND_LONG_DATA calls append to the same parameter buffer,
// produce zero response bytes on the wire, and the following EXECUTE (which
// per spec omits the inline value for a long-data parameter) picks up the
// accumulated blob.
func TestStmtSendLongData_NoResponseAccumulatesAndExecuteUsesIt(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	stmt := &PreparedStatement{
		ID:           7,
		Query:        "INSERT INTO blobs (id, data) VALUES (?, ?)",
		ParamCount:   1,
		OriginalType: StatementInsert,
	}
	session := &ConnectionSession{
		ConnID:        1,
		preparedStmts: map[uint32]*PreparedStatement{stmt.ID: stmt},
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Two chunks for the same (statement_id, param_id) must accumulate.
	server.handleStmtSendLongData(session, buildSendLongDataPayload(stmt.ID, 0, "hello "))
	server.handleStmtSendLongData(session, buildSendLongDataPayload(stmt.ID, 0, "world"))
	require.Equal(t, []byte("hello world"), stmt.LongData[0])

	// Per spec, COM_STMT_SEND_LONG_DATA never gets a response. Prove zero
	// bytes crossed the wire before EXECUTE runs.
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	_, err := clientConn.Read(make([]byte, 1))
	require.Error(t, err, "expected no bytes written for COM_STMT_SEND_LONG_DATA")
	require.NoError(t, clientConn.SetReadDeadline(time.Time{}))

	// EXECUTE with newParamsBoundFlag=1: per spec the client omits the inline
	// value for a parameter that received long data, so no value bytes
	// follow the 2-byte type for param 0.
	execPayload := make([]byte, 13)
	binary.LittleEndian.PutUint32(execPayload[0:4], stmt.ID) // statement_id
	execPayload[4] = 0                                       // flags
	binary.LittleEndian.PutUint32(execPayload[5:9], 1)       // iteration_count
	execPayload[9] = 0x00                                    // NULL bitmap: param 0 not null
	execPayload[10] = 0x01                                   // new_params_bound_flag
	execPayload[11] = 0xFC                                   // MYSQL_TYPE_BLOB
	execPayload[12] = 0x00                                   // param flags (unsigned bit unset)

	done := make(chan struct{})
	go func() {
		server.handleStmtExecute(serverConn, session, execPayload)
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	<-done
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0x00), resp[0], "expected OK packet for INSERT")

	require.Len(t, handler.params, 1)
	require.Equal(t, []byte("hello world"), handler.params[0][0])
}

// TestHandleStmtReset_ClearsLongDataAndReturnsOK verifies COM_STMT_RESET
// clears any accumulated long data and responds with an OK packet.
func TestHandleStmtReset_ClearsLongDataAndReturnsOK(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	stmt := &PreparedStatement{
		ID:         9,
		Query:      "INSERT INTO blobs (id, data) VALUES (?, ?)",
		ParamCount: 1,
		LongData:   map[uint16][]byte{0: []byte("stale data")},
	}
	session := &ConnectionSession{
		ConnID:        1,
		preparedStmts: map[uint32]*PreparedStatement{stmt.ID: stmt},
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, stmt.ID)

	done := make(chan struct{})
	go func() {
		server.handleStmtReset(serverConn, session, payload)
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	<-done
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0x00), resp[0], "expected OK packet")
	require.Nil(t, stmt.LongData)
}

// TestHandleStmtReset_UnknownStatement verifies an ERR packet (1243) is sent
// for an unrecognized statement ID, matching the existing COM_STMT_EXECUTE
// behavior for unknown statements.
func TestHandleStmtReset_UnknownStatement(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	session := &ConnectionSession{
		ConnID:        1,
		preparedStmts: map[uint32]*PreparedStatement{},
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, 999)

	done := make(chan struct{})
	go func() {
		server.handleStmtReset(serverConn, session, payload)
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	<-done
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0xFF), resp[0], "expected ERR packet for unknown statement")
}

// TestParseParamValue_LONGLONG_UnsignedFitsDecodesToInt64 verifies that when
// the UNSIGNED flag is set, a LONGLONG parameter within the int64 range
// (0..math.MaxInt64) decodes to an explicit int64, not a bare uint64 - every
// downstream SQL binding path only accepts signed integers.
func TestParseParamValue_LONGLONG_UnsignedFitsDecodesToInt64(t *testing.T) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, uint64(math.MaxInt64))

	offset, val, err := parseParamValue(payload, 0, 0x08, true)
	require.NoError(t, err)
	require.Equal(t, 8, offset)

	got, ok := val.(int64)
	require.True(t, ok, "expected int64, got %T", val)
	require.Equal(t, int64(math.MaxInt64), got)
}

// TestParseParamValue_LONGLONG_UnsignedOutOfRange verifies that an UNSIGNED
// BIGINT value >= 2^63 - which cannot be represented as SQLite's signed
// 64-bit INTEGER - is reported as errUnsignedBigintOutOfRange rather than
// silently reinterpreted as a negative int64.
func TestParseParamValue_LONGLONG_UnsignedOutOfRange(t *testing.T) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, uint64(math.MaxInt64)+1) // 2^63

	offset, val, err := parseParamValue(payload, 0, 0x08, true)
	require.ErrorIs(t, err, errUnsignedBigintOutOfRange)
	require.Nil(t, val)
	require.Equal(t, 0, offset, "offset should not advance on error")

	// The true maximum (2^64-1) must also be rejected, not wrapped to -1.
	binary.LittleEndian.PutUint64(payload, math.MaxUint64)
	_, val, err = parseParamValue(payload, 0, 0x08, true)
	require.ErrorIs(t, err, errUnsignedBigintOutOfRange)
	require.Nil(t, val)
}

// TestParseParamValue_LONGLONG_SignedUnaffected confirms the signed decode
// path is unchanged by the unsigned-flag plumbing.
func TestParseParamValue_LONGLONG_SignedUnaffected(t *testing.T) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, math.MaxUint64)

	_, val, err := parseParamValue(payload, 0, 0x08, false)
	require.NoError(t, err)
	require.Equal(t, int64(-1), val)
}

// TestStmtExecute_UnsignedBigintFitsInt64_RealSQLiteDriver is a packet-level
// test through a real *sql.DB (the project's actual SQLite driver, not a
// mock): an UNSIGNED BIGINT parameter equal to math.MaxInt64 - the largest
// value representable as int64 - must decode, bind, and persist correctly.
func TestStmtExecute_UnsignedBigintFitsInt64_RealSQLiteDriver(t *testing.T) {
	handler := newSQLiteExecHandler(t, "CREATE TABLE counters (id INTEGER PRIMARY KEY, big INTEGER)")
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	stmt := &PreparedStatement{
		ID:           3,
		Query:        "INSERT INTO counters (id, big) VALUES (1, ?)",
		ParamCount:   1,
		OriginalType: StatementInsert,
	}
	session := &ConnectionSession{
		ConnID:        1,
		preparedStmts: map[uint32]*PreparedStatement{stmt.ID: stmt},
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	const want = uint64(math.MaxInt64) // 9223372036854775807

	execPayload := make([]byte, 21)
	binary.LittleEndian.PutUint32(execPayload[0:4], stmt.ID) // statement_id
	execPayload[4] = 0                                       // flags
	binary.LittleEndian.PutUint32(execPayload[5:9], 1)       // iteration_count
	execPayload[9] = 0x00                                    // NULL bitmap
	execPayload[10] = 0x01                                   // new_params_bound_flag
	execPayload[11] = 0x08                                   // MYSQL_TYPE_LONGLONG
	execPayload[12] = 0x80                                   // UNSIGNED flag set
	binary.LittleEndian.PutUint64(execPayload[13:21], want)  // inline value

	done := make(chan struct{})
	go func() {
		server.handleStmtExecute(serverConn, session, execPayload)
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	<-done
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0x00), resp[0], "expected OK packet for INSERT via real driver")

	var got int64
	require.NoError(t, handler.db.QueryRow("SELECT big FROM counters WHERE id = 1").Scan(&got))
	require.Equal(t, int64(want), got)
}

// TestStmtExecute_UnsignedBigintOutOfRange_ReturnsER1264 verifies that an
// UNSIGNED BIGINT parameter >= 2^63 produces a clean ER_WARN_DATA_OUT_OF_RANGE
// (1264) protocol error during parameter parsing, and never reaches the
// handler/driver at all - avoiding the confusing internal error
// database/sql's default converter would otherwise raise.
func TestStmtExecute_UnsignedBigintOutOfRange_ReturnsER1264(t *testing.T) {
	handler := newSQLiteExecHandler(t, "CREATE TABLE counters (id INTEGER PRIMARY KEY, big INTEGER)")
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	stmt := &PreparedStatement{
		ID:           4,
		Query:        "INSERT INTO counters (id, big) VALUES (2, ?)",
		ParamCount:   1,
		OriginalType: StatementInsert,
	}
	session := &ConnectionSession{
		ConnID:        1,
		preparedStmts: map[uint32]*PreparedStatement{stmt.ID: stmt},
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	const tooBig = uint64(math.MaxInt64) + 1 // 2^63, first value out of int64 range

	execPayload := make([]byte, 21)
	binary.LittleEndian.PutUint32(execPayload[0:4], stmt.ID)
	execPayload[4] = 0
	binary.LittleEndian.PutUint32(execPayload[5:9], 1)
	execPayload[9] = 0x00
	execPayload[10] = 0x01
	execPayload[11] = 0x08 // MYSQL_TYPE_LONGLONG
	execPayload[12] = 0x80 // UNSIGNED flag set
	binary.LittleEndian.PutUint64(execPayload[13:21], tooBig)

	done := make(chan struct{})
	go func() {
		server.handleStmtExecute(serverConn, session, execPayload)
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	<-done
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0xFF), resp[0], "expected ERR packet")
	code := binary.LittleEndian.Uint16(resp[1:3])
	require.Equal(t, uint16(1264), code, "expected ER_WARN_DATA_OUT_OF_RANGE")

	require.Empty(t, handler.queries, "handler/driver must never be reached for an unrepresentable value")

	var count int
	require.NoError(t, handler.db.QueryRow("SELECT COUNT(*) FROM counters").Scan(&count))
	require.Equal(t, 0, count)
}
