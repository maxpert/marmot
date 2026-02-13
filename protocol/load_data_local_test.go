package protocol

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

type captureHandler struct {
	queries []string
	params  [][]interface{}
}

func (h *captureHandler) HandleQuery(session *ConnectionSession, sql string, params []interface{}) (*ResultSet, error) {
	h.queries = append(h.queries, sql)
	if params != nil {
		cp := make([]interface{}, len(params))
		copy(cp, params)
		h.params = append(h.params, cp)
	}
	if sql == "BEGIN" || sql == "COMMIT" || sql == "ROLLBACK" {
		return nil, nil
	}
	return &ResultSet{RowsAffected: 1}, nil
}

type captureLoadDataHandler struct {
	captureHandler
	loadCalls int
	lastSQL   string
	lastData  []byte
}

func (h *captureLoadDataHandler) HandleLoadData(session *ConnectionSession, sql string, data []byte) (*ResultSet, error) {
	h.loadCalls++
	h.lastSQL = sql
	h.lastData = append([]byte(nil), data...)
	return &ResultSet{
		RowsAffected:   2,
		LastInsertId:   11,
		CommittedTxnId: 22,
	}, nil
}

func TestParseLoadDataLocalSpec(t *testing.T) {
	sql := "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES (name,email)"
	spec, err := parseLoadDataLocalSpec(sql)
	require.NoError(t, err)
	require.Equal(t, "users", spec.Table)
	require.Equal(t, ",", spec.FieldTerminator)
	require.Equal(t, "\n", spec.LineTerminator)
	require.Equal(t, 1, spec.IgnoreLines)
	require.Equal(t, []string{"name", "email"}, spec.Columns)
}

func TestParseLoadDataLocalSpecRejectsNonLocal(t *testing.T) {
	_, err := parseLoadDataLocalSpec("LOAD DATA INFILE 'users.csv' INTO TABLE users")
	require.Error(t, err)
	require.Contains(t, err.Error(), "LOCAL")
}

func TestProcessQueryLoadDataLocal(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})
	go func() {
		session := &ConnectionSession{
			ConnID:               1,
			CurrentDatabase:      "testdb",
			ClientCaps:           capabilityClientLocalFiles,
			preparedStmts:        map[uint32]*PreparedStatement{},
			TranspilationEnabled: true,
		}
		server.processQuery(serverConn, session, "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (name,email)")
		close(done)
	}()

	p1 := mustReadPacket(t, clientConn)
	require.NotEmpty(t, p1)
	require.Equal(t, byte(0xFB), p1[0], "server should request LOCAL INFILE data")

	mustWritePacket(t, clientConn, 2, []byte("alice,alice@example.com\nbob,bob@example.com\n"))
	mustWritePacket(t, clientConn, 3, []byte{})

	resp := mustReadPacket(t, clientConn)
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0x00), resp[0], "expected OK packet")
	<-done

	require.Equal(t, []string{
		"BEGIN",
		"INSERT INTO users (name, email) VALUES (?, ?), (?, ?)",
		"COMMIT",
	}, handler.queries)
	require.Len(t, handler.params, 1)
	require.Equal(t, []interface{}{"alice", "alice@example.com", "bob", "bob@example.com"}, handler.params[0])
}

func TestProcessQueryLoadDataDelegatesToLoadDataHandler(t *testing.T) {
	handler := &captureLoadDataHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})
	go func() {
		session := &ConnectionSession{
			ConnID:               5,
			CurrentDatabase:      "testdb",
			ClientCaps:           capabilityClientLocalFiles,
			preparedStmts:        map[uint32]*PreparedStatement{},
			TranspilationEnabled: true,
		}
		server.processQuery(serverConn, session, "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users")
		close(done)
	}()

	p1 := mustReadPacket(t, clientConn)
	require.Equal(t, byte(0xFB), p1[0])
	mustWritePacket(t, clientConn, 2, []byte("alice\nbob\n"))
	mustWritePacket(t, clientConn, 3, []byte{})

	resp := mustReadPacket(t, clientConn)
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0x00), resp[0], "expected OK packet")
	<-done

	require.Equal(t, 1, handler.loadCalls)
	require.Equal(t, "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users", handler.lastSQL)
	require.Equal(t, []byte("alice\nbob\n"), handler.lastData)
	require.Empty(t, handler.queries, "query path should not be used when LoadDataHandler is implemented")
}

func TestProcessQueryLoadDataNonLocalRejected(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})
	go func() {
		session := &ConnectionSession{
			ConnID:               2,
			CurrentDatabase:      "testdb",
			ClientCaps:           capabilityClientLocalFiles,
			preparedStmts:        map[uint32]*PreparedStatement{},
			TranspilationEnabled: true,
		}
		server.processQuery(serverConn, session, "LOAD DATA INFILE '/tmp/users.csv' INTO TABLE users")
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0xFF), resp[0], "expected error packet for non-local INFILE")
	<-done

	require.Empty(t, handler.queries)
}

func TestProcessQueryLoadDataLocalDisabled(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	server.SetLocalInfileEnabled(false)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})
	go func() {
		session := &ConnectionSession{
			ConnID:               3,
			CurrentDatabase:      "testdb",
			ClientCaps:           capabilityClientLocalFiles,
			preparedStmts:        map[uint32]*PreparedStatement{},
			TranspilationEnabled: true,
		}
		server.processQuery(serverConn, session, "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users")
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0xFF), resp[0], "expected error packet when LOCAL INFILE is disabled")
	<-done

	require.Empty(t, handler.queries)
}

func mustReadPacket(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	header := make([]byte, 4)
	_, err := conn.Read(header)
	require.NoError(t, err)

	size := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	payload := make([]byte, size)
	_, err = conn.Read(payload)
	require.NoError(t, err)
	return payload
}

func mustWritePacket(t *testing.T, conn net.Conn, seq byte, payload []byte) {
	t.Helper()
	header := []byte{
		byte(len(payload)),
		byte(len(payload) >> 8),
		byte(len(payload) >> 16),
		seq,
	}
	_, err := conn.Write(header)
	require.NoError(t, err)
	if len(payload) > 0 {
		_, err = conn.Write(payload)
		require.NoError(t, err)
	}
}

func TestHandshakeAdvertisesLocalInfileCapability(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		_ = server.writeHandshake(serverConn)
	}()

	payload := mustReadPacket(t, clientConn)
	require.GreaterOrEqual(t, len(payload), 19)

	// capability lower 2 bytes start at offset:
	// protocol(1) + server-version(NUL-term) + conn-id(4) + auth-part1(8) + filler(1)
	offset := 1
	for offset < len(payload) && payload[offset] != 0 {
		offset++
	}
	offset++ // NUL
	offset += 4 + 8 + 1
	capsLower := binary.LittleEndian.Uint16(payload[offset : offset+2])
	require.NotZero(t, capsLower&0x0080, "CLIENT_LOCAL_FILES should be advertised")
}

func TestProcessQueryLoadDataLocalRequiresClientCapability(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	done := make(chan struct{})
	go func() {
		session := &ConnectionSession{
			ConnID:               4,
			CurrentDatabase:      "testdb",
			preparedStmts:        map[uint32]*PreparedStatement{},
			TranspilationEnabled: true,
		}
		server.processQuery(serverConn, session, "LOAD DATA LOCAL INFILE 'users.csv' INTO TABLE users")
		close(done)
	}()

	resp := mustReadPacket(t, clientConn)
	require.NotEmpty(t, resp)
	require.Equal(t, byte(0xFF), resp[0], "expected error packet when client capability is missing")
	<-done
	require.Empty(t, handler.queries)
}

func TestHandshakeOmitsLocalInfileCapabilityWhenDisabled(t *testing.T) {
	handler := &captureHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	server.SetLocalInfileEnabled(false)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		_ = server.writeHandshake(serverConn)
	}()

	payload := mustReadPacket(t, clientConn)
	require.GreaterOrEqual(t, len(payload), 19)

	offset := 1
	for offset < len(payload) && payload[offset] != 0 {
		offset++
	}
	offset++
	offset += 4 + 8 + 1
	capsLower := binary.LittleEndian.Uint16(payload[offset : offset+2])
	require.Zero(t, capsLower&0x0080, "CLIENT_LOCAL_FILES should not be advertised")
}
