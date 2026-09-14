package protocol

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingHandler is a ConnectionHandler that also implements DatabaseEnsurer
// and SessionCloser, for exercising the handshake/COM_INIT_DB database-ensure
// plumbing in server.go. ensureErr (if non-nil) is returned by every call to
// EnsureDatabase; every call is recorded so tests can assert call counts.
// closed receives the final CurrentDatabase value of every session that gets
// closed, giving tests a race-free way to observe post-handshake session
// state instead of sleeping or polling.
type recordingHandler struct {
	mockHandler

	mu        sync.Mutex
	ensureErr error
	calls     []ensureCall

	closed chan string
}

type ensureCall struct {
	name    string
	session *ConnectionSession
}

func newRecordingHandler(ensureErr error) *recordingHandler {
	return &recordingHandler{
		ensureErr: ensureErr,
		closed:    make(chan string, 8),
	}
}

func (h *recordingHandler) EnsureDatabase(session *ConnectionSession, name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, ensureCall{name: name, session: session})
	return h.ensureErr
}

// setEnsureErr changes the error EnsureDatabase returns for subsequent calls.
func (h *recordingHandler) setEnsureErr(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ensureErr = err
}

func (h *recordingHandler) CloseSession(session *ConnectionSession) {
	h.closed <- session.CurrentDatabase
}

func (h *recordingHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

// noEnsurerHandler implements ConnectionHandler + SessionCloser but NOT
// DatabaseEnsurer, to exercise the regression-guard arms proving the
// optional-interface type assertion in server.go doesn't break handlers
// that don't opt in.
type noEnsurerHandler struct {
	mockHandler

	closed chan string
}

func newNoEnsurerHandler() *noEnsurerHandler {
	return &noEnsurerHandler{closed: make(chan string, 8)}
}

func (h *noEnsurerHandler) CloseSession(session *ConnectionSession) {
	h.closed <- session.CurrentDatabase
}

// readERRCode extracts the 2-byte little-endian error code from a MySQL ERR
// packet payload, following the same inline parsing style already used
// elsewhere in this package (see TestServerConnTracking_DrainingRejectsNewConnections
// and load_data_local_test.go): byte 0 is the 0xFF marker, bytes 1-2 are the
// error code.
func readERRCode(t *testing.T, payload []byte) uint16 {
	t.Helper()
	require.Equal(t, byte(0xFF), payload[0], "expected ERR packet marker 0xFF")
	return binary.LittleEndian.Uint16(payload[1:3])
}

// --- Arm 1: handshake, ensurer returns nil -> OK, CurrentDatabase set. ---

func TestDatabaseEnsurer_Handshake_Success(t *testing.T) {
	t.Parallel()

	handler := newRecordingHandler(nil)
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	resp := completeHandshake(t, conn, "newdb")
	require.Equal(t, byte(0x00), resp[0], "expected OK packet after successful ensure")
	require.Equal(t, 1, handler.callCount(), "EnsureDatabase should be called exactly once")

	require.NoError(t, conn.Close())

	select {
	case finalDB := <-handler.closed:
		require.Equal(t, "newdb", finalDB, "CurrentDatabase should be set to the requested db")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CloseSession")
	}
}

// --- Arm 2: handshake, ensurer returns ErrUnknownDatabase -> ERR 1049, connection closed, CurrentDatabase unchanged. ---

func TestDatabaseEnsurer_Handshake_UnknownDatabase(t *testing.T) {
	t.Parallel()

	handler := newRecordingHandler(ErrUnknownDatabase("missingdb"))
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	resp := completeHandshake(t, conn, "missingdb")
	require.Equal(t, uint16(1049), readERRCode(t, resp), "expected ER_BAD_DB_ERROR (1049)")
	require.Equal(t, 1, handler.callCount(), "EnsureDatabase should be called exactly once")

	// Connection must be closed by the server after sending the handshake error.
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "connection should be closed after a failed handshake ensure")

	select {
	case finalDB := <-handler.closed:
		// handleConnection seeds CurrentDatabase to "marmot" before the
		// handshake is parsed; since EnsureDatabase fails, the assignment to
		// requestedDB never runs, so the default survives untouched.
		require.Equal(t, "marmot", finalDB, "CurrentDatabase must remain the default, not the requested db")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CloseSession")
	}
}

// --- Arm 3: handshake, handler doesn't implement DatabaseEnsurer -> unchanged today's behavior (regression guard). ---

func TestDatabaseEnsurer_Handshake_HandlerWithoutEnsurer(t *testing.T) {
	t.Parallel()

	handler := newNoEnsurerHandler()
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	resp := completeHandshake(t, conn, "somedb")
	require.Equal(t, byte(0x00), resp[0], "expected OK packet when handler doesn't implement DatabaseEnsurer")

	require.NoError(t, conn.Close())

	select {
	case finalDB := <-handler.closed:
		require.Equal(t, "somedb", finalDB, "CurrentDatabase should still be set from the handshake")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CloseSession")
	}
}

// --- Arm 4: COM_INIT_DB, ensurer returns nil -> OK, CurrentDatabase updated. ---

func TestDatabaseEnsurer_ComInitDB_Success(t *testing.T) {
	t.Parallel()

	handler := newRecordingHandler(nil)
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	resp := completeHandshake(t, conn, "marmot")
	require.Equal(t, byte(0x00), resp[0])
	require.Equal(t, 1, handler.callCount(), "handshake should trigger one EnsureDatabase call")

	// COM_INIT_DB is always sent as command sequence 0.
	writeMySQLPacket(t, conn, 0, append([]byte{0x02}, []byte("otherdb")...))
	initResp := readMySQLPacket(t, conn)
	require.Equal(t, byte(0x00), initResp[0], "expected OK packet for successful COM_INIT_DB")
	require.Equal(t, 2, handler.callCount(), "COM_INIT_DB should trigger a second EnsureDatabase call")

	require.NoError(t, conn.Close())

	select {
	case finalDB := <-handler.closed:
		require.Equal(t, "otherdb", finalDB, "CurrentDatabase should be updated by COM_INIT_DB")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CloseSession")
	}
}

// --- Arm 5: COM_INIT_DB, ensurer returns ErrUnknownDatabase -> ERR 1049, connection STAYS OPEN, CurrentDatabase unchanged. ---

func TestDatabaseEnsurer_ComInitDB_UnknownDatabase(t *testing.T) {
	t.Parallel()

	handler := newRecordingHandler(nil)
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Establish a known-good CurrentDatabase via a successful handshake.
	resp := completeHandshake(t, conn, "marmot")
	require.Equal(t, byte(0x00), resp[0])

	// Now make the ensurer fail for the following COM_INIT_DB.
	handler.setEnsureErr(ErrUnknownDatabase("missingdb"))

	writeMySQLPacket(t, conn, 0, append([]byte{0x02}, []byte("missingdb")...))
	initResp := readMySQLPacket(t, conn)
	require.Equal(t, uint16(1049), readERRCode(t, initResp), "expected ER_BAD_DB_ERROR (1049)")

	// Connection must stay open: send a follow-up COM_QUERY and get a normal response.
	sendComQuery(t, conn, "SELECT 1")
	queryResp := readMySQLPacket(t, conn)
	require.NotEqual(t, byte(0xFF), queryResp[0], "connection should stay open and serve the next command normally")

	require.NoError(t, conn.Close())

	select {
	case finalDB := <-handler.closed:
		require.Equal(t, "marmot", finalDB, "CurrentDatabase must remain the value from before the failed COM_INIT_DB")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CloseSession")
	}
}

// --- Arm 6: COM_INIT_DB, handler doesn't implement DatabaseEnsurer -> unchanged today's behavior (regression guard). ---

func TestDatabaseEnsurer_ComInitDB_HandlerWithoutEnsurer(t *testing.T) {
	t.Parallel()

	handler := newNoEnsurerHandler()
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	resp := completeHandshake(t, conn, "marmot")
	require.Equal(t, byte(0x00), resp[0])

	writeMySQLPacket(t, conn, 0, append([]byte{0x02}, []byte("otherdb")...))
	initResp := readMySQLPacket(t, conn)
	require.Equal(t, byte(0x00), initResp[0], "expected OK packet when handler doesn't implement DatabaseEnsurer")

	require.NoError(t, conn.Close())

	select {
	case finalDB := <-handler.closed:
		require.Equal(t, "otherdb", finalDB, "CurrentDatabase should still be updated by COM_INIT_DB")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CloseSession")
	}
}
