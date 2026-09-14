package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockHandler is a minimal ConnectionHandler for testing
type mockHandler struct{}

func (m *mockHandler) HandleQuery(session *ConnectionSession, sql string, params []interface{}) (*ResultSet, error) {
	// Handle system variable queries from MySQL driver
	if len(sql) >= 6 && sql[:6] == "SELECT" {
		return &ResultSet{
			Columns: []ColumnDef{{Name: "value", Type: 0xFD}},
			Rows:    [][]interface{}{{"0"}},
		}, nil
	}
	// Return empty result set for other queries
	return &ResultSet{
		Columns:      []ColumnDef{},
		Rows:         [][]interface{}{},
		RowsAffected: 0,
		LastInsertId: 0,
	}, nil
}

func TestMySQLServer_TCPOnly(t *testing.T) {
	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)

	err := server.Start()
	require.NoError(t, err, "Failed to start MySQL server")
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Get actual port from listener
	require.Len(t, server.listeners, 1, "Expected exactly 1 listener (TCP only)")
	tcpAddr := server.listeners[0].Addr().String()

	// Verify TCP connection works using raw socket
	conn, err := net.Dial("tcp", tcpAddr)
	require.NoError(t, err, "Failed to dial TCP address")
	defer conn.Close()

	// Read handshake packet (should receive MySQL handshake)
	header := make([]byte, 4)
	_, err = conn.Read(header)
	require.NoError(t, err, "Failed to read handshake header")

	// Verify packet header format (length + sequence)
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	require.Greater(t, length, 0, "Handshake packet length should be > 0")
	require.Equal(t, byte(0), header[3], "First packet sequence should be 0")
}

func TestMySQLServer_TCPAndUnixSocket(t *testing.T) {
	socketPath := "/tmp/marmot_test_tcp_unix.sock"
	defer os.Remove(socketPath)

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", socketPath, 0660, handler)

	err := server.Start()
	require.NoError(t, err, "Failed to start MySQL server")
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify socket file was created
	_, err = os.Stat(socketPath)
	require.NoError(t, err, "Unix socket file should exist")

	// Verify both listeners were created
	require.Len(t, server.listeners, 2, "Expected exactly 2 listeners (TCP + Unix socket)")

	// Verify TCP connection works using raw socket
	tcpConn, err := net.Dial("tcp", server.listeners[0].Addr().String())
	require.NoError(t, err, "Failed to dial TCP address")
	defer tcpConn.Close()

	// Read handshake packet from TCP
	tcpHeader := make([]byte, 4)
	_, err = tcpConn.Read(tcpHeader)
	require.NoError(t, err, "Failed to read TCP handshake header")
	require.Greater(t, int(tcpHeader[0])|int(tcpHeader[1])<<8|int(tcpHeader[2])<<16, 0, "TCP handshake packet length should be > 0")

	// Verify Unix socket connection works using raw socket
	unixConn, err := net.Dial("unix", socketPath)
	require.NoError(t, err, "Failed to dial Unix socket")
	defer unixConn.Close()

	// Read handshake packet from Unix socket
	unixHeader := make([]byte, 4)
	_, err = unixConn.Read(unixHeader)
	require.NoError(t, err, "Failed to read Unix socket handshake header")
	require.Greater(t, int(unixHeader[0])|int(unixHeader[1])<<8|int(unixHeader[2])<<16, 0, "Unix socket handshake packet length should be > 0")
}

func TestMySQLServer_StaleSocketCleanup(t *testing.T) {
	socketPath := "/tmp/marmot_test_stale.sock"
	defer os.Remove(socketPath)

	// Pre-create a stale socket file
	staleFile, err := os.Create(socketPath)
	require.NoError(t, err, "Failed to create stale socket file")
	staleFile.Close()

	// Verify stale file exists
	_, err = os.Stat(socketPath)
	require.NoError(t, err, "Stale socket file should exist before server start")

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", socketPath, 0660, handler)

	err = server.Start()
	require.NoError(t, err, "Failed to start MySQL server (should cleanup stale socket)")
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify the stale file was removed and a new socket was created
	stat, err := os.Stat(socketPath)
	require.NoError(t, err, "Unix socket file should exist after cleanup")

	// Verify it's a socket, not a regular file
	require.NotEqual(t, os.ModeType, stat.Mode()&os.ModeType, "Socket file should not be a regular file")
}

func TestMySQLServer_SocketPermissions(t *testing.T) {
	socketPath := "/tmp/marmot_test_perms.sock"
	defer os.Remove(socketPath)

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", socketPath, 0600, handler)

	err := server.Start()
	require.NoError(t, err, "Failed to start MySQL server")
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify socket permissions
	stat, err := os.Stat(socketPath)
	require.NoError(t, err, "Unix socket file should exist")

	// Check permissions (mask off the file type bits)
	perm := stat.Mode().Perm()
	require.Equal(t, os.FileMode(0600), perm, "Socket permissions should be 0600")
}

func TestMySQLServer_SocketCleanupOnStop(t *testing.T) {
	socketPath := "/tmp/marmot_test_cleanup.sock"
	defer os.Remove(socketPath)

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", socketPath, 0660, handler)

	err := server.Start()
	require.NoError(t, err, "Failed to start MySQL server")

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify socket file exists
	_, err = os.Stat(socketPath)
	require.NoError(t, err, "Unix socket file should exist while server is running")

	// Stop server
	server.Stop()

	// Verify socket file was removed
	_, err = os.Stat(socketPath)
	require.True(t, os.IsNotExist(err), "Unix socket file should be removed after server stop")
}

func TestMySQLServer_MultipleConnections(t *testing.T) {
	socketPath := "/tmp/marmot_test_multi.sock"
	defer os.Remove(socketPath)

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", socketPath, 0660, handler)

	err := server.Start()
	require.NoError(t, err, "Failed to start MySQL server")
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Create multiple connections via TCP using raw sockets
	var tcpConns []net.Conn
	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", server.listeners[0].Addr().String())
		require.NoError(t, err, fmt.Sprintf("Failed to open TCP connection %d", i))
		defer conn.Close()

		// Read handshake to verify connection is alive
		header := make([]byte, 4)
		_, err = conn.Read(header)
		require.NoError(t, err, fmt.Sprintf("Failed to read handshake for TCP connection %d", i))

		tcpConns = append(tcpConns, conn)
	}

	// Create multiple connections via Unix socket using raw sockets
	var unixConns []net.Conn
	for i := 0; i < 5; i++ {
		conn, err := net.Dial("unix", socketPath)
		require.NoError(t, err, fmt.Sprintf("Failed to open Unix socket connection %d", i))
		defer conn.Close()

		// Read handshake to verify connection is alive
		header := make([]byte, 4)
		_, err = conn.Read(header)
		require.NoError(t, err, fmt.Sprintf("Failed to read handshake for Unix socket connection %d", i))

		unixConns = append(unixConns, conn)
	}

	// Verify all connections are still alive by writing a small data packet
	testData := []byte{0x01, 0x00, 0x00, 0x00, 0x01} // Minimal packet
	for i, conn := range tcpConns {
		_, err = conn.Write(testData)
		require.NoError(t, err, fmt.Sprintf("TCP connection %d died", i))
	}

	for i, conn := range unixConns {
		_, err = conn.Write(testData)
		require.NoError(t, err, fmt.Sprintf("Unix socket connection %d died", i))
	}
}

func TestMySQLServer_RawSocketConnection(t *testing.T) {
	socketPath := "/tmp/marmot_test_raw.sock"
	defer os.Remove(socketPath)

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", socketPath, 0660, handler)

	err := server.Start()
	require.NoError(t, err, "Failed to start MySQL server")
	defer server.Stop()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test raw connection to Unix socket
	conn, err := net.Dial("unix", socketPath)
	require.NoError(t, err, "Failed to dial Unix socket")
	defer conn.Close()

	// Read handshake packet (should receive MySQL handshake)
	header := make([]byte, 4)
	_, err = conn.Read(header)
	require.NoError(t, err, "Failed to read handshake header")

	// Verify packet header format (length + sequence)
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	require.Greater(t, length, 0, "Handshake packet length should be > 0")
	require.Equal(t, byte(0), header[3], "First packet sequence should be 0")
}

// --- connection-tracking helpers ---

// readMySQLPacket reads a full MySQL packet (4-byte header + payload).
func readMySQLPacket(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	header := make([]byte, 4)
	_, err := io.ReadFull(conn, header)
	require.NoError(t, err, "readMySQLPacket: header read failed")
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	require.NoError(t, err, "readMySQLPacket: payload read failed")
	return payload
}

// writeMySQLPacket writes a MySQL packet (4-byte header + payload).
func writeMySQLPacket(t *testing.T, conn net.Conn, seq byte, payload []byte) {
	t.Helper()
	header := make([]byte, 4)
	header[0] = byte(len(payload))
	header[1] = byte(len(payload) >> 8)
	header[2] = byte(len(payload) >> 16)
	header[3] = seq
	_, err := conn.Write(append(header, payload...))
	require.NoError(t, err, "writeMySQLPacket: write failed")
}

// sendHandshakeResponse reads the server's initial handshake packet and
// replies with a HandshakeResponse41 requesting dbName as the initial
// database. It does not wait for the server's OK/ERR response, so it is
// safe to use when that response may be withheld (e.g. a DatabaseEnsurer
// parked mid-call).
func sendHandshakeResponse(t *testing.T, conn net.Conn, dbName string) {
	t.Helper()

	// Server sends: Initial Handshake (seq=0)
	readMySQLPacket(t, conn)

	// Client sends: HandshakeResponse41 (seq=1)
	// Capability flags: CLIENT_PROTOCOL_41 | CLIENT_LONG_PASSWORD | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB
	caps := uint32(0x0000a209)
	capBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(capBytes, caps)

	var resp []byte
	resp = append(resp, capBytes...)                    // capabilities (4)
	resp = append(resp, 0, 0, 0, 1)                     // max packet size (4)
	resp = append(resp, 45)                             // charset (1)
	resp = append(resp, make([]byte, 23)...)            // reserved (23)
	resp = append(resp, "root\x00"...)                  // username
	resp = append(resp, 0)                              // auth data length (empty)
	resp = append(resp, dbName+"\x00"...)               // database
	resp = append(resp, "mysql_native_password\x00"...) // auth plugin name
	writeMySQLPacket(t, conn, 1, resp)
}

// completeHandshake performs the minimum MySQL handshake on a raw connection,
// requesting dbName as the initial database, and returns the server's
// authentication response payload (OK 0x00 or ERR 0xFF).
func completeHandshake(t *testing.T, conn net.Conn, dbName string) []byte {
	t.Helper()

	sendHandshakeResponse(t, conn, dbName)

	// Server sends: OK (seq=2) if accepted, ERR (seq=2) if draining
	return readMySQLPacket(t, conn)
}

// --- connection-tracking tests ---

func TestServerConnTracking_ActiveConnectionCount(t *testing.T) {
	t.Parallel()

	server := NewMySQLServer("127.0.0.1:0", "", 0, &mockHandler{})
	require.NoError(t, server.Start())
	defer server.Stop()

	addr := server.listeners[0].Addr().String()

	require.Equal(t, 0, server.ActiveConnectionCount(), "no connections initially")

	conn1, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn1.Close()
	resp1 := completeHandshake(t, conn1, "marmot")
	require.Equal(t, byte(0x00), resp1[0], "expected OK after handshake")

	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 1
	}, time.Second, 10*time.Millisecond, "expected 1 active connection")

	conn2, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn2.Close()
	resp2 := completeHandshake(t, conn2, "marmot")
	require.Equal(t, byte(0x00), resp2[0], "expected OK after handshake")

	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 2
	}, time.Second, 10*time.Millisecond, "expected 2 active connections")

	conn1.Close()
	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 1
	}, time.Second, 10*time.Millisecond, "count should drop to 1 after first connection closes")
}

func TestServerConnTracking_IsDraining(t *testing.T) {
	t.Parallel()

	server := NewMySQLServer("127.0.0.1:0", "", 0, &mockHandler{})
	require.NoError(t, server.Start())

	require.False(t, server.IsDraining(), "server should not be draining before Stop")

	server.Stop()

	require.True(t, server.IsDraining(), "server should be draining after Stop")
}

func TestServerConnTracking_DrainingRejectsNewConnections(t *testing.T) {
	t.Parallel()

	server := NewMySQLServer("127.0.0.1:0", "", 0, &mockHandler{})
	require.NoError(t, server.Start())
	defer server.Stop()

	addr := server.listeners[0].Addr().String()

	// One connection established before draining.
	conn1, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn1.Close()
	resp1 := completeHandshake(t, conn1, "marmot")
	require.Equal(t, byte(0x00), resp1[0], "existing connection should get OK")

	// Set draining flag without closing the listener so we can complete
	// the TCP+MySQL handshake for the second connection.
	server.draining.Store(true)

	conn2, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn2.Close()
	resp2 := completeHandshake(t, conn2, "marmot")
	// Error packet header byte is 0xFF.
	require.Equal(t, byte(0xFF), resp2[0], "new connection during drain should receive error packet")
	errCode := binary.LittleEndian.Uint16(resp2[1:3])
	require.Equal(t, uint16(1053), errCode, "error code should be ER_SERVER_SHUTDOWN (1053)")
}

func TestServerConnTracking_GracefulDrain(t *testing.T) {
	t.Parallel()

	server := NewMySQLServer("127.0.0.1:0", "", 0, &mockHandler{})
	require.NoError(t, server.Start())

	addr := server.listeners[0].Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	resp := completeHandshake(t, conn, "marmot")
	require.Equal(t, byte(0x00), resp[0], "expected OK after handshake")

	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 1
	}, time.Second, 10*time.Millisecond, "expected 1 active connection")

	// GracefulDrain with a short timeout should force-close the remaining connection.
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.GracefulDrain(200 * time.Millisecond)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GracefulDrain did not return within expected time")
	}

	require.Equal(t, 0, server.ActiveConnectionCount(), "all connections should be gone after GracefulDrain")

	server.Stop()
}

// blockingEnsurerHandler is a ConnectionHandler that also implements
// DatabaseEnsurer, whose EnsureDatabase parks until release is closed. It
// closes entered exactly once, the first time EnsureDatabase is called, so
// tests can synchronize on "a connection is parked inside EnsureDatabase"
// without sleeping and hoping.
type blockingEnsurerHandler struct {
	mockHandler

	entered     chan struct{}
	enteredOnce sync.Once
	release     chan struct{}
}

func newBlockingEnsurerHandler() *blockingEnsurerHandler {
	return &blockingEnsurerHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (h *blockingEnsurerHandler) EnsureDatabase(_ *ConnectionSession, _ string) error {
	h.enteredOnce.Do(func() { close(h.entered) })
	<-h.release
	return nil
}

// TestServerConnTracking_ActiveConnectionCount_ParkedInEnsureDatabase proves
// that a connection parked inside a slow DatabaseEnsurer.EnsureDatabase call
// (e.g. a multi-second 2PC CREATE DATABASE) is already registered in
// activeConns, and therefore visible to ActiveConnectionCount(), before the
// OK packet is ever written.
func TestServerConnTracking_ActiveConnectionCount_ParkedInEnsureDatabase(t *testing.T) {
	t.Parallel()

	handler := newBlockingEnsurerHandler()
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	addr := server.listeners[0].Addr().String()
	require.Equal(t, 0, server.ActiveConnectionCount(), "no connections initially")

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	sendHandshakeResponse(t, conn, "newdb")

	// Wait for the connection to actually be inside EnsureDatabase, rather
	// than sleeping and hoping.
	select {
	case <-handler.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("EnsureDatabase was never entered")
	}

	require.Equal(t, 1, server.ActiveConnectionCount(),
		"connection parked in EnsureDatabase must already be registered as active")

	close(handler.release)

	// The OK packet should now arrive, and the count should settle back to 0
	// once the connection is closed.
	resp := readMySQLPacket(t, conn)
	require.Equal(t, byte(0x00), resp[0], "expected OK once EnsureDatabase returns")

	conn.Close()
	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 0
	}, time.Second, 10*time.Millisecond, "count should return to 0 after the connection finishes")
}

// TestServerConnTracking_GracefulDrain_ParkedInEnsureDatabase proves that
// GracefulDrain can see and force-close a connection parked inside
// EnsureDatabase, not merely that ActiveConnectionCount reports it. Closing
// the socket does not by itself unblock a goroutine parked in EnsureDatabase
// (it observes no context tied to the conn), so the test verifies the two
// halves separately: (1) the Range force-close reaches and closes the
// connection's socket while EnsureDatabase is still blocked, observable from
// the client side, and while GracefulDrain is still waiting on connWg; then
// (2) releasing EnsureDatabase lets the handler goroutine exit, at which
// point GracefulDrain's connWg.Wait() unblocks and it returns.
func TestServerConnTracking_GracefulDrain_ParkedInEnsureDatabase(t *testing.T) {
	t.Parallel()

	handler := newBlockingEnsurerHandler()
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())

	addr := server.listeners[0].Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	sendHandshakeResponse(t, conn, "newdb")

	select {
	case <-handler.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("EnsureDatabase was never entered")
	}

	require.Equal(t, 1, server.ActiveConnectionCount(), "expected 1 active connection parked in EnsureDatabase")

	done := make(chan struct{})
	go func() {
		defer close(done)
		server.GracefulDrain(200 * time.Millisecond)
	}()

	// The force-close should reach the parked connection's socket well
	// before EnsureDatabase ever returns: prove it from the client side.
	require.Eventually(t, func() bool {
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		_, err := conn.Read(make([]byte, 1))
		return err != nil
	}, 2*time.Second, 20*time.Millisecond, "GracefulDrain should force-close the socket of a connection parked in EnsureDatabase")

	// At this point EnsureDatabase is still blocked, so GracefulDrain must
	// still be waiting on connWg rather than having already returned.
	select {
	case <-done:
		t.Fatal("GracefulDrain returned before the parked EnsureDatabase call finished")
	default:
	}
	require.Equal(t, 1, server.ActiveConnectionCount(),
		"the connection stays registered until its handleConnection goroutine actually exits")

	// Release EnsureDatabase; the handler goroutine can now exit, unblocking
	// GracefulDrain's connWg.Wait().
	close(handler.release)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("GracefulDrain did not return within expected time after EnsureDatabase was released")
	}

	require.Equal(t, 0, server.ActiveConnectionCount(), "connection parked in EnsureDatabase should be gone after GracefulDrain")

	server.Stop()
}
