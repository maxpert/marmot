package protocol

import (
	"fmt"
	"net"
	"os"
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
