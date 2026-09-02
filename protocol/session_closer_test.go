package protocol

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sessionCloserHandler is a ConnectionHandler that also implements
// SessionCloser, recording every session it is asked to close as well as
// every session it saw a query from.
type sessionCloserHandler struct {
	mockHandler

	mu      sync.Mutex
	queried []*ConnectionSession
	closed  []*ConnectionSession
}

func (h *sessionCloserHandler) HandleQuery(session *ConnectionSession, sql string, params []interface{}) (*ResultSet, error) {
	h.mu.Lock()
	h.queried = append(h.queried, session)
	h.mu.Unlock()
	return h.mockHandler.HandleQuery(session, sql, params)
}

func (h *sessionCloserHandler) CloseSession(session *ConnectionSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = append(h.closed, session)
}

func (h *sessionCloserHandler) closedSessions() []*ConnectionSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*ConnectionSession, len(h.closed))
	copy(out, h.closed)
	return out
}

func (h *sessionCloserHandler) queriedSessions() []*ConnectionSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*ConnectionSession, len(h.queried))
	copy(out, h.queried)
	return out
}

// TestSessionCloser_CalledOnceOnDisconnect verifies CloseSession is invoked
// exactly once, with the same session used for the connection, when the
// client simply closes the socket after handshaking.
func TestSessionCloser_CalledOnceOnDisconnect(t *testing.T) {
	t.Parallel()

	handler := &sessionCloserHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	addr := server.listeners[0].Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	resp := completeHandshake(t, conn)
	require.Equal(t, byte(0x00), resp[0], "expected OK after handshake")

	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 1
	}, time.Second, 10*time.Millisecond, "expected 1 active connection")

	// Send a query so we know which session this connection was assigned.
	sendComQuery(t, conn, "SELECT 1")
	_ = readMySQLPacket(t, conn) // response to the query

	// Disconnect without COMMIT/ROLLBACK or COM_QUIT.
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool {
		return len(handler.closedSessions()) == 1
	}, time.Second, 10*time.Millisecond, "expected CloseSession to be called exactly once")

	closed := handler.closedSessions()
	require.Len(t, closed, 1)
	require.NotNil(t, closed[0])

	queried := handler.queriedSessions()
	require.Len(t, queried, 1)
	require.Equal(t, queried[0].ConnID, closed[0].ConnID,
		"CloseSession must receive the same session (ConnID) used for the connection's queries")
}

// TestSessionCloser_HandlerWithoutSessionCloser verifies that a handler
// which only implements ConnectionHandler (not SessionCloser) causes no
// panic and no behavior change when a connection closes.
func TestSessionCloser_HandlerWithoutSessionCloser(t *testing.T) {
	t.Parallel()

	handler := &mockHandler{}
	server := NewMySQLServer("127.0.0.1:0", "", 0, handler)
	require.NoError(t, server.Start())
	defer server.Stop()

	addr := server.listeners[0].Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	resp := completeHandshake(t, conn)
	require.Equal(t, byte(0x00), resp[0], "expected OK after handshake")

	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 1
	}, time.Second, 10*time.Millisecond, "expected 1 active connection")

	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool {
		return server.ActiveConnectionCount() == 0
	}, time.Second, 10*time.Millisecond, "connection should be deregistered without panicking")
}

// sendComQuery writes a COM_QUERY packet for sql on conn.
func sendComQuery(t *testing.T, conn net.Conn, sql string) {
	t.Helper()
	payload := append([]byte{0x03}, []byte(sql)...) // COM_QUERY = 0x03
	writeMySQLPacket(t, conn, 0, payload)
}
