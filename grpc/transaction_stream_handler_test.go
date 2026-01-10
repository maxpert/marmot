//go:build sqlite_preupdate_hook

package grpc

import (
	"context"
	"os"
	"testing"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// mockClientStreamingServer implements grpc.ClientStreamingServer for testing
type mockClientStreamingServer struct {
	grpc.ServerStream
	messages []*TransactionStreamMessage
	index    int
	response *TransactionResponse
	ctx      context.Context
}

func newMockStream(ctx context.Context, msgs []*TransactionStreamMessage) *mockClientStreamingServer {
	return &mockClientStreamingServer{
		messages: msgs,
		ctx:      ctx,
	}
}

func (m *mockClientStreamingServer) Recv() (*TransactionStreamMessage, error) {
	if m.index >= len(m.messages) {
		return nil, nil // Simulate end of stream without proper EOF
	}
	msg := m.messages[m.index]
	m.index++
	return msg, nil
}

func (m *mockClientStreamingServer) SendAndClose(resp *TransactionResponse) error {
	m.response = resp
	return nil
}

func (m *mockClientStreamingServer) Context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

// setupTestServer creates a test server with database manager
func setupTestServer(t *testing.T) (*Server, *db.DatabaseManager, string, func()) {
	tmpDir, err := os.MkdirTemp("", "marmot_stream_test")
	require.NoError(t, err)

	clock := hlc.NewClock(1)
	dbMgr, err := db.NewDatabaseManager(tmpDir, 1, clock)
	require.NoError(t, err)

	server := &Server{
		nodeID:    1,
		dbManager: dbMgr,
		registry:  NewNodeRegistry(1, "localhost:5050"),
	}

	cleanup := func() {
		dbMgr.Close()
		os.RemoveAll(tmpDir)
	}

	return server, dbMgr, tmpDir, cleanup
}

// encodeMsgpackValue encodes a value using msgpack for CDC test data
func encodeMsgpackValue(t *testing.T, v interface{}) []byte {
	data, err := encoding.Marshal(v)
	require.NoError(t, err)
	return data
}

func TestTransactionStream_BasicFlow(t *testing.T) {
	server, dbMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create test database with table
	testDB := "stream_test_db"
	err := dbMgr.CreateDatabase(testDB)
	require.NoError(t, err)

	dbInstance, err := dbMgr.GetDatabase(testDB)
	require.NoError(t, err)

	// Create test table
	_, err = dbInstance.GetDB().Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`)
	require.NoError(t, err)

	// Begin a transaction via TransactionManager (simulating PREPARE phase)
	txnMgr := dbInstance.GetTransactionManager()
	clock := hlc.NewClock(2)
	startTS := clock.Now()
	txnID := startTS.ToTxnID()

	txn, err := txnMgr.BeginTransactionWithID(txnID, 2, startTS)
	require.NoError(t, err)
	require.NotNil(t, txn)

	// Build test CDC data using msgpack encoding
	newValues1 := map[string][]byte{
		"id":    encodeMsgpackValue(t, int64(1)),
		"name":  encodeMsgpackValue(t, "Alice"),
		"email": encodeMsgpackValue(t, "alice@example.com"),
	}

	newValues2 := map[string][]byte{
		"id":    encodeMsgpackValue(t, int64(2)),
		"name":  encodeMsgpackValue(t, "Bob"),
		"email": encodeMsgpackValue(t, "bob@example.com"),
	}

	// Build stream messages: 2 chunks + commit
	messages := []*TransactionStreamMessage{
		{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      txnID,
					Database:   testDB,
					ChunkIndex: 0,
					Statements: []*Statement{
						{
							Type:      pb.StatementType_INSERT,
							TableName: "users",
							Database:  testDB,
							Payload: &Statement_RowChange{
								RowChange: &RowChange{
									IntentKey: []byte("users:1"),
									NewValues: newValues1,
								},
							},
						},
					},
				},
			},
		},
		{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      txnID,
					Database:   testDB,
					ChunkIndex: 1,
					Statements: []*Statement{
						{
							Type:      pb.StatementType_INSERT,
							TableName: "users",
							Database:  testDB,
							Payload: &Statement_RowChange{
								RowChange: &RowChange{
									IntentKey: []byte("users:2"),
									NewValues: newValues2,
								},
							},
						},
					},
				},
			},
		},
		{
			Payload: &TransactionStreamMessage_Commit{
				Commit: &TransactionCommit{
					TxnId:        txnID,
					Database:     testDB,
					SourceNodeId: 2,
					Timestamp: &HLC{
						WallTime: startTS.WallTime + 1000,
						Logical:  0,
						NodeId:   2,
					},
				},
			},
		},
	}

	// Create mock stream and call handler
	ctx := context.Background()
	stream := newMockStream(ctx, messages)
	err = server.TransactionStream(stream)
	require.NoError(t, err)

	// Verify response
	require.NotNil(t, stream.response)
	assert.True(t, stream.response.Success, "Expected success but got error: %s", stream.response.ErrorMessage)

	// Verify data was written to SQLite
	var count int
	err = dbInstance.GetDB().QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "Expected 2 users in database")

	// Verify specific data
	var name, email string
	err = dbInstance.GetDB().QueryRow("SELECT name, email FROM users WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
	assert.Equal(t, "alice@example.com", email)
}

func TestTransactionStream_CommitNotFound(t *testing.T) {
	server, dbMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create test database
	testDB := "commit_notfound_db"
	err := dbMgr.CreateDatabase(testDB)
	require.NoError(t, err)

	// Use a txnID that was never prepared
	nonExistentTxnID := uint64(999999999)

	// Build stream with just a commit (no PREPARE was done)
	messages := []*TransactionStreamMessage{
		{
			Payload: &TransactionStreamMessage_Commit{
				Commit: &TransactionCommit{
					TxnId:        nonExistentTxnID,
					Database:     testDB,
					SourceNodeId: 2,
					Timestamp: &HLC{
						WallTime: 1000000,
						Logical:  0,
						NodeId:   2,
					},
				},
			},
		},
	}

	ctx := context.Background()
	stream := newMockStream(ctx, messages)
	err = server.TransactionStream(stream)
	require.NoError(t, err)

	// Verify error response
	require.NotNil(t, stream.response)
	assert.False(t, stream.response.Success, "Expected failure for non-existent transaction")
	assert.Contains(t, stream.response.ErrorMessage, "not found")
}

func TestTransactionStream_ChunkError_DatabaseNotFound(t *testing.T) {
	server, _, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Use non-existent database
	messages := []*TransactionStreamMessage{
		{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      12345,
					Database:   "nonexistent_db",
					ChunkIndex: 0,
					Statements: []*Statement{
						{
							Type:      pb.StatementType_INSERT,
							TableName: "users",
							Database:  "nonexistent_db",
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	stream := newMockStream(ctx, messages)
	err := server.TransactionStream(stream)
	require.NoError(t, err)

	// Verify error response
	require.NotNil(t, stream.response)
	assert.False(t, stream.response.Success)
	assert.Contains(t, stream.response.ErrorMessage, "not found")
}

func TestTransactionStream_TxnIDMismatch(t *testing.T) {
	server, dbMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create test database
	testDB := "mismatch_db"
	err := dbMgr.CreateDatabase(testDB)
	require.NoError(t, err)

	// Build stream with mismatched txn_ids
	messages := []*TransactionStreamMessage{
		{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      12345,
					Database:   testDB,
					ChunkIndex: 0,
				},
			},
		},
		{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      99999, // Different txn_id
					Database:   testDB,
					ChunkIndex: 1,
				},
			},
		},
	}

	ctx := context.Background()
	stream := newMockStream(ctx, messages)
	err = server.TransactionStream(stream)
	require.NoError(t, err)

	// Verify error response
	require.NotNil(t, stream.response)
	assert.False(t, stream.response.Success)
	assert.Contains(t, stream.response.ErrorMessage, "mismatch")
}

func TestTransactionStream_EmptyCommit(t *testing.T) {
	server, dbMgr, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create test database
	testDB := "empty_commit_db"
	err := dbMgr.CreateDatabase(testDB)
	require.NoError(t, err)

	// Build stream with empty commit payload
	messages := []*TransactionStreamMessage{
		{
			Payload: &TransactionStreamMessage_Commit{
				Commit: nil, // Empty commit
			},
		},
	}

	ctx := context.Background()
	stream := newMockStream(ctx, messages)
	err = server.TransactionStream(stream)
	require.NoError(t, err)

	// Verify error response
	require.NotNil(t, stream.response)
	assert.False(t, stream.response.Success)
	assert.Contains(t, stream.response.ErrorMessage, "empty commit")
}

func TestTransactionStream_NoDatabaseManager(t *testing.T) {
	// Server with no database manager
	server := &Server{
		nodeID:    1,
		dbManager: nil,
		registry:  NewNodeRegistry(1, "localhost:5050"),
	}

	messages := []*TransactionStreamMessage{
		{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      12345,
					Database:   "test_db",
					ChunkIndex: 0,
				},
			},
		},
	}

	ctx := context.Background()
	stream := newMockStream(ctx, messages)
	err := server.TransactionStream(stream)
	require.NoError(t, err)

	// Verify error response
	require.NotNil(t, stream.response)
	assert.False(t, stream.response.Success)
	assert.Contains(t, stream.response.ErrorMessage, "database manager not initialized")
}
