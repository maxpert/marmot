//go:build sqlite_preupdate_hook

package grpc

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/maxpert/marmot/grpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockTransactionStreamServer implements MarmotServiceServer for testing TransactionStream
type mockTransactionStreamServer struct {
	UnimplementedMarmotServiceServer
	chunksReceived atomic.Int32
	commitReceived atomic.Bool
	statements     []*Statement
	lastTxnID      uint64
	lastDatabase   string
	lastSourceNode uint64
}

func (m *mockTransactionStreamServer) TransactionStream(stream grpc.ClientStreamingServer[TransactionStreamMessage, TransactionResponse]) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&TransactionResponse{
				Success: true,
			})
		}
		if err != nil {
			return err
		}

		switch payload := msg.GetPayload().(type) {
		case *TransactionStreamMessage_Chunk:
			m.chunksReceived.Add(1)
			m.statements = append(m.statements, payload.Chunk.Statements...)
			m.lastTxnID = payload.Chunk.TxnId
			m.lastDatabase = payload.Chunk.Database
		case *TransactionStreamMessage_Commit:
			m.commitReceived.Store(true)
			m.lastTxnID = payload.Commit.TxnId
			m.lastDatabase = payload.Commit.Database
			m.lastSourceNode = payload.Commit.SourceNodeId
		}
	}
}

func TestClient_TransactionStream_ChunksSentCorrectly(t *testing.T) {
	// Start mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	mockServer := &mockTransactionStreamServer{}
	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	// Create client and connect
	client := NewClient(1)
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client.RegisterTestConnection(2, conn)

	// Create 125 statements (should result in 3 chunks: 50 + 50 + 25)
	statements := make([]*Statement, 125)
	for i := range statements {
		statements[i] = &Statement{
			Type:      common.StatementType_INSERT,
			TableName: "test_table",
			Database:  "test_db",
			Payload: &Statement_RowChange{
				RowChange: &RowChange{
					IntentKey: []byte("key"),
					NewValues: map[string][]byte{"id": []byte("1")},
				},
			},
		}
	}

	timestamp := &HLC{
		WallTime: 1000,
		Logical:  1,
		NodeId:   1,
	}

	// Execute
	resp, err := client.TransactionStream(
		context.Background(),
		2,
		12345,
		"test_db",
		statements,
		timestamp,
		1,
	)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)

	// Verify 3 chunks were sent (50 + 50 + 25 = 125 statements)
	assert.Equal(t, int32(3), mockServer.chunksReceived.Load())
	assert.True(t, mockServer.commitReceived.Load())
	assert.Equal(t, 125, len(mockServer.statements))
	assert.Equal(t, uint64(12345), mockServer.lastTxnID)
	assert.Equal(t, "test_db", mockServer.lastDatabase)
	assert.Equal(t, uint64(1), mockServer.lastSourceNode)
}

func TestClient_TransactionStream_SingleChunk(t *testing.T) {
	// Start mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	mockServer := &mockTransactionStreamServer{}
	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	// Create client and connect
	client := NewClient(1)
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client.RegisterTestConnection(2, conn)

	// Create 30 statements (should result in 1 chunk)
	statements := make([]*Statement, 30)
	for i := range statements {
		statements[i] = &Statement{
			Type:      common.StatementType_UPDATE,
			TableName: "users",
			Database:  "mydb",
		}
	}

	timestamp := &HLC{
		WallTime: 2000,
		Logical:  2,
		NodeId:   2,
	}

	resp, err := client.TransactionStream(
		context.Background(),
		2,
		99999,
		"mydb",
		statements,
		timestamp,
		5,
	)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)

	// Verify 1 chunk was sent
	assert.Equal(t, int32(1), mockServer.chunksReceived.Load())
	assert.True(t, mockServer.commitReceived.Load())
	assert.Equal(t, 30, len(mockServer.statements))
}

func TestClient_TransactionStream_ExactlyFiftyStatements(t *testing.T) {
	// Start mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	mockServer := &mockTransactionStreamServer{}
	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	// Create client and connect
	client := NewClient(1)
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client.RegisterTestConnection(2, conn)

	// Create exactly 50 statements (boundary case - exactly 1 chunk)
	statements := make([]*Statement, 50)
	for i := range statements {
		statements[i] = &Statement{
			Type:      common.StatementType_DELETE,
			TableName: "orders",
			Database:  "shop",
		}
	}

	timestamp := &HLC{
		WallTime: 3000,
		Logical:  0,
		NodeId:   1,
	}

	resp, err := client.TransactionStream(
		context.Background(),
		2,
		54321,
		"shop",
		statements,
		timestamp,
		10,
	)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)

	// Verify exactly 1 chunk was sent
	assert.Equal(t, int32(1), mockServer.chunksReceived.Load())
	assert.True(t, mockServer.commitReceived.Load())
	assert.Equal(t, 50, len(mockServer.statements))
}

func TestClient_TransactionStream_EmptyStatements(t *testing.T) {
	// Start mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	mockServer := &mockTransactionStreamServer{}
	grpcServer := grpc.NewServer()
	RegisterMarmotServiceServer(grpcServer, mockServer)

	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	// Create client and connect
	client := NewClient(1)
	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client.RegisterTestConnection(2, conn)

	// No statements - only commit should be sent
	timestamp := &HLC{
		WallTime: 4000,
		Logical:  1,
		NodeId:   1,
	}

	resp, err := client.TransactionStream(
		context.Background(),
		2,
		11111,
		"empty_db",
		[]*Statement{},
		timestamp,
		7,
	)

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)

	// No chunks should be sent
	assert.Equal(t, int32(0), mockServer.chunksReceived.Load())
	assert.True(t, mockServer.commitReceived.Load())
	assert.Equal(t, 0, len(mockServer.statements))
}

func TestClient_TransactionStream_NotConnected(t *testing.T) {
	client := NewClient(1)

	// Try to stream to a node we're not connected to
	resp, err := client.TransactionStream(
		context.Background(),
		999, // Non-existent node
		12345,
		"test_db",
		[]*Statement{},
		&HLC{},
		1,
	)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "not connected to node 999")
}
