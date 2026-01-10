//go:build sqlite_preupdate_hook

package grpc

import (
	"testing"

	"github.com/maxpert/marmot/grpc/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionChunk_BasicFields(t *testing.T) {
	chunk := &TransactionChunk{
		TxnId:      12345,
		Database:   "test_db",
		ChunkIndex: 0,
		Statements: []*Statement{
			{
				Type:      common.StatementType_INSERT,
				TableName: "users",
				Database:  "test_db",
				Payload: &Statement_RowChange{
					RowChange: &RowChange{
						IntentKey: []byte("users:1"),
						NewValues: map[string][]byte{
							"id":   []byte("1"),
							"name": []byte("Alice"),
						},
					},
				},
			},
		},
	}

	assert.Equal(t, uint64(12345), chunk.TxnId)
	assert.Equal(t, "test_db", chunk.Database)
	assert.Equal(t, uint32(0), chunk.ChunkIndex)
	require.Len(t, chunk.Statements, 1)
	assert.Equal(t, common.StatementType_INSERT, chunk.Statements[0].Type)
}

func TestTransactionCommit_BasicFields(t *testing.T) {
	commit := &TransactionCommit{
		TxnId:        12345,
		Database:     "test_db",
		SourceNodeId: 1,
		Timestamp: &HLC{
			WallTime: 1000,
			Logical:  1,
			NodeId:   1,
		},
	}

	assert.Equal(t, uint64(12345), commit.TxnId)
	assert.Equal(t, "test_db", commit.Database)
	assert.Equal(t, uint64(1), commit.SourceNodeId)
	assert.Equal(t, int64(1000), commit.Timestamp.WallTime)
}

func TestTransactionStreamMessage_ChunkPayload(t *testing.T) {
	chunk := &TransactionChunk{
		TxnId:      12345,
		Database:   "test_db",
		ChunkIndex: 0,
	}

	msg := &TransactionStreamMessage{
		Payload: &TransactionStreamMessage_Chunk{
			Chunk: chunk,
		},
	}

	// Verify oneof works correctly
	payload := msg.GetPayload()
	require.NotNil(t, payload)

	gotChunk := msg.GetChunk()
	require.NotNil(t, gotChunk)
	assert.Equal(t, uint64(12345), gotChunk.TxnId)

	// Commit should be nil
	assert.Nil(t, msg.GetCommit())
}

func TestTransactionStreamMessage_CommitPayload(t *testing.T) {
	commit := &TransactionCommit{
		TxnId:        12345,
		Database:     "test_db",
		SourceNodeId: 1,
	}

	msg := &TransactionStreamMessage{
		Payload: &TransactionStreamMessage_Commit{
			Commit: commit,
		},
	}

	// Verify oneof works correctly
	gotCommit := msg.GetCommit()
	require.NotNil(t, gotCommit)
	assert.Equal(t, uint64(12345), gotCommit.TxnId)

	// Chunk should be nil
	assert.Nil(t, msg.GetChunk())
}

func TestTransactionStreamMessage_MultipleChunks(t *testing.T) {
	// Simulate building multiple chunks for a large transaction
	chunks := make([]*TransactionStreamMessage, 3)

	for i := 0; i < 3; i++ {
		chunks[i] = &TransactionStreamMessage{
			Payload: &TransactionStreamMessage_Chunk{
				Chunk: &TransactionChunk{
					TxnId:      12345,
					Database:   "test_db",
					ChunkIndex: uint32(i),
					Statements: []*Statement{
						{
							Type:      common.StatementType_INSERT,
							TableName: "users",
							Database:  "test_db",
						},
					},
				},
			},
		}
	}

	// Verify chunk indices are sequential
	for i, msg := range chunks {
		chunk := msg.GetChunk()
		require.NotNil(t, chunk)
		assert.Equal(t, uint32(i), chunk.ChunkIndex)
	}
}
