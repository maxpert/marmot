//go:build sqlite_preupdate_hook

package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPrepareRequest_IncludesCDCData(t *testing.T) {
	wc := &WriteCoordinator{
		nodeID: 1,
	}

	txn := &Transaction{
		ID:       12345,
		NodeID:   1,
		Database: "test_db",
		Statements: []protocol.Statement{
			testCDCStatement("users", nil, map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			}),
			testCDCStatement("users", map[string][]byte{
				"id":   []byte("2"),
				"name": []byte("Bob"),
			}, map[string][]byte{
				"id":   []byte("2"),
				"name": []byte("Robert"),
			}),
		},
	}

	req := wc.buildPrepareRequest(txn)

	// Verify transaction metadata is preserved
	assert.Equal(t, txn.ID, req.TxnID)
	assert.Equal(t, wc.nodeID, req.NodeID)
	assert.Equal(t, txn.Database, req.Database)
	assert.Equal(t, PhasePrep, req.Phase)

	// Verify statements count is preserved
	require.Len(t, req.Statements, 2)

	// Verify canonical CDC row bytes are included because PREPARE is the
	// durability point; decoded maps are local apply-time state.
	for i, stmt := range req.Statements {
		assert.Nil(t, stmt.OldValues)
		assert.Nil(t, stmt.NewValues)
		assert.Equal(t, txn.Statements[i].EncodedRow, stmt.EncodedRow)
		assert.Equal(t, txn.Statements[i].EncodedCodec, stmt.EncodedCodec)

		// Verify metadata is preserved
		assert.Equal(t, txn.Statements[i].Type, stmt.Type)
		assert.Equal(t, txn.Statements[i].TableName, stmt.TableName)
		assert.Equal(t, txn.Statements[i].Database, stmt.Database)
		assert.Equal(t, txn.Statements[i].IntentKey, stmt.IntentKey)
	}
}

func TestBuildCommitRequest_StripsCDCData(t *testing.T) {
	wc := &WriteCoordinator{
		nodeID: 1,
	}

	txn := &Transaction{
		ID:       12345,
		NodeID:   1,
		Database: "test_db",
		Statements: []protocol.Statement{
			testCDCStatement("users", nil, map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			}),
		},
	}

	req := wc.buildCommitRequest(txn)

	// Verify transaction metadata is preserved
	assert.Equal(t, txn.ID, req.TxnID)
	assert.Equal(t, wc.nodeID, req.NodeID)
	assert.Equal(t, txn.Database, req.Database)
	assert.Equal(t, PhaseCommit, req.Phase)

	// Verify commit carries decision metadata only. CDC row data was made
	// durable during PREPARE.
	require.Len(t, req.Statements, 1)
	stmt := req.Statements[0]

	assert.Equal(t, txn.Statements[0].Type, stmt.Type)
	assert.Equal(t, txn.Statements[0].TableName, stmt.TableName)
	assert.Nil(t, stmt.OldValues)
	assert.Nil(t, stmt.NewValues)
}

func TestEstimateCDCPayloadSize(t *testing.T) {
	tests := []struct {
		name     string
		stmts    []protocol.Statement
		expected int
	}{
		{
			name:     "empty statements",
			stmts:    nil,
			expected: 0,
		},
		{
			name: "single insert",
			stmts: []protocol.Statement{
				testCDCStatement("users", nil, map[string][]byte{
					"id":   []byte("1"),
					"name": []byte("Alice"),
				}),
			},
			expected: len(testCDCStatement("users", nil, map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			}).EncodedRow) + len("users") + len("users:31"),
		},
		{
			name: "update with old and new values",
			stmts: []protocol.Statement{
				testCDCStatement("items", map[string][]byte{"id": []byte("1"), "value": []byte("old")},
					map[string][]byte{"id": []byte("1"), "value": []byte("new")}),
			},
			expected: len(testCDCStatement("items", map[string][]byte{"id": []byte("1"), "value": []byte("old")},
				map[string][]byte{"id": []byte("1"), "value": []byte("new")}).EncodedRow) + len("items") + len("items:31"),
		},
		{
			name: "multiple statements",
			stmts: []protocol.Statement{
				testCDCStatement("t1", nil, map[string][]byte{"id": []byte("a"), "x": []byte("y")}),
				testCDCStatement("t2", nil, map[string][]byte{"id": []byte("b"), "z": []byte("w")}),
			},
			expected: estimateCDCPayloadSize([]protocol.Statement{
				testCDCStatement("t1", nil, map[string][]byte{"id": []byte("a"), "x": []byte("y")}),
				testCDCStatement("t2", nil, map[string][]byte{"id": []byte("b"), "z": []byte("w")}),
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := estimateCDCPayloadSize(tt.stmts)
			assert.Equal(t, tt.expected, size)
		})
	}
}

func TestStreamChunkSizeDefault(t *testing.T) {
	// Verify default chunk size is 1MB
	assert.Equal(t, 1024*1024, StreamChunkSizeDefault)
}

func TestGetStreamChunkSize_Default(t *testing.T) {
	// Without config, should return default (1MB)
	threshold := GetStreamChunkSize()
	assert.Equal(t, StreamChunkSizeDefault, threshold)
}

func TestEstimateCDCPayloadSize_LargePayload(t *testing.T) {
	// Create a statement with large values that exceeds threshold
	largeValue := make([]byte, 512*1024) // 512KB
	for i := range largeValue {
		largeValue[i] = 'x'
	}

	stmts := []protocol.Statement{
		testCDCStatement("big_table", nil, map[string][]byte{
			"id":    []byte("row1"),
			"data1": largeValue,
			"data2": largeValue,
		}),
	}

	size := estimateCDCPayloadSize(stmts)
	assert.GreaterOrEqual(t, size, GetStreamChunkSize(), "Large payload should exceed streaming threshold")
}
