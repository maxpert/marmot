//go:build sqlite_preupdate_hook

package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPrepareRequest_StripsCDCData(t *testing.T) {
	wc := &WriteCoordinator{
		nodeID: 1,
	}

	txn := &Transaction{
		ID:       12345,
		NodeID:   1,
		Database: "test_db",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				TableName: "users",
				Database:  "test_db",
				IntentKey: []byte("users:1"),
				OldValues: nil, // INSERT has no old values
				NewValues: map[string][]byte{
					"id":   []byte("1"),
					"name": []byte("Alice"),
				},
			},
			{
				Type:      protocol.StatementUpdate,
				TableName: "users",
				Database:  "test_db",
				IntentKey: []byte("users:2"),
				OldValues: map[string][]byte{
					"id":   []byte("2"),
					"name": []byte("Bob"),
				},
				NewValues: map[string][]byte{
					"id":   []byte("2"),
					"name": []byte("Robert"),
				},
			},
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

	// Verify CDC data (OldValues, NewValues) is stripped
	for i, stmt := range req.Statements {
		assert.Nil(t, stmt.OldValues, "Statement %d should have nil OldValues", i)
		assert.Nil(t, stmt.NewValues, "Statement %d should have nil NewValues", i)

		// Verify metadata is preserved
		assert.Equal(t, txn.Statements[i].Type, stmt.Type)
		assert.Equal(t, txn.Statements[i].TableName, stmt.TableName)
		assert.Equal(t, txn.Statements[i].Database, stmt.Database)
		assert.Equal(t, txn.Statements[i].IntentKey, stmt.IntentKey)
	}
}

func TestBuildCommitRequest_IncludesCDCData(t *testing.T) {
	wc := &WriteCoordinator{
		nodeID: 1,
	}

	txn := &Transaction{
		ID:       12345,
		NodeID:   1,
		Database: "test_db",
		Statements: []protocol.Statement{
			{
				Type:      protocol.StatementInsert,
				TableName: "users",
				Database:  "test_db",
				IntentKey: []byte("users:1"),
				NewValues: map[string][]byte{
					"id":   []byte("1"),
					"name": []byte("Alice"),
				},
			},
		},
	}

	req := wc.buildCommitRequest(txn)

	// Verify transaction metadata is preserved
	assert.Equal(t, txn.ID, req.TxnID)
	assert.Equal(t, wc.nodeID, req.NodeID)
	assert.Equal(t, txn.Database, req.Database)
	assert.Equal(t, PhaseCommit, req.Phase)

	// Verify statements include full CDC data
	require.Len(t, req.Statements, 1)
	stmt := req.Statements[0]

	assert.Equal(t, txn.Statements[0].Type, stmt.Type)
	assert.Equal(t, txn.Statements[0].TableName, stmt.TableName)
	require.NotNil(t, stmt.NewValues)
	assert.Equal(t, []byte("1"), stmt.NewValues["id"])
	assert.Equal(t, []byte("Alice"), stmt.NewValues["name"])
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
				{
					TableName: "users",      // 5 bytes
					IntentKey: []byte("k1"), // 2 bytes
					NewValues: map[string][]byte{
						"id":   []byte("1"),     // 1 byte
						"name": []byte("Alice"), // 5 bytes
					},
				},
			},
			expected: 5 + 2 + 1 + 5, // 13 bytes
		},
		{
			name: "update with old and new values",
			stmts: []protocol.Statement{
				{
					TableName: "items",           // 5 bytes
					IntentKey: []byte("items:1"), // 7 bytes
					OldValues: map[string][]byte{
						"value": []byte("old"), // 3 bytes
					},
					NewValues: map[string][]byte{
						"value": []byte("new"), // 3 bytes
					},
				},
			},
			expected: 5 + 7 + 3 + 3, // 18 bytes
		},
		{
			name: "multiple statements",
			stmts: []protocol.Statement{
				{
					TableName: "t1",        // 2 bytes
					IntentKey: []byte("a"), // 1 byte
					NewValues: map[string][]byte{"x": []byte("y")},
				},
				{
					TableName: "t2",        // 2 bytes
					IntentKey: []byte("b"), // 1 byte
					NewValues: map[string][]byte{"z": []byte("w")},
				},
			},
			expected: (2 + 1 + 1) + (2 + 1 + 1), // 8 bytes
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
		{
			TableName: "big_table",
			IntentKey: []byte("row1"),
			NewValues: map[string][]byte{
				"data1": largeValue,
				"data2": largeValue,
			},
		},
	}

	size := estimateCDCPayloadSize(stmts)
	assert.GreaterOrEqual(t, size, GetStreamChunkSize(), "Large payload should exceed streaming threshold")
}
