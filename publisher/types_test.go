package publisher

import (
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCDCEventMsgpackSerialization(t *testing.T) {
	event := CDCEvent{
		SeqNum:    12345,
		TxnID:     67890,
		Database:  "test_db",
		Table:     "users",
		Operation: OpInsert,
		IntentKey: "user:123",
		Before:    nil,
		After: map[string][]byte{
			"id":   []byte("123"),
			"name": []byte("Alice"),
		},
		CommitTS: 1234567890,
		NodeID:   1,
	}

	// Marshal
	data, err := encoding.Marshal(&event)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal
	var decoded CDCEvent
	err = encoding.Unmarshal(data, &decoded)
	require.NoError(t, err)

	// Verify all fields
	assert.Equal(t, event.SeqNum, decoded.SeqNum)
	assert.Equal(t, event.TxnID, decoded.TxnID)
	assert.Equal(t, event.Database, decoded.Database)
	assert.Equal(t, event.Table, decoded.Table)
	assert.Equal(t, event.Operation, decoded.Operation)
	assert.Equal(t, event.IntentKey, decoded.IntentKey)
	assert.Equal(t, event.CommitTS, decoded.CommitTS)
	assert.Equal(t, event.NodeID, decoded.NodeID)
	assert.Nil(t, decoded.Before)
	assert.NotNil(t, decoded.After)
	assert.Equal(t, event.After["id"], decoded.After["id"])
	assert.Equal(t, event.After["name"], decoded.After["name"])
}

func TestCDCEventOperationConstants(t *testing.T) {
	// Verify operation constants have expected values
	assert.Equal(t, uint8(0), OpInsert)
	assert.Equal(t, uint8(1), OpUpdate)
	assert.Equal(t, uint8(2), OpDelete)
}

func TestCDCEventUpdateOperation(t *testing.T) {
	event := CDCEvent{
		SeqNum:    1,
		TxnID:     2,
		Database:  "db",
		Table:     "tbl",
		Operation: OpUpdate,
		IntentKey: "key:1",
		Before: map[string][]byte{
			"status": []byte("active"),
		},
		After: map[string][]byte{
			"status": []byte("inactive"),
		},
		CommitTS: 1000,
		NodeID:   1,
	}

	data, err := encoding.Marshal(&event)
	require.NoError(t, err)

	var decoded CDCEvent
	err = encoding.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, OpUpdate, decoded.Operation)
	assert.NotNil(t, decoded.Before)
	assert.NotNil(t, decoded.After)
}

func TestCDCEventDeleteOperation(t *testing.T) {
	event := CDCEvent{
		SeqNum:    1,
		TxnID:     2,
		Database:  "db",
		Table:     "tbl",
		Operation: OpDelete,
		IntentKey: "key:1",
		Before: map[string][]byte{
			"id": []byte("1"),
		},
		After:    nil, // Delete has no after values
		CommitTS: 1000,
		NodeID:   1,
	}

	data, err := encoding.Marshal(&event)
	require.NoError(t, err)

	var decoded CDCEvent
	err = encoding.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, OpDelete, decoded.Operation)
	assert.NotNil(t, decoded.Before)
	assert.Nil(t, decoded.After)
}

func TestTableSchemaCreation(t *testing.T) {
	schema := TableSchema{
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: true, IsPK: false},
			{Name: "email", Type: "TEXT", Nullable: false, IsPK: false},
		},
	}

	// Verify schema structure
	assert.Len(t, schema.Columns, 3)
	assert.Equal(t, "id", schema.Columns[0].Name)
	assert.True(t, schema.Columns[0].IsPK)
	assert.False(t, schema.Columns[1].IsPK)
	assert.True(t, schema.Columns[1].Nullable)
}

func TestCDCEventEmptyMaps(t *testing.T) {
	// Test with empty Before/After maps (not nil)
	event := CDCEvent{
		SeqNum:    1,
		TxnID:     2,
		Database:  "db",
		Table:     "tbl",
		Operation: OpInsert,
		IntentKey: "key",
		Before:    make(map[string][]byte),
		After:     make(map[string][]byte),
		CommitTS:  1000,
		NodeID:    1,
	}

	data, err := encoding.Marshal(&event)
	require.NoError(t, err)

	var decoded CDCEvent
	err = encoding.Unmarshal(data, &decoded)
	require.NoError(t, err)

	// Empty maps should be preserved
	assert.NotNil(t, decoded.Before)
	assert.NotNil(t, decoded.After)
	assert.Len(t, decoded.Before, 0)
	assert.Len(t, decoded.After, 0)
}
