package transformer

import (
	"encoding/json"
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebeziumTransformer_Transform_Insert(t *testing.T) {
	transformer := NewDebeziumTransformer()

	// Create schema
	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: false, IsPK: false},
			{Name: "age", Type: "INTEGER", Nullable: true, IsPK: false},
		},
	}

	// Create INSERT event
	afterData := map[string][]byte{
		"id":   mustMarshalMsgpack(int64(1)),
		"name": mustMarshalMsgpack("Alice"),
		"age":  mustMarshalMsgpack(int64(30)),
	}

	event := publisher.CDCEvent{
		SeqNum:    100,
		TxnID:     12345,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		Before:    nil,
		After:     afterData,
		CommitTS:  1702345678901,
		NodeID:    1,
	}

	// Transform
	data, err := transformer.Transform(event, schema)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Parse result
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify schema
	schemaMap := result["schema"].(map[string]interface{})
	assert.Equal(t, "struct", schemaMap["type"])
	assert.Equal(t, "mydb.users.Envelope", schemaMap["name"])

	fields := schemaMap["fields"].([]interface{})
	assert.Len(t, fields, 5) // before, after, op, ts_ms, source

	// Verify payload
	payload := result["payload"].(map[string]interface{})
	assert.Nil(t, payload["before"])
	assert.NotNil(t, payload["after"])
	assert.Equal(t, "c", payload["op"])
	assert.Equal(t, float64(1702345678901), payload["ts_ms"])

	// Verify after data
	after := payload["after"].(map[string]interface{})
	assert.Equal(t, float64(1), after["id"])
	assert.Equal(t, "Alice", after["name"])
	assert.Equal(t, float64(30), after["age"])

	// Verify source
	source := payload["source"].(map[string]interface{})
	assert.Equal(t, "marmot", source["connector"])
	assert.Equal(t, "mydb", source["db"])
	assert.Equal(t, "users", source["table"])
	assert.Equal(t, float64(12345), source["txId"])
	assert.Equal(t, float64(100), source["lsn"])
}

func TestDebeziumTransformer_Transform_Update(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: false, IsPK: false},
			{Name: "age", Type: "INTEGER", Nullable: true, IsPK: false},
		},
	}

	// Create UPDATE event
	beforeData := map[string][]byte{
		"id":   mustMarshalMsgpack(int64(1)),
		"name": mustMarshalMsgpack("Alice"),
		"age":  mustMarshalMsgpack(int64(30)),
	}

	afterData := map[string][]byte{
		"id":   mustMarshalMsgpack(int64(1)),
		"name": mustMarshalMsgpack("Alice Updated"),
		"age":  mustMarshalMsgpack(int64(31)),
	}

	event := publisher.CDCEvent{
		SeqNum:    101,
		TxnID:     12346,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpUpdate,
		IntentKey: "1",
		Before:    beforeData,
		After:     afterData,
		CommitTS:  1702345678902,
		NodeID:    1,
	}

	// Transform
	data, err := transformer.Transform(event, schema)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Parse result
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify payload
	payload := result["payload"].(map[string]interface{})
	assert.NotNil(t, payload["before"])
	assert.NotNil(t, payload["after"])
	assert.Equal(t, "u", payload["op"])

	// Verify before data
	before := payload["before"].(map[string]interface{})
	assert.Equal(t, float64(1), before["id"])
	assert.Equal(t, "Alice", before["name"])
	assert.Equal(t, float64(30), before["age"])

	// Verify after data
	after := payload["after"].(map[string]interface{})
	assert.Equal(t, float64(1), after["id"])
	assert.Equal(t, "Alice Updated", after["name"])
	assert.Equal(t, float64(31), after["age"])
}

func TestDebeziumTransformer_Transform_Delete(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: false, IsPK: false},
		},
	}

	// Create DELETE event
	beforeData := map[string][]byte{
		"id":   mustMarshalMsgpack(int64(1)),
		"name": mustMarshalMsgpack("Alice"),
	}

	event := publisher.CDCEvent{
		SeqNum:    102,
		TxnID:     12347,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpDelete,
		IntentKey: "1",
		Before:    beforeData,
		After:     nil,
		CommitTS:  1702345678903,
		NodeID:    1,
	}

	// Transform
	data, err := transformer.Transform(event, schema)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Parse result
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify payload
	payload := result["payload"].(map[string]interface{})
	assert.NotNil(t, payload["before"])
	assert.Nil(t, payload["after"])
	assert.Equal(t, "d", payload["op"])

	// Verify before data
	before := payload["before"].(map[string]interface{})
	assert.Equal(t, float64(1), before["id"])
	assert.Equal(t, "Alice", before["name"])
}

func TestDebeziumTransformer_AllDataTypes(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "int_col", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "text_col", Type: "TEXT", Nullable: false, IsPK: false},
			{Name: "real_col", Type: "REAL", Nullable: false, IsPK: false},
			{Name: "blob_col", Type: "BLOB", Nullable: true, IsPK: false},
			{Name: "null_col", Type: "NULL", Nullable: true, IsPK: false},
		},
	}

	// Create event with all data types
	afterData := map[string][]byte{
		"int_col":  mustMarshalMsgpack(int64(42)),
		"text_col": mustMarshalMsgpack("hello"),
		"real_col": mustMarshalMsgpack(3.14),
		"blob_col": mustMarshalMsgpack([]byte{0x01, 0x02, 0x03}),
		"null_col": mustMarshalMsgpack(nil),
	}

	event := publisher.CDCEvent{
		SeqNum:    103,
		TxnID:     12348,
		Database:  "testdb",
		Table:     "types_table",
		Operation: publisher.OpInsert,
		IntentKey: "42",
		Before:    nil,
		After:     afterData,
		CommitTS:  1702345678904,
		NodeID:    1,
	}

	// Transform
	data, err := transformer.Transform(event, schema)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Parse result
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify schema field types
	schemaMap := result["schema"].(map[string]interface{})
	fields := schemaMap["fields"].([]interface{})
	afterField := fields[1].(map[string]interface{})
	afterFields := afterField["fields"].([]interface{})

	// Check type mapping
	intField := afterFields[0].(map[string]interface{})
	assert.Equal(t, "int64", intField["type"])

	textField := afterFields[1].(map[string]interface{})
	assert.Equal(t, "string", textField["type"])

	realField := afterFields[2].(map[string]interface{})
	assert.Equal(t, "double", realField["type"])

	blobField := afterFields[3].(map[string]interface{})
	assert.Equal(t, "bytes", blobField["type"])

	nullField := afterFields[4].(map[string]interface{})
	assert.Equal(t, "string", nullField["type"]) // NULL maps to string
}

func TestDebeziumTransformer_SchemaCache(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
		},
	}

	event := publisher.CDCEvent{
		SeqNum:    104,
		TxnID:     12349,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		Before:    nil,
		After: map[string][]byte{
			"id": mustMarshalMsgpack(int64(1)),
		},
		CommitTS: 1702345678905,
		NodeID:   1,
	}

	// First transform - builds schema
	data1, err := transformer.Transform(event, schema)
	require.NoError(t, err)

	// Second transform - should use cached schema
	event.SeqNum = 105
	event.TxnID = 12350
	data2, err := transformer.Transform(event, schema)
	require.NoError(t, err)

	// Both should have identical schema structure
	var result1, result2 map[string]interface{}
	err = json.Unmarshal(data1, &result1)
	require.NoError(t, err)
	err = json.Unmarshal(data2, &result2)
	require.NoError(t, err)

	schema1 := result1["schema"].(map[string]interface{})
	schema2 := result2["schema"].(map[string]interface{})

	assert.Equal(t, schema1["name"], schema2["name"])
	assert.Equal(t, schema1["type"], schema2["type"])
}

func TestDebeziumTransformer_Tombstone(t *testing.T) {
	transformer := NewDebeziumTransformer()

	result := transformer.Tombstone("any-key")
	assert.Nil(t, result)
}

func TestDebeziumTransformer_NullableColumns(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "optional_name", Type: "TEXT", Nullable: true, IsPK: false},
		},
	}

	// Create event with null value
	afterData := map[string][]byte{
		"id":            mustMarshalMsgpack(int64(1)),
		"optional_name": mustMarshalMsgpack(nil),
	}

	event := publisher.CDCEvent{
		SeqNum:    106,
		TxnID:     12351,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		Before:    nil,
		After:     afterData,
		CommitTS:  1702345678906,
		NodeID:    1,
	}

	// Transform
	data, err := transformer.Transform(event, schema)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Parse result
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify schema marks optional_name as optional
	schemaMap := result["schema"].(map[string]interface{})
	fields := schemaMap["fields"].([]interface{})
	afterField := fields[1].(map[string]interface{})
	afterFields := afterField["fields"].([]interface{})

	optionalField := afterFields[1].(map[string]interface{})
	assert.Equal(t, "optional_name", optionalField["field"])
	assert.True(t, optionalField["optional"].(bool))

	// Verify null value in payload
	payload := result["payload"].(map[string]interface{})
	after := payload["after"].(map[string]interface{})
	assert.Nil(t, after["optional_name"])
}

func TestDebeziumTransformer_InvalidMsgpack(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
		},
	}

	// Create event with truncated/invalid msgpack data
	// 0xd9 is a str8 marker that expects length byte + string data, but we provide incomplete data
	afterData := map[string][]byte{
		"id": {0xd9}, // incomplete msgpack str8 - missing length and data
	}

	event := publisher.CDCEvent{
		SeqNum:    107,
		TxnID:     12352,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		Before:    nil,
		After:     afterData,
		CommitTS:  1702345678907,
		NodeID:    1,
	}

	// Transform should fail
	_, err := transformer.Transform(event, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode")
}

func TestDebeziumTransformer_EmptySchema(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{},
	}

	event := publisher.CDCEvent{
		SeqNum:    108,
		TxnID:     12353,
		Database:  "mydb",
		Table:     "empty",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		Before:    nil,
		After:     map[string][]byte{},
		CommitTS:  1702345678908,
		NodeID:    1,
	}

	// Transform should work with empty schema
	data, err := transformer.Transform(event, schema)
	require.NoError(t, err)
	require.NotNil(t, data)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify schema has no column fields
	schemaMap := result["schema"].(map[string]interface{})
	fields := schemaMap["fields"].([]interface{})
	afterField := fields[1].(map[string]interface{})
	afterFields, ok := afterField["fields"].([]interface{})
	if ok {
		assert.Len(t, afterFields, 0)
	} else {
		// fields may not be present for empty schema
		assert.Empty(t, afterField["fields"])
	}
}

func TestDebeziumTransformer_MultipleTablesSchema(t *testing.T) {
	transformer := NewDebeziumTransformer()

	schema1 := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
		},
	}

	schema2 := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "product_id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: false, IsPK: false},
		},
	}

	event1 := publisher.CDCEvent{
		SeqNum:    109,
		TxnID:     12354,
		Database:  "mydb",
		Table:     "users",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		After: map[string][]byte{
			"id": mustMarshalMsgpack(int64(1)),
		},
		CommitTS: 1702345678909,
		NodeID:   1,
	}

	event2 := publisher.CDCEvent{
		SeqNum:    110,
		TxnID:     12355,
		Database:  "mydb",
		Table:     "products",
		Operation: publisher.OpInsert,
		IntentKey: "1",
		After: map[string][]byte{
			"product_id": mustMarshalMsgpack(int64(1)),
			"name":       mustMarshalMsgpack("Widget"),
		},
		CommitTS: 1702345678910,
		NodeID:   1,
	}

	// Transform both events
	data1, err := transformer.Transform(event1, schema1)
	require.NoError(t, err)

	data2, err := transformer.Transform(event2, schema2)
	require.NoError(t, err)

	// Parse results
	var result1, result2 map[string]interface{}
	err = json.Unmarshal(data1, &result1)
	require.NoError(t, err)
	err = json.Unmarshal(data2, &result2)
	require.NoError(t, err)

	// Verify different schema names
	schema1Map := result1["schema"].(map[string]interface{})
	schema2Map := result2["schema"].(map[string]interface{})
	assert.Equal(t, "mydb.users.Envelope", schema1Map["name"])
	assert.Equal(t, "mydb.products.Envelope", schema2Map["name"])
}

// Helper function to marshal data with msgpack
func mustMarshalMsgpack(v interface{}) []byte {
	data, err := encoding.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
