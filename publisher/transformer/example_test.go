package transformer_test

import (
	"encoding/json"
	"fmt"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/publisher"
	"github.com/maxpert/marmot/publisher/transformer"
)

func ExampleDebeziumTransformer() {
	// Create a new Debezium transformer
	t := transformer.NewDebeziumTransformer()

	// Define table schema
	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: false, IsPK: false},
			{Name: "email", Type: "TEXT", Nullable: true, IsPK: false},
		},
	}

	// Create a CDC INSERT event
	afterData := map[string][]byte{
		"id":    mustMarshal(int64(1)),
		"name":  mustMarshal("Alice"),
		"email": mustMarshal("alice@example.com"),
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

	// Transform to Debezium JSON
	data, err := t.Transform(event, schema)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Parse the result to verify structure
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		return
	}

	// Print the operation and source
	payload := result["payload"].(map[string]interface{})
	fmt.Printf("Operation: %s\n", payload["op"])

	source := payload["source"].(map[string]interface{})
	fmt.Printf("Source: %s.%s\n", source["db"], source["table"])

	after := payload["after"].(map[string]interface{})
	fmt.Printf("After: name=%s\n", after["name"])

	// Output:
	// Operation: c
	// Source: mydb.users
	// After: name=Alice
}

func ExampleDebeziumTransformer_update() {
	// Create a new Debezium transformer
	t := transformer.NewDebeziumTransformer()

	// Define table schema
	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "status", Type: "TEXT", Nullable: false, IsPK: false},
		},
	}

	// Create a CDC UPDATE event
	beforeData := map[string][]byte{
		"id":     mustMarshal(int64(1)),
		"status": mustMarshal("pending"),
	}

	afterData := map[string][]byte{
		"id":     mustMarshal(int64(1)),
		"status": mustMarshal("completed"),
	}

	event := publisher.CDCEvent{
		SeqNum:    101,
		TxnID:     12346,
		Database:  "orders_db",
		Table:     "orders",
		Operation: publisher.OpUpdate,
		IntentKey: "1",
		Before:    beforeData,
		After:     afterData,
		CommitTS:  1702345678902,
		NodeID:    1,
	}

	// Transform to Debezium JSON
	data, err := t.Transform(event, schema)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Parse the result
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		return
	}

	// Print the operation and data
	payload := result["payload"].(map[string]interface{})
	fmt.Printf("Operation: %s\n", payload["op"])

	before := payload["before"].(map[string]interface{})
	after := payload["after"].(map[string]interface{})
	fmt.Printf("Status changed: %s -> %s\n", before["status"], after["status"])

	// Output:
	// Operation: u
	// Status changed: pending -> completed
}

func ExampleDebeziumTransformer_delete() {
	// Create a new Debezium transformer
	t := transformer.NewDebeziumTransformer()

	// Define table schema
	schema := publisher.TableSchema{
		Columns: []publisher.ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false, IsPK: true},
			{Name: "name", Type: "TEXT", Nullable: false, IsPK: false},
		},
	}

	// Create a CDC DELETE event
	beforeData := map[string][]byte{
		"id":   mustMarshal(int64(1)),
		"name": mustMarshal("Alice"),
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

	// Transform to Debezium JSON
	data, err := t.Transform(event, schema)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Parse the result
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		return
	}

	// Print the operation
	payload := result["payload"].(map[string]interface{})
	fmt.Printf("Operation: %s\n", payload["op"])

	before := payload["before"].(map[string]interface{})
	fmt.Printf("Deleted: name=%s\n", before["name"])

	// After should be nil for deletes
	fmt.Printf("After is nil: %v\n", payload["after"] == nil)

	// Output:
	// Operation: d
	// Deleted: name=Alice
	// After is nil: true
}

func ExampleDebeziumTransformer_Tombstone() {
	// Create a new Debezium transformer
	t := transformer.NewDebeziumTransformer()

	// Get tombstone for a key (returns nil for Kafka log compaction)
	tombstone := t.Tombstone("users:1")

	fmt.Printf("Tombstone is nil: %v\n", tombstone == nil)

	// Output:
	// Tombstone is nil: true
}

// Helper function to marshal data with msgpack
func mustMarshal(v interface{}) []byte {
	data, err := encoding.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
