package publisher_test

import (
	"fmt"
	"log"
	"os"

	"github.com/maxpert/marmot/publisher"
)

func ExamplePublishLog() {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "publisher-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a new PublishLog
	pubLog, err := publisher.NewPublishLog(tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer pubLog.Close()

	// Create CDC events
	events := []publisher.CDCEvent{
		{
			TxnID:     100,
			Database:  "mydb",
			Table:     "users",
			Operation: publisher.OpInsert,
			IntentKey: []byte("user:1"),
			After: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			},
			CommitTS: 1000,
			NodeID:   1,
		},
		{
			TxnID:     101,
			Database:  "mydb",
			Table:     "users",
			Operation: publisher.OpUpdate,
			IntentKey: []byte("user:1"),
			Before: map[string][]byte{
				"name": []byte("Alice"),
			},
			After: map[string][]byte{
				"name": []byte("Alice Smith"),
			},
			CommitTS: 2000,
			NodeID:   1,
		},
	}

	// Append events to log
	if err := pubLog.Append(events); err != nil {
		log.Fatal(err)
	}

	// Read events for a sink
	sinkName := "kafka-sink"
	cursor, _ := pubLog.GetCursor(sinkName)
	readEvents, _ := pubLog.ReadFrom(cursor, 10)

	fmt.Printf("Read %d events\n", len(readEvents))
	fmt.Printf("First event: TxnID=%d, Table=%s, Operation=%d\n",
		readEvents[0].TxnID, readEvents[0].Table, readEvents[0].Operation)

	// Advance cursor after successful publish
	if len(readEvents) > 0 {
		lastSeq := readEvents[len(readEvents)-1].SeqNum
		pubLog.AdvanceCursor(sinkName, lastSeq)
	}

	// Output:
	// Read 2 events
	// First event: TxnID=100, Table=users, Operation=0
}

func ExampleGlobFilter() {
	// Create a filter for specific tables and databases
	filter, err := publisher.NewGlobFilter(
		[]string{"users", "orders*"},     // table patterns
		[]string{"production", "prod_*"}, // database patterns
	)
	if err != nil {
		log.Fatal(err)
	}

	// Test matching
	matches := []struct {
		db    string
		table string
	}{
		{"production", "users"},
		{"prod_us", "orders"},
		{"staging", "users"},   // won't match (database doesn't match)
		{"production", "logs"}, // won't match (table doesn't match)
	}

	for _, m := range matches {
		if filter.Match(m.db, m.table) {
			fmt.Printf("MATCH: %s.%s\n", m.db, m.table)
		}
	}

	// Output:
	// MATCH: production.users
	// MATCH: prod_us.orders
}

func ExampleCDCEvent_operations() {
	// INSERT operation
	insertEvent := publisher.CDCEvent{
		SeqNum:    1,
		TxnID:     100,
		Database:  "db",
		Table:     "users",
		Operation: publisher.OpInsert,
		IntentKey: []byte("1"),
		Before:    nil, // INSERT has no before values
		After: map[string][]byte{
			"id":   []byte("1"),
			"name": []byte("Alice"),
		},
		CommitTS: 1000,
		NodeID:   1,
	}
	fmt.Printf("INSERT: Operation=%d, HasBefore=%v, HasAfter=%v\n",
		insertEvent.Operation, insertEvent.Before != nil, insertEvent.After != nil)

	// UPDATE operation
	updateEvent := publisher.CDCEvent{
		SeqNum:    2,
		TxnID:     101,
		Database:  "db",
		Table:     "users",
		Operation: publisher.OpUpdate,
		IntentKey: []byte("1"),
		Before: map[string][]byte{
			"name": []byte("Alice"),
		},
		After: map[string][]byte{
			"name": []byte("Alice Smith"),
		},
		CommitTS: 2000,
		NodeID:   1,
	}
	fmt.Printf("UPDATE: Operation=%d, HasBefore=%v, HasAfter=%v\n",
		updateEvent.Operation, updateEvent.Before != nil, updateEvent.After != nil)

	// DELETE operation
	deleteEvent := publisher.CDCEvent{
		SeqNum:    3,
		TxnID:     102,
		Database:  "db",
		Table:     "users",
		Operation: publisher.OpDelete,
		IntentKey: []byte("1"),
		Before: map[string][]byte{
			"id":   []byte("1"),
			"name": []byte("Alice Smith"),
		},
		After:    nil, // DELETE has no after values
		CommitTS: 3000,
		NodeID:   1,
	}
	fmt.Printf("DELETE: Operation=%d, HasBefore=%v, HasAfter=%v\n",
		deleteEvent.Operation, deleteEvent.Before != nil, deleteEvent.After != nil)

	// Output:
	// INSERT: Operation=0, HasBefore=false, HasAfter=true
	// UPDATE: Operation=1, HasBefore=true, HasAfter=true
	// DELETE: Operation=2, HasBefore=true, HasAfter=false
}
