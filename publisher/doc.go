// Package publisher provides the CDC (Change Data Capture) Publishing System
// foundation for Marmot v2.9.0-beta.
//
// This package implements a durable, ordered event log backed by Pebble that
// captures CDC events and tracks per-sink consumption cursors for reliable
// event delivery to external systems (Kafka, NATS, HTTP, etc).
//
// # Architecture
//
// The publisher package consists of three main components:
//
// 1. PublishLog: Pebble-backed append-only log with cursor tracking
// 2. Filters: Glob-based table/database filtering for selective CDC
// 3. Interfaces: Sink, Transformer, and Filter abstractions
//
// # PublishLog
//
// PublishLog stores CDC events in a Pebble database with monotonically
// increasing sequence numbers. Each sink tracks its consumption progress
// via cursors, enabling:
//
//   - Crash recovery (cursors persisted to Pebble)
//   - Multiple independent sinks consuming at different rates
//   - Automatic cleanup of consumed events
//
// Key prefixes:
//
//	/publog/{seq:016x}       -> msgpack(CDCEvent)
//	/pubcursor/{sinkName}    -> uint64 (cursor)
//	/pubseq                  -> uint64 (next sequence)
//
// Example usage:
//
//	log, err := NewPublishLog("/data/marmot")
//	if err != nil {
//		return err
//	}
//	defer log.Close()
//
//	// Append CDC events
//	events := []CDCEvent{
//		{TxnID: 1, Database: "db", Table: "users", Operation: OpInsert, ...},
//	}
//	if err := log.Append(events); err != nil {
//		return err
//	}
//
//	// Read from cursor
//	cursor, _ := log.GetCursor("kafka-sink")
//	events, _ = log.ReadFrom(cursor, 100)
//
//	// Advance cursor after successful publish
//	log.AdvanceCursor("kafka-sink", events[len(events)-1].SeqNum)
//
// # Filters
//
// GlobFilter enables selective CDC replication based on table/database patterns:
//
//	filter, err := NewGlobFilter(
//		[]string{"users", "orders*"},  // table patterns
//		[]string{"prod_*"},             // database patterns
//	)
//
//	if filter.Match("prod_us", "users") {
//		// Publish event
//	}
//
// # Thread Safety
//
// All operations are safe for concurrent use:
//
//   - PublishLog uses atomic operations for sequence numbers
//   - Cursor map protected by RWMutex
//   - Pebble handles concurrent reads/writes internally
//
// # Cleanup
//
// PublishLog automatically triggers cleanup every 128 sequence numbers,
// deleting all events below the minimum cursor across all sinks. This
// prevents unbounded log growth while ensuring no sink loses data.
package publisher
