package publisher

// Operation types for CDC events
const (
	OpInsert uint8 = 0
	OpUpdate uint8 = 1
	OpDelete uint8 = 2
)

// CDCEvent represents a single CDC event to publish
type CDCEvent struct {
	SeqNum    uint64            `msgpack:"seq"`    // Monotonic sequence
	TxnID     uint64            `msgpack:"txn"`    // Marmot transaction ID
	Database  string            `msgpack:"db"`     // Database name
	Table     string            `msgpack:"tbl"`    // Table name
	Operation uint8             `msgpack:"op"`     // 0=INSERT, 1=UPDATE, 2=DELETE
	IntentKey string            `msgpack:"key"`    // Intent tracking key
	Before    map[string][]byte `msgpack:"before"` // Old values (msgpack encoded)
	After     map[string][]byte `msgpack:"after"`  // New values (msgpack encoded)
	CommitTS  int64             `msgpack:"ts"`     // Commit timestamp (unix ms)
	NodeID    uint64            `msgpack:"node"`   // Originating node
}

// Sink represents a destination for CDC events (e.g., Kafka, NATS, HTTP)
type Sink interface {
	// Publish sends an event to the sink
	Publish(topic string, key string, value []byte) error
	// Close releases any resources held by the sink
	Close() error
}

// Transformer converts CDC events to sink-specific formats
type Transformer interface {
	// Transform converts a CDC event to bytes for publishing
	Transform(event CDCEvent, schema TableSchema) ([]byte, error)
	// Tombstone creates a tombstone/delete marker for the given key
	Tombstone(key string) []byte
}

// Filter determines whether a CDC event should be published
type Filter interface {
	// Match returns true if the event should be published
	Match(database, table string) bool
}

// TableSchema holds column metadata for a table
type TableSchema struct {
	Columns []ColumnInfo
}

// ColumnInfo represents metadata for a single column
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	IsPK     bool
}
