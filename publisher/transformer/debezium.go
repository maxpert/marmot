// Package transformer provides implementations of the publisher.Transformer interface
// for converting CDC events to various sink-specific formats.
package transformer

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/publisher"
	"github.com/rs/zerolog/log"
)

func init() {
	// Register debezium transformer factory
	publisher.RegisterTransformer("debezium", func() publisher.Transformer {
		return NewDebeziumTransformer()
	})
}

// DebeziumTransformer transforms CDC events to Debezium JSON with Schema format.
// It implements the Debezium message format with both schema and payload sections,
// compatible with Debezium consumers like Kafka Connect and stream processing systems.
//
// The transformer:
//   - Generates Debezium-compatible JSON messages with embedded schema
//   - Caches table schemas to avoid rebuilding for each event
//   - Maps SQLite types to Debezium types (INTEGER->int64, TEXT->string, etc.)
//   - Supports INSERT ("c"), UPDATE ("u"), and DELETE ("d") operations
//   - Handles nullable columns and null values correctly
//   - Decodes msgpack-encoded column values from CDC events
//
// Output format includes:
//   - schema: Structured schema definition with column types
//   - payload: Event data with before/after states, operation type, timestamp, and source metadata
type DebeziumTransformer struct {
	connectorName string
	schemaCache   sync.Map // cache built schemas: "db.table" -> *debeziumEnvelopeSchema
}

// NewDebeziumTransformer creates a new Debezium transformer
func NewDebeziumTransformer() *DebeziumTransformer {
	return &DebeziumTransformer{
		connectorName: "marmot",
	}
}

// debeziumEnvelopeSchema represents the cached schema structure
type debeziumEnvelopeSchema struct {
	Type   string                `json:"type"`
	Name   string                `json:"name"`
	Fields []debeziumSchemaField `json:"fields"`
}

type debeziumSchemaField struct {
	Field    string                `json:"field"`
	Type     interface{}           `json:"type"` // string or nested struct
	Optional bool                  `json:"optional,omitempty"`
	Name     string                `json:"name,omitempty"`
	Fields   []debeziumSchemaField `json:"fields,omitempty"`
}

type debeziumMessage struct {
	Schema  *debeziumEnvelopeSchema `json:"schema"`
	Payload debeziumPayload         `json:"payload"`
}

type debeziumPayload struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	Op     string                 `json:"op"`
	TsMs   int64                  `json:"ts_ms"`
	Source debeziumSource         `json:"source"`
}

type debeziumSource struct {
	Connector string `json:"connector"`
	Db        string `json:"db"`
	Table     string `json:"table"`
	TxID      uint64 `json:"txId"`
	LSN       uint64 `json:"lsn"`
}

// Transform converts a CDC event to Debezium JSON with Schema format
func (d *DebeziumTransformer) Transform(event publisher.CDCEvent, schema publisher.TableSchema) ([]byte, error) {
	// Get or build envelope schema
	envelopeSchema, err := d.getOrBuildSchema(event.Database, event.Table, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build schema: %w", err)
	}

	// Decode before/after values from msgpack
	var before, after map[string]interface{}
	var decodeErr error

	if event.Before != nil {
		before, decodeErr = d.decodeRowData(event.Before)
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode before data: %w", decodeErr)
		}
	}

	if event.After != nil {
		after, decodeErr = d.decodeRowData(event.After)
		if decodeErr != nil {
			return nil, fmt.Errorf("failed to decode after data: %w", decodeErr)
		}
	}

	// Build payload
	payload := debeziumPayload{
		Before: before,
		After:  after,
		Op:     d.mapOperation(event.Operation),
		TsMs:   event.CommitTS,
		Source: debeziumSource{
			Connector: d.connectorName,
			Db:        event.Database,
			Table:     event.Table,
			TxID:      event.TxnID,
			LSN:       event.SeqNum,
		},
	}

	// Build full message
	message := debeziumMessage{
		Schema:  envelopeSchema,
		Payload: payload,
	}

	// Encode as JSON
	data, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return data, nil
}

// Tombstone creates a tombstone marker (null value for Kafka log compaction)
func (d *DebeziumTransformer) Tombstone(key string) []byte {
	return nil
}

// decodeRowData decodes msgpack-encoded column values
func (d *DebeziumTransformer) decodeRowData(data map[string][]byte) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(data))
	for colName, msgpackData := range data {
		var val interface{}
		if err := encoding.Unmarshal(msgpackData, &val); err != nil {
			return nil, fmt.Errorf("failed to decode column %s: %w", colName, err)
		}
		result[colName] = val
	}
	return result, nil
}

// mapOperation maps Marmot operation to Debezium operation
func (d *DebeziumTransformer) mapOperation(op uint8) string {
	switch op {
	case publisher.OpInsert:
		return "c" // create
	case publisher.OpUpdate:
		return "u" // update
	case publisher.OpDelete:
		return "d" // delete
	default:
		log.Warn().Uint8("operation", op).Msg("unknown CDC operation, defaulting to update")
		return "u" // default to update
	}
}

// getOrBuildSchema retrieves or builds the envelope schema for a table
func (d *DebeziumTransformer) getOrBuildSchema(database, table string, schema publisher.TableSchema) (*debeziumEnvelopeSchema, error) {
	key := database + "." + table

	// Check cache
	if cached, ok := d.schemaCache.Load(key); ok {
		return cached.(*debeziumEnvelopeSchema), nil
	}

	// Build schema
	envelopeSchema := d.buildEnvelopeSchema(database, table, schema)

	// Store in cache
	d.schemaCache.Store(key, envelopeSchema)

	return envelopeSchema, nil
}

// buildEnvelopeSchema constructs the Debezium envelope schema
func (d *DebeziumTransformer) buildEnvelopeSchema(database, table string, schema publisher.TableSchema) *debeziumEnvelopeSchema {
	valueSchemaName := database + "." + table + ".Value"
	envelopeName := database + "." + table + ".Envelope"

	// Build column fields
	columnFields := make([]debeziumSchemaField, len(schema.Columns))
	for i, col := range schema.Columns {
		columnFields[i] = debeziumSchemaField{
			Field:    col.Name,
			Type:     d.mapSQLiteType(col.Type),
			Optional: col.Nullable,
		}
	}

	// Build envelope schema
	return &debeziumEnvelopeSchema{
		Type: "struct",
		Name: envelopeName,
		Fields: []debeziumSchemaField{
			{
				Field:    "before",
				Type:     "struct",
				Optional: true,
				Name:     valueSchemaName,
				Fields:   columnFields,
			},
			{
				Field:    "after",
				Type:     "struct",
				Optional: true,
				Name:     valueSchemaName,
				Fields:   columnFields,
			},
			{
				Field: "op",
				Type:  "string",
			},
			{
				Field: "ts_ms",
				Type:  "int64",
			},
			{
				Field: "source",
				Type:  "struct",
				Name:  "io.marmot.Source",
				Fields: []debeziumSchemaField{
					{Field: "connector", Type: "string"},
					{Field: "db", Type: "string"},
					{Field: "table", Type: "string"},
					{Field: "txId", Type: "int64"},
					{Field: "lsn", Type: "int64"},
				},
			},
		},
	}
}

// mapSQLiteType maps SQLite types to Debezium types using SQLite type affinity rules.
// SQLite uses flexible type names, so we check for substrings rather than exact matches.
// See: https://www.sqlite.org/datatype3.html
func (d *DebeziumTransformer) mapSQLiteType(sqliteType string) string {
	upperType := strings.ToUpper(sqliteType)

	// Check exact matches first (common cases)
	switch upperType {
	case "INTEGER", "INT":
		return "int64"
	case "TEXT":
		return "string"
	case "REAL":
		return "double"
	case "BLOB":
		return "bytes"
	case "NULL":
		return "string"
	case "BOOLEAN", "BOOL":
		return "boolean"
	case "NUMERIC":
		return "double"
	}

	// Apply SQLite type affinity rules (substring matching)
	if strings.Contains(upperType, "INT") {
		return "int64"
	}
	if strings.Contains(upperType, "CHAR") || strings.Contains(upperType, "TEXT") || strings.Contains(upperType, "CLOB") {
		return "string"
	}
	if strings.Contains(upperType, "BLOB") {
		return "bytes"
	}
	if strings.Contains(upperType, "REAL") || strings.Contains(upperType, "FLOA") || strings.Contains(upperType, "DOUB") {
		return "double"
	}

	// Default to string for unknown types (matches SQLite's NUMERIC affinity default)
	return "string"
}
