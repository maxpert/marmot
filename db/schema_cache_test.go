//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"testing"

	"github.com/maxpert/marmot/protocol/determinism"
)

func TestSchemaCache_BuildDeterminismSchema_WithDefaults(t *testing.T) {
	cache := NewSchemaCache()

	// Manually add a table with CreateSQL that has non-deterministic DEFAULT
	cache.Update("events", &TableSchema{
		TableName: "events",
		Columns:   []string{"id", "name", "created_at"},
		CreateSQL: `CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			name TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
	})

	schema := cache.BuildDeterminismSchema()
	if schema == nil {
		t.Fatal("BuildDeterminismSchema returned nil")
	}

	// Check that the schema was built correctly
	table, ok := schema.Tables["events"]
	if !ok {
		t.Fatal("events table not found in determinism schema")
	}

	// Check created_at has non-deterministic DEFAULT
	createdAt, ok := table.Columns["created_at"]
	if !ok {
		t.Fatal("created_at column not found")
	}
	if !createdAt.HasDefault {
		t.Error("created_at should have DEFAULT")
	}
	if createdAt.DefaultIsDeterministic {
		t.Error("created_at DEFAULT CURRENT_TIMESTAMP should be non-deterministic")
	}
}

func TestSchemaCache_BuildDeterminismSchema_WithTriggers(t *testing.T) {
	cache := NewSchemaCache()

	// Add table with trigger
	cache.Update("audit_log", &TableSchema{
		TableName: "audit_log",
		Columns:   []string{"id", "msg"},
		CreateSQL: `CREATE TABLE audit_log (id INTEGER, msg TEXT)`,
		Triggers: []string{
			`CREATE TRIGGER log_changes AFTER INSERT ON audit_log
				BEGIN INSERT INTO backup (ts) VALUES (datetime('now')); END`,
		},
	})

	schema := cache.BuildDeterminismSchema()
	if schema == nil {
		t.Fatal("BuildDeterminismSchema returned nil")
	}

	table, ok := schema.Tables["audit_log"]
	if !ok {
		t.Fatal("audit_log table not found")
	}

	// Table should be marked with trigger
	if !table.HasTrigger {
		t.Error("audit_log should have trigger marked")
	}
}

func TestSchemaCache_BuildDeterminismSchema_Empty(t *testing.T) {
	cache := NewSchemaCache()

	schema := cache.BuildDeterminismSchema()
	if schema == nil {
		t.Fatal("BuildDeterminismSchema returned nil for empty cache")
	}
	if len(schema.Tables) != 0 {
		t.Errorf("expected empty schema, got %d tables", len(schema.Tables))
	}
}

func TestSchemaCache_BuildDeterminismSchema_MultipleTables(t *testing.T) {
	cache := NewSchemaCache()

	cache.Update("users", &TableSchema{
		TableName: "users",
		Columns:   []string{"id", "name"},
		CreateSQL: `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'anonymous')`,
	})

	cache.Update("orders", &TableSchema{
		TableName: "orders",
		Columns:   []string{"id", "user_id", "created"},
		CreateSQL: `CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, created DATETIME DEFAULT CURRENT_TIMESTAMP)`,
	})

	schema := cache.BuildDeterminismSchema()
	if schema == nil {
		t.Fatal("BuildDeterminismSchema returned nil")
	}

	if len(schema.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(schema.Tables))
	}

	// Check users table - name has deterministic DEFAULT
	users, ok := schema.Tables["users"]
	if !ok {
		t.Fatal("users table not found")
	}
	nameCol := users.Columns["name"]
	if !nameCol.HasDefault || !nameCol.DefaultIsDeterministic {
		t.Error("users.name should have deterministic DEFAULT")
	}

	// Check orders table - created has non-deterministic DEFAULT
	orders, ok := schema.Tables["orders"]
	if !ok {
		t.Fatal("orders table not found")
	}
	createdCol := orders.Columns["created"]
	if !createdCol.HasDefault || createdCol.DefaultIsDeterministic {
		t.Error("orders.created should have non-deterministic DEFAULT")
	}
}

func TestSchemaCache_BuildDeterminismSchema_NoCreateSQL(t *testing.T) {
	cache := NewSchemaCache()

	// Table without CreateSQL should be skipped silently
	cache.Update("simple", &TableSchema{
		TableName: "simple",
		Columns:   []string{"id", "data"},
		CreateSQL: "", // No CREATE SQL
	})

	schema := cache.BuildDeterminismSchema()
	if schema == nil {
		t.Fatal("BuildDeterminismSchema returned nil")
	}

	// Table should not be in schema since we couldn't parse it
	if len(schema.Tables) != 0 {
		t.Errorf("expected 0 tables (no CreateSQL), got %d", len(schema.Tables))
	}
}

func TestPipeline_DeterminismWithSchema(t *testing.T) {
	// Create a determinism schema with a table that has non-det DEFAULT
	schema := determinism.NewSchema()
	schema.AddTable(`CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		name TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	// Create schema provider
	schemaProvider := func() *determinism.Schema {
		return schema
	}

	// This test would require creating a Pipeline with the schemaProvider
	// and verifying INSERT statements are marked non-deterministic when
	// missing the created_at column.
	// For now, we test the schema provider function works:
	result := schemaProvider()
	if result == nil {
		t.Fatal("schemaProvider returned nil")
	}

	// Verify the schema has the expected table
	if _, ok := result.Tables["events"]; !ok {
		t.Fatal("events table not found in schema")
	}
}
