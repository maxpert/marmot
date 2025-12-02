package protocol

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSchemaProvider_AutoIncrementDetection(t *testing.T) {
	tests := []struct {
		name            string
		createSQL       string
		tableName       string
		expectedAutoInc string
	}{
		{
			name:            "single INTEGER PRIMARY KEY is auto-increment",
			createSQL:       "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			tableName:       "users",
			expectedAutoInc: "id",
		},
		{
			name:            "single integer PRIMARY KEY lowercase is auto-increment",
			createSQL:       "CREATE TABLE items (item_id integer PRIMARY KEY, data TEXT)",
			tableName:       "items",
			expectedAutoInc: "item_id",
		},
		{
			name:            "INT PRIMARY KEY is NOT auto-increment",
			createSQL:       "CREATE TABLE orders (id INT PRIMARY KEY, total REAL)",
			tableName:       "orders",
			expectedAutoInc: "",
		},
		{
			name:            "BIGINT PRIMARY KEY is auto-increment",
			createSQL:       "CREATE TABLE events (id BIGINT PRIMARY KEY, data TEXT)",
			tableName:       "events",
			expectedAutoInc: "id",
		},
		{
			name:            "composite PRIMARY KEY is NOT auto-increment",
			createSQL:       "CREATE TABLE kv (k1 INTEGER, k2 INTEGER, val TEXT, PRIMARY KEY(k1, k2))",
			tableName:       "kv",
			expectedAutoInc: "",
		},
		{
			name:            "TEXT PRIMARY KEY is NOT auto-increment",
			createSQL:       "CREATE TABLE names (id TEXT PRIMARY KEY, value TEXT)",
			tableName:       "names",
			expectedAutoInc: "",
		},
		{
			name:            "INTEGER with explicit AUTOINCREMENT keyword",
			createSQL:       "CREATE TABLE seq (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)",
			tableName:       "seq",
			expectedAutoInc: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create in-memory SQLite database
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("failed to open database: %v", err)
			}
			defer db.Close()

			// Create the table
			if _, err := db.Exec(tt.createSQL); err != nil {
				t.Fatalf("failed to create table: %v", err)
			}

			// Create schema provider and get schema
			sp := NewSchemaProvider(db)
			schema, err := sp.GetTableSchema(tt.tableName)
			if err != nil {
				t.Fatalf("GetTableSchema failed: %v", err)
			}

			if schema.AutoIncrementCol != tt.expectedAutoInc {
				t.Errorf("AutoIncrementCol = %q, want %q", schema.AutoIncrementCol, tt.expectedAutoInc)
			}
		})
	}
}

func TestSchemaProvider_GetAutoIncrementColumn(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables with different PK types
	if _, err := db.Exec("CREATE TABLE auto_inc (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE no_auto_inc (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	sp := NewSchemaProvider(db)

	// Test auto-increment table
	col, err := sp.GetAutoIncrementColumn("auto_inc")
	if err != nil {
		t.Fatalf("GetAutoIncrementColumn failed: %v", err)
	}
	if col != "id" {
		t.Errorf("GetAutoIncrementColumn(auto_inc) = %q, want %q", col, "id")
	}

	// Test non-auto-increment table
	col, err = sp.GetAutoIncrementColumn("no_auto_inc")
	if err != nil {
		t.Fatalf("GetAutoIncrementColumn failed: %v", err)
	}
	if col != "" {
		t.Errorf("GetAutoIncrementColumn(no_auto_inc) = %q, want empty string", col)
	}

	// Test non-existent table
	_, err = sp.GetAutoIncrementColumn("nonexistent")
	if err == nil {
		t.Error("GetAutoIncrementColumn(nonexistent) should have returned an error")
	}
}

func TestTableSchema_AutoIncrementCol_InSchemaVersion(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create two tables with same structure except INTEGER vs BIGINT PK
	if _, err := db.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t2 (id BIGINT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	sp := NewSchemaProvider(db)

	s1, err := sp.GetTableSchema("t1")
	if err != nil {
		t.Fatal(err)
	}
	s2, err := sp.GetTableSchema("t2")
	if err != nil {
		t.Fatal(err)
	}

	// Both should be auto-increment: INTEGER and BIGINT PRIMARY KEY
	if s1.AutoIncrementCol != "id" {
		t.Errorf("t1.AutoIncrementCol = %q, want %q", s1.AutoIncrementCol, "id")
	}
	if s2.AutoIncrementCol != "id" {
		t.Errorf("t2.AutoIncrementCol = %q, want %q", s2.AutoIncrementCol, "id")
	}
}
