package grpc

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/protocol"

	_ "github.com/mattn/go-sqlite3"
)

// TestApplyReplayCDCUpdateWithPKChange_Simple tests PK-change UPDATE in isolation
func TestApplyReplayCDCUpdateWithPKChange_Simple(t *testing.T) {
	// Create in-memory database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial row
	_, err = db.Exec(`INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')`)
	if err != nil {
		t.Fatalf("Failed to insert row: %v", err)
	}

	// Simulate PK-changing UPDATE
	oldValues := map[string][]byte{
		"id":    mustMarshal(int64(1)),
		"name":  mustMarshal("Alice"),
		"email": mustMarshal("alice@example.com"),
	}

	newValues := map[string][]byte{
		"id":    mustMarshal(int64(2)),
		"name":  mustMarshal("Alice Updated"),
		"email": mustMarshal("alice@example.com"),
	}

	// Build proper UPDATE statement manually (simulating what applyReplayCDCUpdate should do)
	schemaProvider := protocol.NewSchemaProvider(db)
	schema, err := schemaProvider.GetTableSchema("users")
	if err != nil {
		t.Fatalf("Failed to get schema: %v", err)
	}

	// Build SET clause from newValues
	setClauses := []string{}
	setValues := []interface{}{}
	for col, valBytes := range newValues {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))
		var value interface{}
		if err := encoding.Unmarshal(valBytes, &value); err != nil {
			t.Fatalf("Failed to unmarshal value: %v", err)
		}
		setValues = append(setValues, value)
	}

	// Build WHERE clause from oldValues using PK columns
	whereClauses := []string{}
	whereValues := []interface{}{}
	for _, pkCol := range schema.PrimaryKeys {
		pkBytes, ok := oldValues[pkCol]
		if !ok {
			t.Fatalf("PK column %s not found in oldValues", pkCol)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))
		var value interface{}
		if err := encoding.Unmarshal(pkBytes, &value); err != nil {
			t.Fatalf("Failed to unmarshal PK value: %v", err)
		}
		whereValues = append(whereValues, value)
	}

	// Execute UPDATE
	sqlStmt := fmt.Sprintf("UPDATE users SET %s WHERE %s",
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	allValues := append(setValues, whereValues...)
	_, err = db.Exec(sqlStmt, allValues...)
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify: old row (id=1) should NOT exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = 1").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Old row still exists (orphan bug). Expected 0, got %d", count)
	}

	// Verify: new row (id=2) should exist
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = 2").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("New row not found. Expected 1, got %d", count)
	}

	// Verify total row count
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Total row count incorrect. Expected 1, got %d", count)
	}
}

func mustMarshal(v interface{}) []byte {
	data, err := encoding.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal value %v: %v", v, err))
	}
	return data
}
