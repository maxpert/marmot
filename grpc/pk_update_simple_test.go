package grpc

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/maxpert/marmot/encoding"

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
	// Get primary keys from PRAGMA table_info
	primaryKeys, err := getPrimaryKeysFromPragma(db, "users")
	if err != nil {
		t.Fatalf("Failed to get primary keys: %v", err)
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
	for _, pkCol := range primaryKeys {
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

// getPrimaryKeysFromPragma extracts primary key column names from PRAGMA table_info
func getPrimaryKeysFromPragma(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pkCol struct {
		name  string
		order int
	}
	var pkCols []pkCol

	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dfltValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		if pk > 0 {
			pkCols = append(pkCols, pkCol{name: name, order: pk})
		}
	}

	// Sort by PK order
	sort.Slice(pkCols, func(i, j int) bool {
		return pkCols[i].order < pkCols[j].order
	})

	pks := make([]string, len(pkCols))
	for i, col := range pkCols {
		pks[i] = col.name
	}
	return pks, rows.Err()
}
