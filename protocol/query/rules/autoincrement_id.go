package rules

import (
	"strconv"
	"strings"

	"github.com/maxpert/marmot/id"
	"vitess.io/vitess/go/vt/sqlparser"
)

// SchemaLookup returns the auto-increment column name for a table.
// Returns empty string if the table has no auto-increment column.
type SchemaLookup func(table string) string

// AutoIncrementIDRule injects distributed HLC-based IDs for auto-increment columns.
// Detection is based on SQLite schema: single BIGINT PRIMARY KEY column.
//
// The rule handles two cases:
// 1. Column missing from INSERT → add column with generated ID
// 2. Column present with 0 or NULL → replace with generated ID
type AutoIncrementIDRule struct {
	generator id.Generator
}

// NewAutoIncrementIDRule creates a new rule with the given ID generator.
func NewAutoIncrementIDRule(gen id.Generator) *AutoIncrementIDRule {
	return &AutoIncrementIDRule{
		generator: gen,
	}
}

func (r *AutoIncrementIDRule) Name() string  { return "AutoIncrementID" }
func (r *AutoIncrementIDRule) Priority() int { return 5 }

// NeedsIDInjection checks if an INSERT statement needs ID injection.
// This is used to bypass cache for statements that need unique IDs.
func (r *AutoIncrementIDRule) NeedsIDInjection(stmt sqlparser.Statement, schemaLookup SchemaLookup) bool {
	if r.generator == nil || schemaLookup == nil {
		return false
	}

	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return false
	}

	tableName := extractInsertTableName(insert)
	if tableName == "" {
		return false
	}

	autoIncCol := schemaLookup(tableName)
	if autoIncCol == "" {
		return false
	}

	// Check if the auto-inc column is missing or has 0/NULL value
	colIdx := findColumnIndex(insert.Columns, autoIncCol)
	if colIdx < 0 {
		// Column missing from INSERT - needs injection
		return true
	}

	// Column present - check if any row has 0/NULL value
	rows, ok := insert.Rows.(sqlparser.Values)
	if !ok {
		return false
	}

	for _, row := range rows {
		if colIdx < len(row) && needsIDInjection(row[colIdx]) {
			return true
		}
	}

	return false
}

func (r *AutoIncrementIDRule) ApplyAST(stmt sqlparser.Statement, schemaLookup SchemaLookup) (sqlparser.Statement, bool, error) {
	if r.generator == nil || schemaLookup == nil {
		return stmt, false, nil
	}

	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return stmt, false, nil
	}

	tableName := extractInsertTableName(insert)
	if tableName == "" {
		return stmt, false, nil
	}

	autoIncCol := schemaLookup(tableName)
	if autoIncCol == "" {
		return stmt, false, nil
	}

	rows, ok := insert.Rows.(sqlparser.Values)
	if !ok {
		return stmt, false, nil
	}

	modified := false

	// Check if the auto-inc column is in the INSERT columns list
	colIdx := findColumnIndex(insert.Columns, autoIncCol)

	if colIdx < 0 {
		// Column missing from INSERT - add column and values
		insert.Columns = append(insert.Columns, sqlparser.NewIdentifierCI(autoIncCol))
		colIdx = len(insert.Columns) - 1

		// Add generated ID to each row
		for i := range rows {
			newID := r.generator.NextID()
			rows[i] = append(rows[i], sqlparser.NewIntLiteral(strconv.FormatUint(newID, 10)))
		}
		modified = true
	} else {
		// Column present - replace 0/NULL values with generated IDs
		for _, row := range rows {
			if colIdx < len(row) && needsIDInjection(row[colIdx]) {
				newID := r.generator.NextID()
				row[colIdx] = sqlparser.NewIntLiteral(strconv.FormatUint(newID, 10))
				modified = true
			}
		}
	}

	if !modified {
		return stmt, false, nil
	}

	return insert, true, nil
}

// extractInsertTableName extracts the table name from an INSERT statement.
func extractInsertTableName(insert *sqlparser.Insert) string {
	if insert.Table == nil {
		return ""
	}
	if tn, ok := insert.Table.Expr.(sqlparser.TableName); ok {
		return tn.Name.String()
	}
	return ""
}

// findColumnIndex returns the index of a column in the columns list, or -1 if not found.
func findColumnIndex(columns sqlparser.Columns, name string) int {
	nameLower := strings.ToLower(name)
	for i, col := range columns {
		if strings.ToLower(col.String()) == nameLower {
			return i
		}
	}
	return -1
}

// needsIDInjection checks if an expression needs an auto-increment ID injection.
// Returns true for:
// - NULL values
// - Integer literal 0
func needsIDInjection(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.NullVal:
		return true
	case *sqlparser.Literal:
		if v.Type == sqlparser.IntVal {
			val := strings.TrimSpace(string(v.Val))
			return val == "0"
		}
	}
	return false
}
