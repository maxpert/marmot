package cdc

import (
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// RowData represents the extracted row-level data from a SQL statement
// This is the CDC (Change Data Capture) representation
// Note: RowKey is NOT populated here - it's generated at caller level using schema
type RowData struct {
	OldValues map[string][]byte // Before image (for UPDATE/DELETE)
	NewValues map[string][]byte // After image (for INSERT/UPDATE/REPLACE)
}

// ExtractRowData extracts row-level data from a MySQL AST for CDC replication
// This follows the industry standard approach:
// - MySQL binlog: sends before/after row images
// - TiDB TiCDC: sends "p" (previous) and "u" (updated) data
// - CockroachDB: uses diff option for before/after values
// NOTE: For multi-row INSERTs, this only returns the FIRST row. Use ExtractAllRowData instead.
func ExtractRowData(ast sqlparser.Statement) (*RowData, error) {
	if ast == nil {
		return nil, fmt.Errorf("nil AST")
	}

	switch stmt := ast.(type) {
	case *sqlparser.Insert:
		return extractFromInsert(stmt)
	case *sqlparser.Update:
		return extractFromUpdate(stmt)
	case *sqlparser.Delete:
		return extractFromDelete(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// ExtractAllRowData extracts ALL rows from a MySQL AST for CDC replication
// For INSERT statements with multiple rows (VALUES (...), (...)), returns one RowData per row
// For UPDATE/DELETE, returns a single-element slice (same as ExtractRowData)
func ExtractAllRowData(ast sqlparser.Statement) ([]*RowData, error) {
	if ast == nil {
		return nil, fmt.Errorf("nil AST")
	}

	switch stmt := ast.(type) {
	case *sqlparser.Insert:
		return extractAllFromInsert(stmt)
	case *sqlparser.Update:
		rowData, err := extractFromUpdate(stmt)
		if err != nil {
			return nil, err
		}
		return []*RowData{rowData}, nil
	case *sqlparser.Delete:
		rowData, err := extractFromDelete(stmt)
		if err != nil {
			return nil, err
		}
		return []*RowData{rowData}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// extractAllFromInsert extracts ALL rows from a multi-row INSERT statement
func extractAllFromInsert(stmt *sqlparser.Insert) ([]*RowData, error) {
	// Get column names
	columns := make([]string, 0)
	if stmt.Columns != nil {
		for _, col := range stmt.Columns {
			columns = append(columns, col.String())
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("INSERT must have explicit column list for CDC")
	}

	// Get values from VALUES clause
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(rows) == 0 {
		return nil, fmt.Errorf("invalid INSERT VALUES clause")
	}

	// Extract each row
	result := make([]*RowData, 0, len(rows))
	for rowIdx, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("column count mismatch in row %d: %d columns, %d values", rowIdx, len(columns), len(row))
		}

		newValues := make(map[string][]byte)
		for i, col := range columns {
			value, err := extractValue(row[i])
			if err != nil {
				return nil, fmt.Errorf("failed to extract value for column %s in row %d: %w", col, rowIdx, err)
			}

			valueBytes, err := serializeValue(value)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize value for column %s in row %d: %w", col, rowIdx, err)
			}

			newValues[col] = valueBytes
		}

		result = append(result, &RowData{NewValues: newValues})
	}

	return result, nil
}

// extractFromInsert extracts row data from INSERT statement
// Returns: RowKey + NewValues
func extractFromInsert(stmt *sqlparser.Insert) (*RowData, error) {
	// Get column names
	columns := make([]string, 0)
	if stmt.Columns != nil {
		for _, col := range stmt.Columns {
			columns = append(columns, col.String())
		}
	}

	// Get values from VALUES clause
	// Note: For multi-row inserts, this should be called once per row
	// The caller is responsible for splitting multi-row INSERTs
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok || len(rows) == 0 {
		return nil, fmt.Errorf("invalid INSERT VALUES clause")
	}

	// Take first row (caller should split multi-row inserts)
	firstRow := rows[0]
	if len(columns) == 0 {
		return nil, fmt.Errorf("INSERT must have explicit column list for CDC")
	}
	if len(firstRow) != len(columns) {
		return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(firstRow))
	}

	// Extract values
	newValues := make(map[string][]byte)

	for i, col := range columns {
		value, err := extractValue(firstRow[i])
		if err != nil {
			return nil, fmt.Errorf("failed to extract value for column %s: %w", col, err)
		}

		// Serialize value to bytes
		valueBytes, err := serializeValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize value for column %s: %w", col, err)
		}

		newValues[col] = valueBytes
	}

	return &RowData{
		NewValues: newValues,
	}, nil
}

// extractFromUpdate extracts row data from UPDATE statement
// Returns: OldValues + NewValues
// Note: For UPDATE statements that affect multiple rows, this should be called once per row
// Note: RowKey extraction happens at caller level using schema
func extractFromUpdate(stmt *sqlparser.Update) (*RowData, error) {
	if stmt.Where == nil {
		return nil, fmt.Errorf("UPDATE without WHERE clause not supported for CDC (unsafe multi-row update)")
	}

	// Extract new values from SET clause
	newValues := make(map[string][]byte)
	for _, expr := range stmt.Exprs {
		colName := expr.Name.Name.String()
		value, err := extractValue(expr.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to extract value for column %s: %w", colName, err)
		}

		valueBytes, err := serializeValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize value for column %s: %w", colName, err)
		}

		newValues[colName] = valueBytes
	}

	// Extract primary key values from WHERE clause
	// These are needed to generate row key at caller level
	whereValues, err := extractPKValuesFromWhere(stmt.Where.Expr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PK values from WHERE: %w", err)
	}

	// Merge WHERE PK values into newValues so caller has all PK values
	for col, val := range whereValues {
		// Only add if not already in SET clause
		if _, exists := newValues[col]; !exists {
			newValues[col] = val
		}
	}

	// TODO: For proper CDC, we need to read old values from the database
	// For now, we'll only send new values (sufficient for most cases)
	// Old values are needed for:
	// 1. Conflict detection (comparing old values)
	// 2. Downstream systems that need before/after images
	oldValues := make(map[string][]byte)

	return &RowData{
		OldValues: oldValues, // TODO: Read from database
		NewValues: newValues,
	}, nil
}

// extractFromDelete extracts row data from DELETE statement
// Returns: OldValues (with PK values from WHERE)
// Note: RowKey extraction happens at caller level using schema
func extractFromDelete(stmt *sqlparser.Delete) (*RowData, error) {
	if stmt.Where == nil {
		return nil, fmt.Errorf("DELETE without WHERE clause not supported for CDC (unsafe multi-row delete)")
	}

	// Extract primary key values from WHERE clause
	// These are needed to generate row key at caller level
	whereValues, err := extractPKValuesFromWhere(stmt.Where.Expr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PK values from WHERE: %w", err)
	}

	// TODO: For proper CDC, we should read old values from database before deletion
	// This allows downstream systems to know what was deleted
	// For now, we just provide the PK values from WHERE
	oldValues := whereValues

	return &RowData{
		OldValues: oldValues, // TODO: Read full row from database
	}, nil
}

// extractValue extracts string representation from an expression
func extractValue(expr sqlparser.Expr) (string, error) {
	switch v := expr.(type) {
	case *sqlparser.Literal:
		return v.Val, nil
	case *sqlparser.Argument:
		// For prepared statements
		return fmt.Sprintf("?%s", string(v.Name)), nil
	default:
		// Fallback to string representation
		return sqlparser.String(expr), nil
	}
}

// extractPKValuesFromWhere extracts primary key values from WHERE clause
// Returns a map of column name -> serialized value
// This is used to identify which row is being updated/deleted
func extractPKValuesFromWhere(expr sqlparser.Expr) (map[string][]byte, error) {
	values := make(map[string][]byte)

	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		// e.g., YCSB_KEY = 'user123' or id = 42
		if e.Operator == sqlparser.EqualOp {
			// Get column name from left side
			var colName string
			switch left := e.Left.(type) {
			case *sqlparser.ColName:
				colName = left.Name.String()
			default:
				return nil, fmt.Errorf("unsupported left side in comparison: %T", e.Left)
			}

			// Get value from right side
			val, err := extractValue(e.Right)
			if err != nil {
				return nil, fmt.Errorf("failed to extract value: %w", err)
			}

			// Serialize value
			valBytes, err := serializeValue(val)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize value: %w", err)
			}

			values[colName] = valBytes
		}

	case *sqlparser.AndExpr:
		// For AND expressions, collect all equality comparisons
		leftVals, err := extractPKValuesFromWhere(e.Left)
		if err != nil {
			return nil, err
		}
		rightVals, err := extractPKValuesFromWhere(e.Right)
		if err != nil {
			return nil, err
		}

		// Merge both sides
		for k, v := range leftVals {
			values[k] = v
		}
		for k, v := range rightVals {
			values[k] = v
		}

	case *sqlparser.OrExpr:
		// OR is not supported for PK extraction (ambiguous which row)
		return nil, fmt.Errorf("OR expressions not supported in WHERE for CDC")

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("no equality comparisons found in WHERE clause")
	}

	return values, nil
}

// serializeValue converts a value to bytes for transmission
// Uses JSON for simplicity (could be optimized with msgpack or protobuf)
func serializeValue(value string) ([]byte, error) {
	return json.Marshal(value)
}
