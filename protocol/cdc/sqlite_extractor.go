package cdc

import (
	"fmt"
	"strings"

	rqlitesql "github.com/rqlite/sql"
)

// ExtractRowDataFromSQLite extracts row-level data from a SQLite AST for CDC replication
// This is the SQLite equivalent of ExtractRowData which uses Vitess AST
// NOTE: For multi-row INSERTs, this only returns the FIRST row. Use ExtractAllRowDataFromSQLite instead.
func ExtractRowDataFromSQLite(ast rqlitesql.Statement) (*RowData, error) {
	if ast == nil {
		return nil, fmt.Errorf("nil AST")
	}

	switch stmt := ast.(type) {
	case *rqlitesql.InsertStatement:
		return extractFromSQLiteInsert(stmt)
	case *rqlitesql.UpdateStatement:
		return extractFromSQLiteUpdate(stmt)
	case *rqlitesql.DeleteStatement:
		return extractFromSQLiteDelete(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type for CDC: %T", stmt)
	}
}

// ExtractAllRowDataFromSQLite extracts ALL rows from a SQLite AST for CDC replication
// For INSERT statements with multiple rows (VALUES (...), (...)), returns one RowData per row
// For UPDATE/DELETE, returns a single-element slice (same as ExtractRowDataFromSQLite)
func ExtractAllRowDataFromSQLite(ast rqlitesql.Statement) ([]*RowData, error) {
	if ast == nil {
		return nil, fmt.Errorf("nil AST")
	}

	switch stmt := ast.(type) {
	case *rqlitesql.InsertStatement:
		return extractAllFromSQLiteInsert(stmt)
	case *rqlitesql.UpdateStatement:
		rowData, err := extractFromSQLiteUpdate(stmt)
		if err != nil {
			return nil, err
		}
		return []*RowData{rowData}, nil
	case *rqlitesql.DeleteStatement:
		rowData, err := extractFromSQLiteDelete(stmt)
		if err != nil {
			return nil, err
		}
		return []*RowData{rowData}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type for CDC: %T", stmt)
	}
}

// extractAllFromSQLiteInsert extracts ALL rows from a multi-row SQLite INSERT statement
func extractAllFromSQLiteInsert(stmt *rqlitesql.InsertStatement) ([]*RowData, error) {
	// Get column names
	columns := make([]string, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		columns = append(columns, rqlitesql.IdentName(col))
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("INSERT must have explicit column list for CDC")
	}

	// Check for INSERT ... SELECT (not supported for CDC)
	if stmt.Select != nil {
		return nil, fmt.Errorf("INSERT ... SELECT not supported for CDC")
	}

	if len(stmt.ValueLists) == 0 {
		return nil, fmt.Errorf("INSERT must have VALUES clause for CDC")
	}

	// Extract each row
	result := make([]*RowData, 0, len(stmt.ValueLists))
	for rowIdx, valueList := range stmt.ValueLists {
		if len(valueList.Exprs) != len(columns) {
			return nil, fmt.Errorf("column count mismatch in row %d: %d columns, %d values", rowIdx, len(columns), len(valueList.Exprs))
		}

		newValues := make(map[string][]byte)
		for i, col := range columns {
			value, err := extractSQLiteExprValue(valueList.Exprs[i])
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

// extractFromSQLiteInsert extracts row data from SQLite INSERT statement
func extractFromSQLiteInsert(stmt *rqlitesql.InsertStatement) (*RowData, error) {
	// Get column names
	columns := make([]string, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		columns = append(columns, rqlitesql.IdentName(col))
	}

	// Require explicit column list for CDC
	if len(columns) == 0 {
		return nil, fmt.Errorf("INSERT must have explicit column list for CDC")
	}

	// Check for INSERT ... SELECT (not supported for CDC)
	if stmt.Select != nil {
		return nil, fmt.Errorf("INSERT ... SELECT not supported for CDC")
	}

	// Get values from VALUES clause
	if len(stmt.ValueLists) == 0 {
		return nil, fmt.Errorf("INSERT must have VALUES clause for CDC")
	}

	// Take first row (caller should split multi-row inserts if needed)
	firstRow := stmt.ValueLists[0]
	if len(firstRow.Exprs) != len(columns) {
		return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(columns), len(firstRow.Exprs))
	}

	// Extract values
	newValues := make(map[string][]byte)

	for i, col := range columns {
		value, err := extractSQLiteExprValue(firstRow.Exprs[i])
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

// extractFromSQLiteUpdate extracts row data from SQLite UPDATE statement
func extractFromSQLiteUpdate(stmt *rqlitesql.UpdateStatement) (*RowData, error) {
	if stmt.WhereExpr == nil {
		return nil, fmt.Errorf("UPDATE without WHERE clause not supported for CDC (unsafe multi-row update)")
	}

	// Extract new values from SET clause (Assignments)
	newValues := make(map[string][]byte)
	for _, assign := range stmt.Assignments {
		// Get column name(s) - typically one column per assignment
		for _, col := range assign.Columns {
			colName := rqlitesql.IdentName(col)
			value, err := extractSQLiteExprValue(assign.Expr)
			if err != nil {
				return nil, fmt.Errorf("failed to extract value for column %s: %w", colName, err)
			}

			valueBytes, err := serializeValue(value)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize value for column %s: %w", colName, err)
			}

			newValues[colName] = valueBytes
		}
	}

	// Extract primary key values from WHERE clause
	whereValues, err := extractSQLitePKValuesFromWhere(stmt.WhereExpr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PK values from WHERE: %w", err)
	}

	// Merge WHERE PK values into newValues so caller has all PK values
	for col, val := range whereValues {
		if _, exists := newValues[col]; !exists {
			newValues[col] = val
		}
	}

	// OldValues would require reading from database (not implemented yet)
	oldValues := make(map[string][]byte)

	return &RowData{
		OldValues: oldValues,
		NewValues: newValues,
	}, nil
}

// extractFromSQLiteDelete extracts row data from SQLite DELETE statement
func extractFromSQLiteDelete(stmt *rqlitesql.DeleteStatement) (*RowData, error) {
	if stmt.WhereExpr == nil {
		return nil, fmt.Errorf("DELETE without WHERE clause not supported for CDC (unsafe multi-row delete)")
	}

	// Extract primary key values from WHERE clause
	whereValues, err := extractSQLitePKValuesFromWhere(stmt.WhereExpr)
	if err != nil {
		return nil, fmt.Errorf("failed to extract PK values from WHERE: %w", err)
	}

	return &RowData{
		OldValues: whereValues,
	}, nil
}

// extractSQLiteExprValue extracts string representation from a SQLite expression
func extractSQLiteExprValue(expr rqlitesql.Expr) (string, error) {
	switch v := expr.(type) {
	case *rqlitesql.StringLit:
		return v.Value, nil
	case *rqlitesql.NumberLit:
		return v.Value, nil
	case *rqlitesql.BoolLit:
		if v.Value {
			return "1", nil
		}
		return "0", nil
	case *rqlitesql.NullLit:
		return "NULL", nil
	case *rqlitesql.BlobLit:
		return v.Value, nil
	case *rqlitesql.BindExpr:
		// For prepared statements with placeholders
		return fmt.Sprintf("?%s", v.Name), nil
	case *rqlitesql.Ident:
		// Column reference or identifier
		return v.Name, nil
	case *rqlitesql.QualifiedRef:
		// table.column reference
		if v.Column != nil {
			return v.Column.Name, nil
		}
		return "", fmt.Errorf("QualifiedRef without column")
	default:
		// Fallback to string representation
		return expr.String(), nil
	}
}

// extractSQLitePKValuesFromWhere extracts primary key values from WHERE clause
func extractSQLitePKValuesFromWhere(expr rqlitesql.Expr) (map[string][]byte, error) {
	values := make(map[string][]byte)

	switch e := expr.(type) {
	case *rqlitesql.BinaryExpr:
		// Check for equality comparison (column = value)
		if e.Op == rqlitesql.EQ {
			colName, val, err := extractSQLiteComparisonParts(e)
			if err != nil {
				return nil, err
			}
			if colName != "" {
				values[colName] = val
			}
		} else if e.Op == rqlitesql.AND {
			// For AND expressions, collect all equality comparisons
			leftVals, err := extractSQLitePKValuesFromWhere(e.X)
			if err != nil {
				return nil, err
			}
			rightVals, err := extractSQLitePKValuesFromWhere(e.Y)
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
		} else if e.Op == rqlitesql.OR {
			// OR is not supported for PK extraction (ambiguous which row)
			return nil, fmt.Errorf("OR expressions not supported in WHERE for CDC")
		}
		// Other operators are ignored (not equality comparisons)

	case *rqlitesql.ParenExpr:
		// Unwrap parenthesized expression
		return extractSQLitePKValuesFromWhere(e.X)

	default:
		// Unsupported expression type - not necessarily an error
		// Could be a complex expression that doesn't affect PK extraction
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("no equality comparisons found in WHERE clause")
	}

	return values, nil
}

// extractSQLiteComparisonParts extracts column name and value from a binary comparison
func extractSQLiteComparisonParts(expr *rqlitesql.BinaryExpr) (string, []byte, error) {
	var colName string
	var valueExpr rqlitesql.Expr

	// Try left side as column, right side as value
	switch left := expr.X.(type) {
	case *rqlitesql.Ident:
		colName = left.Name
		valueExpr = expr.Y
	case *rqlitesql.QualifiedRef:
		if left.Column != nil {
			colName = left.Column.Name
			valueExpr = expr.Y
		}
	default:
		// Try right side as column, left side as value (reverse comparison)
		switch right := expr.Y.(type) {
		case *rqlitesql.Ident:
			colName = right.Name
			valueExpr = expr.X
		case *rqlitesql.QualifiedRef:
			if right.Column != nil {
				colName = right.Column.Name
				valueExpr = expr.X
			}
		}
	}

	if colName == "" {
		return "", nil, nil // Not a simple column = value comparison
	}

	// Extract the value
	val, err := extractSQLiteExprValue(valueExpr)
	if err != nil {
		return "", nil, fmt.Errorf("failed to extract value: %w", err)
	}

	// Serialize value
	valBytes, err := serializeValue(val)
	if err != nil {
		return "", nil, fmt.Errorf("failed to serialize value: %w", err)
	}

	return colName, valBytes, nil
}

// ParseSQLiteStatement parses SQL using rqlite/sql parser
func ParseSQLiteStatement(sql string) (rqlitesql.Statement, error) {
	parser := rqlitesql.NewParser(strings.NewReader(sql))
	return parser.ParseStatement()
}

// ExtractRowDataFromSQL parses SQL and extracts CDC data using SQLite parser
// This is a convenience function that combines parsing and extraction
func ExtractRowDataFromSQL(sql string) (*RowData, error) {
	stmt, err := ParseSQLiteStatement(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	return ExtractRowDataFromSQLite(stmt)
}
