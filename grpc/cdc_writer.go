package grpc

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/maxpert/marmot/protocol"
)

// writeCDCData writes row data directly to the database (CDC approach)
// This replaces SQL execution for DML operations
func writeCDCData(sqlDB *sql.DB, tableName string, statementType StatementType, rowChange *RowChange) error {
	if rowChange == nil {
		return fmt.Errorf("nil row change data")
	}

	switch statementType {
	case StatementType_INSERT, StatementType_REPLACE:
		return writeInsert(sqlDB, tableName, rowChange.NewValues)
	case StatementType_UPDATE:
		return writeUpdate(sqlDB, tableName, rowChange.RowKey, rowChange.NewValues)
	case StatementType_DELETE:
		return writeDelete(sqlDB, tableName, rowChange.RowKey, rowChange.OldValues)
	default:
		return fmt.Errorf("unsupported statement type for CDC: %v", statementType)
	}
}

// writeInsert performs INSERT OR REPLACE using CDC row data
func writeInsert(db *sql.DB, tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	// Build INSERT OR REPLACE statement
	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	// Collect columns and sort for deterministic SQL generation
	for col := range newValues {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	for _, col := range columns {
		placeholders = append(placeholders, "?")

		// Deserialize value from JSON
		var value string
		if err := json.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sql := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := db.Exec(sql, values...)
	return err
}

// writeUpdate performs UPDATE using CDC row data
func writeUpdate(sqlDB *sql.DB, tableName string, rowKey string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to update")
	}

	// Get table schema to build proper WHERE clause
	schemaProvider := protocol.NewSchemaProvider(sqlDB)
	schema, err := schemaProvider.GetTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}

	// Build UPDATE statement with SET clause
	setClauses := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues)+len(schema.PrimaryKeys))

	for col, valBytes := range newValues {
		// Skip PK columns in SET clause (they're in WHERE clause)
		isPK := false
		for _, pkCol := range schema.PrimaryKeys {
			if col == pkCol {
				isPK = true
				break
			}
		}
		if isPK {
			continue
		}

		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))

		// Deserialize value from JSON
		var value interface{}
		if err := json.Unmarshal(valBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	// Build WHERE clause using primary key columns
	// For both single and composite PK: extract PK values from newValues
	// (newValues contains both SET columns and PK columns from WHERE clause)
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		pkValue, ok := newValues[pkCol]
		if !ok {
			return fmt.Errorf("primary key column %s not found in CDC data", pkCol)
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		// Deserialize PK value
		var value interface{}
		if err := json.Unmarshal(pkValue, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	_, err = sqlDB.Exec(sqlStmt, values...)
	return err
}

// writeDelete performs DELETE using CDC row data
func writeDelete(sqlDB *sql.DB, tableName string, rowKey string, oldValues map[string][]byte) error {
	if rowKey == "" {
		return fmt.Errorf("empty row key for delete")
	}

	// Get table schema to build proper WHERE clause
	schemaProvider := protocol.NewSchemaProvider(sqlDB)
	schema, err := schemaProvider.GetTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}

	// Build WHERE clause using primary key columns
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))
	values := make([]interface{}, 0, len(schema.PrimaryKeys))

	if len(schema.PrimaryKeys) == 1 {
		// Single PK: row key is the value directly
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", schema.PrimaryKeys[0]))
		values = append(values, rowKey)
	} else {
		// Composite PK: extract PK values from oldValues
		for _, pkCol := range schema.PrimaryKeys {
			pkValue, ok := oldValues[pkCol]
			if !ok {
				return fmt.Errorf("primary key column %s not found in CDC old values", pkCol)
			}

			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

			// Deserialize PK value
			var value interface{}
			if err := json.Unmarshal(pkValue, &value); err != nil {
				return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
			}
			values = append(values, value)
		}
	}

	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
		tableName,
		strings.Join(whereClauses, " AND "))

	_, err = sqlDB.Exec(sqlStmt, values...)
	return err
}
