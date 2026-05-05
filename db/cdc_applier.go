package db

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// CDCExecutor abstracts sql.DB and sql.Tx - both have identical Exec signature.
// This allows CDC operations to work within or outside transactions.
type CDCExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// CDCSchemaProvider provides primary key information needed for UPDATE/DELETE operations.
// Implementations must return the list of primary key column names for a given table.
type CDCSchemaProvider interface {
	GetPrimaryKeys(tableName string) ([]string, error)
}

func quoteSQLiteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteSQLiteIdentList(columns []string) []string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteSQLiteIdent(col)
	}
	return quoted
}

func ApplyCDCEntry(exec CDCExecutor, schema CDCSchemaProvider, entry *IntentEntry) error {
	if entry == nil {
		return nil
	}
	return ApplyCDCValues(exec, schema, OpType(entry.Operation), entry.Table, entry.OldValues, entry.NewValues)
}

func ApplyCDCValues(exec CDCExecutor, schema CDCSchemaProvider, opType OpType, tableName string, oldValues, newValues map[string][]byte) error {
	switch opType {
	case OpTypeInsert, OpTypeReplace:
		return ApplyCDCInsert(exec, tableName, newValues)
	case OpTypeUpdate:
		return ApplyCDCUpdate(exec, schema, tableName, oldValues, newValues)
	case OpTypeDelete:
		return ApplyCDCDelete(exec, schema, tableName, oldValues)
	default:
		return fmt.Errorf("unsupported operation type for CDC: %v", opType)
	}
}

// ApplyCDCInsert performs INSERT OR REPLACE using CDC row data.
// Columns are sorted alphabetically to ensure deterministic SQL generation.
// Values are deserialized from msgpack and []byte values are converted to strings
// to preserve TEXT type affinity in SQLite.
func ApplyCDCInsert(exec CDCExecutor, tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("ApplyCDCInsert %s: no values to insert", tableName)
	}

	// Build INSERT OR REPLACE statement with sorted columns for determinism
	columns := make([]string, 0, len(newValues))
	for col := range newValues {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	placeholders := make([]string, len(columns))
	values := make([]interface{}, len(columns))

	for i, col := range columns {
		placeholders[i] = "?"
		value, err := unmarshalCDCValue(newValues[col])
		if err != nil {
			return fmt.Errorf("ApplyCDCInsert %s: failed to deserialize column %s: %w", tableName, col, err)
		}
		values[i] = value
	}

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		quoteSQLiteIdent(tableName),
		strings.Join(quoteSQLiteIdentList(columns), ", "),
		strings.Join(placeholders, ", "))

	_, err := exec.Exec(sqlStmt, values...)
	if err != nil {
		return fmt.Errorf("ApplyCDCInsert %s: %w", tableName, err)
	}
	return nil
}

// ApplyCDCUpdate performs UPDATE using CDC row data.
// Uses oldValues for WHERE clause (critical for primary key changes) and newValues for SET clause.
// This ensures the correct row is updated even when the primary key itself changes.
func ApplyCDCUpdate(exec CDCExecutor, schema CDCSchemaProvider, tableName string,
	oldValues, newValues map[string][]byte) error {

	if len(newValues) == 0 {
		return fmt.Errorf("ApplyCDCUpdate %s: no values to update", tableName)
	}

	// Get primary key columns from schema
	primaryKeys, err := schema.GetPrimaryKeys(tableName)
	if err != nil {
		return fmt.Errorf("ApplyCDCUpdate %s: failed to get primary keys: %w", tableName, err)
	}

	// Build SET clause from newValues (sorted for determinism)
	setCols := make([]string, 0, len(newValues))
	for col := range newValues {
		setCols = append(setCols, col)
	}
	sort.Strings(setCols)

	setClauses := make([]string, len(setCols))
	values := make([]interface{}, 0, len(newValues)+len(primaryKeys))

	for i, col := range setCols {
		setClauses[i] = fmt.Sprintf("%s = ?", quoteSQLiteIdent(col))
		value, err := unmarshalCDCValue(newValues[col])
		if err != nil {
			return fmt.Errorf("ApplyCDCUpdate %s: failed to deserialize column %s: %w", tableName, col, err)
		}
		values = append(values, value)
	}

	// Build WHERE clause using primary key columns from oldValues
	// This is critical: when PK changes, we need OLD values to find the row
	whereClauses := make([]string, len(primaryKeys))
	for i, pkCol := range primaryKeys {
		// Use oldValues for WHERE clause if available, fallback to newValues
		pkBytes, ok := oldValues[pkCol]
		if !ok {
			// Fallback to newValues if oldValues doesn't have PK
			pkBytes, ok = newValues[pkCol]
			if !ok {
				return fmt.Errorf("ApplyCDCUpdate %s: primary key column %s not found in CDC data", tableName, pkCol)
			}
		}

		clause, nextValues, err := cdcPrimaryKeyPredicate("ApplyCDCUpdate", tableName, pkCol, pkBytes, values)
		if err != nil {
			return err
		}
		whereClauses[i] = clause
		values = nextValues
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		quoteSQLiteIdent(tableName),
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	_, err = exec.Exec(sqlStmt, values...)
	if err != nil {
		return fmt.Errorf("ApplyCDCUpdate %s: %w", tableName, err)
	}
	return nil
}

// ApplyCDCDelete performs DELETE using CDC row data.
// Uses oldValues to build WHERE clause based on primary key columns.
func ApplyCDCDelete(exec CDCExecutor, schema CDCSchemaProvider, tableName string,
	oldValues map[string][]byte) error {

	if len(oldValues) == 0 {
		return fmt.Errorf("ApplyCDCDelete %s: no values provided", tableName)
	}

	// Get primary key columns from schema
	primaryKeys, err := schema.GetPrimaryKeys(tableName)
	if err != nil {
		return fmt.Errorf("ApplyCDCDelete %s: failed to get primary keys: %w", tableName, err)
	}

	// Build WHERE clause using primary key columns from oldValues
	whereClauses := make([]string, len(primaryKeys))
	values := make([]interface{}, 0, len(primaryKeys))

	for i, pkCol := range primaryKeys {
		pkBytes, ok := oldValues[pkCol]
		if !ok {
			return fmt.Errorf("ApplyCDCDelete %s: primary key column %s not found in old values", tableName, pkCol)
		}

		clause, nextValues, err := cdcPrimaryKeyPredicate("ApplyCDCDelete", tableName, pkCol, pkBytes, values)
		if err != nil {
			return err
		}
		whereClauses[i] = clause
		values = nextValues
	}

	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
		quoteSQLiteIdent(tableName),
		strings.Join(whereClauses, " AND "))

	_, err = exec.Exec(sqlStmt, values...)
	if err != nil {
		return fmt.Errorf("ApplyCDCDelete %s: %w", tableName, err)
	}
	return nil
}

func cdcPrimaryKeyPredicate(opName, tableName, pkCol string, pkBytes []byte, values []interface{}) (string, []interface{}, error) {
	value, err := unmarshalCDCValue(pkBytes)
	if err != nil {
		return "", nil, fmt.Errorf("%s %s: failed to deserialize PK column %s: %w", opName, tableName, pkCol, err)
	}
	if value == nil {
		return fmt.Sprintf("%s IS NULL", quoteSQLiteIdent(pkCol)), values, nil
	}
	return fmt.Sprintf("%s = ?", quoteSQLiteIdent(pkCol)), append(values, value), nil
}
