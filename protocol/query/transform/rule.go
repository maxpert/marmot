// Package transform provides AST transformation rules for MySQL to SQLite transpilation.
//
// Transform rules modify the Vitess AST structure for semantic changes that cannot
// be handled by the serializer alone. Examples include:
//   - Converting DELETE with JOIN to subquery form
//   - Converting UPDATE with JOIN to correlated subquery
//   - Extracting KEY definitions from CREATE TABLE into separate CREATE INDEX
//
// Syntactic differences (data types, function names, etc.) are handled by the
// SQLiteSerializer, not transform rules.
package transform

import (
	"errors"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TranspiledStatement represents a SQLite-ready statement with parameters
type TranspiledStatement struct {
	SQL    string
	Params []interface{}
}

// ErrRuleNotApplicable indicates rule doesn't apply to this statement type
var ErrRuleNotApplicable = errors.New("rule not applicable")

// FoundRowsColumnName is the internal column name used for SQL_CALC_FOUND_ROWS support.
// The execution layer looks for this column to extract the total row count.
const FoundRowsColumnName = "__marmot_found_rows"

// SchemaInfo provides table schema information needed for transpilation.
// Used for ON CONFLICT target detection and AUTO_INCREMENT ID injection.
type SchemaInfo struct {
	// PrimaryKey contains column names that form the PRIMARY KEY.
	// For single-column PK: ["id"]
	// For composite PK: ["tenant_id", "user_id"]
	// Empty if no PRIMARY KEY defined.
	PrimaryKey []string

	// UniqueKeys contains column sets for each UNIQUE constraint.
	// Each inner slice is one unique constraint's columns.
	// Example: [["email"], ["tenant_id", "slug"]]
	UniqueKeys [][]string

	// AutoIncrementColumn is the column with AUTO_INCREMENT.
	// Empty string if no auto-increment column exists.
	// In SQLite, this is typically INTEGER PRIMARY KEY.
	AutoIncrementColumn string
}

// HasPrimaryKey returns true if table has a PRIMARY KEY defined
func (s *SchemaInfo) HasPrimaryKey() bool {
	return s != nil && len(s.PrimaryKey) > 0
}

// HasAutoIncrement returns true if table has an auto-increment column
func (s *SchemaInfo) HasAutoIncrement() bool {
	return s != nil && s.AutoIncrementColumn != ""
}

// ConflictTarget returns the best columns to use for ON CONFLICT.
// Prefers PRIMARY KEY, falls back to first UNIQUE KEY.
// Returns nil if no suitable conflict target exists.
func (s *SchemaInfo) ConflictTarget() []string {
	if s == nil {
		return nil
	}
	if len(s.PrimaryKey) > 0 {
		return s.PrimaryKey
	}
	if len(s.UniqueKeys) > 0 {
		return s.UniqueKeys[0]
	}
	return nil
}

// SchemaProvider returns schema information for a table.
// Returns nil if table doesn't exist or schema is unknown.
type SchemaProvider func(database, table string) *SchemaInfo

// Serializer converts a Vitess AST to SQL string
type Serializer interface {
	Serialize(stmt sqlparser.Statement) string
}

// Rule transforms a MySQL AST for SQLite compatibility.
// Rules handle semantic changes; syntactic mapping is done by the serializer.
type Rule interface {
	// Name returns the rule name for logging
	Name() string

	// Priority determines rule execution order (lower runs first)
	Priority() int

	// Transform converts MySQL AST to SQLite statements with parameters.
	// Mutates stmt in place where possible.
	// Returns ErrRuleNotApplicable if rule doesn't apply to this statement type.
	// serializer is used to convert the mutated AST to SQL string.
	Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error)
}
