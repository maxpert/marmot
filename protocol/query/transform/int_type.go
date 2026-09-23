package transform

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// IntTypeRule normalizes MySQL integer types to SQLite-compatible INTEGER.
//
// MySQL has many integer variations that SQLite doesn't support:
//   - TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT (all become INTEGER)
//   - UNSIGNED modifier (stripped - SQLite doesn't support it)
//   - ZEROFILL modifier (stripped - SQLite doesn't support it)
//   - Display width e.g. INT(11) (kept but ignored by SQLite)
//
// All integer types map to SQLite's INTEGER which is a 64-bit signed int.
// AUTO_INCREMENT columns are converted to INTEGER for SQLite compatibility.
type IntTypeRule struct{}

func (r *IntTypeRule) Name() string {
	return "IntType"
}

func (r *IntTypeRule) Priority() int {
	return 5
}

func (r *IntTypeRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	create, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if create.TableSpec == nil {
		return nil, ErrRuleNotApplicable
	}

	// Read BEFORE CreateTableRule (priority 10, runs after this rule in the
	// same transpile pass) clears TableSpec.Options for SQLite serialization.
	floor := autoIncFloorFromOptions(create.TableSpec.Options)

	modified := false

	for _, col := range create.TableSpec.Columns {
		if col.Type == nil {
			continue
		}

		upperType := strings.ToUpper(col.Type.Type)

		// Check if this is an integer type
		if !isIntegerType(upperType) {
			continue
		}

		if collapseIntegerTypeWithMarker(col.Type, floor) {
			modified = true
		}
	}

	if !modified {
		return nil, ErrRuleNotApplicable
	}

	// Always defer to CreateTableRule for CREATE TABLE serialization.
	// CreateTableRule handles MySQL-specific options (COMMENT, ENGINE, etc.)
	// that IntTypeRule doesn't strip. Without this, both rules would produce
	// output and the unstripped version from IntTypeRule would be executed first.
	return nil, ErrRuleNotApplicable
}

// isIntegerType checks if the type is a MySQL integer type
func isIntegerType(t string) bool {
	switch strings.ToUpper(t) {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT":
		return true
	default:
		return false
	}
}
