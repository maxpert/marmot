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

		// Strip AUTO_INCREMENT keyword (SQLite doesn't use it)
		if col.Type.Options != nil && col.Type.Options.Autoincrement {
			col.Type.Options.Autoincrement = false
			modified = true
		}

		// Strip UNSIGNED
		if col.Type.Unsigned {
			col.Type.Unsigned = false
			modified = true
		}

		// Strip ZEROFILL
		if col.Type.Zerofill {
			col.Type.Zerofill = false
			modified = true
		}

		// Convert all integer types to INTEGER for SQLite
		if upperType != "INTEGER" {
			col.Type.Type = "INTEGER"
			modified = true
		}
	}

	if !modified {
		return nil, ErrRuleNotApplicable
	}

	// Check if CreateTableRule should handle serialization (to extract indexes)
	if len(create.TableSpec.Indexes) > 0 {
		for _, idx := range create.TableSpec.Indexes {
			if idx.Info != nil && idx.Info.Type != sqlparser.IndexTypePrimary {
				// Let CreateTableRule handle serialization (it extracts non-primary indexes)
				return nil, ErrRuleNotApplicable
			}
		}
	}

	sql := serializer.Serialize(create)
	return []TranspiledStatement{{SQL: sql, Params: params}}, nil
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
