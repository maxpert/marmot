package rules

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// IntToBigintRule transforms INT/INTEGER AUTO_INCREMENT columns to BIGINT
// in CREATE TABLE statements. This is needed for distributed auto-increment
// support because HLC-based IDs are 64-bit and require BIGINT storage.
//
// Transformation:
//   - INT AUTO_INCREMENT → BIGINT
//   - INTEGER AUTO_INCREMENT → BIGINT
//
// This rule must run BEFORE CreateTableRule which removes AUTO_INCREMENT.
type IntToBigintRule struct{}

func (r *IntToBigintRule) Name() string  { return "IntToBigint" }
func (r *IntToBigintRule) Priority() int { return 4 } // Run before CreateTableRule (priority 5)

func (r *IntToBigintRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	create, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return stmt, false, nil
	}

	if create.TableSpec == nil {
		return stmt, false, nil
	}

	modified := false

	for _, col := range create.TableSpec.Columns {
		if col.Type == nil || col.Type.Options == nil {
			continue
		}

		// Only transform columns with AUTO_INCREMENT
		if !col.Type.Options.Autoincrement {
			continue
		}

		// Check if type is INT or INTEGER (case-insensitive)
		upperType := strings.ToUpper(col.Type.Type)
		if upperType == "INT" || upperType == "INTEGER" {
			col.Type.Type = "bigint"
			modified = true
		}
	}

	if !modified {
		return stmt, false, nil
	}

	return create, true, nil
}

func (r *IntToBigintRule) ApplyPattern(sql string) (string, bool, error) {
	// No pattern-based transformations needed
	return sql, false, nil
}
