package rules

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type EscapingRule struct{}

func (r *EscapingRule) Name() string  { return "Escaping" }
func (r *EscapingRule) Priority() int { return 60 }

func (r *EscapingRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *EscapingRule) ApplyPattern(sql string) (string, bool, error) {
	original := sql

	sql = strings.ReplaceAll(sql, `\'`, `''`)
	sql = strings.ReplaceAll(sql, `\"`, `"`)
	sql = strings.ReplaceAll(sql, `\\`, `\`)

	if sql != original {
		return sql, true, nil
	}

	return sql, false, nil
}
