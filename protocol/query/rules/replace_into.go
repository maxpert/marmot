package rules

import (
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

type ReplaceIntoRule struct{}

func (r *ReplaceIntoRule) Name() string  { return "ReplaceInto" }
func (r *ReplaceIntoRule) Priority() int { return 11 }

func (r *ReplaceIntoRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *ReplaceIntoRule) ApplyPattern(sql string) (string, bool, error) {
	re := regexp.MustCompile(`(?i)^\s*REPLACE\s+INTO\b`)
	if !re.MatchString(sql) {
		return sql, false, nil
	}

	newSQL := re.ReplaceAllString(sql, "INSERT OR REPLACE INTO")
	return newSQL, true, nil
}
