package rules

import (
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

type InsertIgnoreRule struct{}

func (r *InsertIgnoreRule) Name() string  { return "InsertIgnore" }
func (r *InsertIgnoreRule) Priority() int { return 10 }

func (r *InsertIgnoreRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *InsertIgnoreRule) ApplyPattern(sql string) (string, bool, error) {
	re := regexp.MustCompile(`(?i)\bINSERT\s+IGNORE\s+INTO\b`)
	if !re.MatchString(sql) {
		return sql, false, nil
	}

	newSQL := re.ReplaceAllString(sql, "INSERT OR IGNORE INTO")
	return newSQL, true, nil
}
