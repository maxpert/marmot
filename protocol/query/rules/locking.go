package rules

import (
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

type LockingRule struct{}

func (r *LockingRule) Name() string  { return "Locking" }
func (r *LockingRule) Priority() int { return 40 }

func (r *LockingRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *LockingRule) ApplyPattern(sql string) (string, bool, error) {
	original := sql
	applied := false

	re := regexp.MustCompile(`(?i)\s+FOR\s+UPDATE(\s+|$)`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, " ")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\s+LOCK\s+IN\s+SHARE\s+MODE(\s+|$)`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, " ")
		applied = true
	}

	if applied && original != sql {
		return sql, true, nil
	}

	return sql, false, nil
}
