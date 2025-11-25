package rules

import (
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

type IndexHintsRule struct{}

func (r *IndexHintsRule) Name() string  { return "IndexHints" }
func (r *IndexHintsRule) Priority() int { return 20 }

func (r *IndexHintsRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *IndexHintsRule) ApplyPattern(sql string) (string, bool, error) {
	original := sql
	applied := false

	re := regexp.MustCompile(`(?i)\s+FORCE\s+INDEX\s*\([^)]+\)`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\s+USE\s+INDEX\s*\([^)]+\)`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\s+IGNORE\s+INDEX\s*\([^)]+\)`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	if applied && original != sql {
		return sql, true, nil
	}

	return sql, false, nil
}
