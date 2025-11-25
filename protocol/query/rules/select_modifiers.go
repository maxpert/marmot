package rules

import (
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

type SelectModifiersRule struct{}

func (r *SelectModifiersRule) Name() string  { return "SelectModifiers" }
func (r *SelectModifiersRule) Priority() int { return 30 }

func (r *SelectModifiersRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *SelectModifiersRule) ApplyPattern(sql string) (string, bool, error) {
	original := sql
	applied := false

	re := regexp.MustCompile(`(?i)\bSTRAIGHT_JOIN\b`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "JOIN")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\bSQL_CALC_FOUND_ROWS\b`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\bSQL_(NO_)?CACHE\b`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\bSQL_(SMALL|BIG|BUFFER)_RESULT\b`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\b(HIGH|LOW)_PRIORITY\b`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	re = regexp.MustCompile(`(?i)\bDELAYED\b`)
	if re.MatchString(sql) {
		sql = re.ReplaceAllString(sql, "")
		applied = true
	}

	if applied && original != sql {
		return sql, true, nil
	}

	return sql, false, nil
}
