package rules

import (
	"fmt"
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

type LimitRule struct{}

func (r *LimitRule) Name() string  { return "Limit" }
func (r *LimitRule) Priority() int { return 50 }

func (r *LimitRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *LimitRule) ApplyPattern(sql string) (string, bool, error) {
	re := regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)\s*,\s*(\d+)`)
	if !re.MatchString(sql) {
		return sql, false, nil
	}

	newSQL := re.ReplaceAllStringFunc(sql, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) == 3 {
			return fmt.Sprintf("LIMIT %s OFFSET %s", parts[2], parts[1])
		}
		return match
	})

	return newSQL, newSQL != sql, nil
}
