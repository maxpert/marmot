package rules

import (
	"regexp"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TransactionSyntaxRule converts MySQL transaction syntax to SQLite
// MySQL: START TRANSACTION [READ ONLY | READ WRITE]
// SQLite: BEGIN [DEFERRED | IMMEDIATE | EXCLUSIVE]
type TransactionSyntaxRule struct{}

func (r *TransactionSyntaxRule) Name() string  { return "TransactionSyntax" }
func (r *TransactionSyntaxRule) Priority() int { return 5 }

func (r *TransactionSyntaxRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	return stmt, false, nil
}

func (r *TransactionSyntaxRule) ApplyPattern(sql string) (string, bool, error) {
	// START TRANSACTION [READ ONLY | READ WRITE] → BEGIN
	re := regexp.MustCompile(`(?i)^\s*START\s+TRANSACTION\b.*$`)
	if !re.MatchString(sql) {
		return sql, false, nil
	}
	return "BEGIN", true, nil
}
