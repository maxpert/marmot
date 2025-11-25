package query

import "vitess.io/vitess/go/vt/sqlparser"

type Rule interface {
	Name() string
	Priority() int
	ApplyAST(ast sqlparser.Statement) (sqlparser.Statement, bool, error)
	ApplyPattern(sql string) (string, bool, error)
}

type RuleSet []Rule

func (rs RuleSet) Len() int           { return len(rs) }
func (rs RuleSet) Less(i, j int) bool { return rs[i].Priority() < rs[j].Priority() }
func (rs RuleSet) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }
