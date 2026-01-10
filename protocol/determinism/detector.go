// Package determinism provides detection of non-deterministic SQL expressions.
// This module determines if a DML statement can be safely replicated via statement-based
// replication, or if it requires CDC (row-based) replication to avoid data divergence.
package determinism

import (
	"strings"

	"github.com/rqlite/sql"
)

// deterministicFunctions contains functions known to be pure/deterministic.
// Unknown functions default to non-deterministic (fail-safe).
var deterministicFunctions = map[string]bool{
	// Math functions
	"abs":      true,
	"sign":     true,
	"round":    true,
	"min":      true,
	"max":      true,
	"least":    true,
	"greatest": true,

	// SQLite core functions
	"coalesce":     true,
	"ifnull":       true,
	"nullif":       true,
	"iif":          true,
	"typeof":       true,
	"zeroblob":     true,
	"likelihood":   true,
	"likely":       true,
	"unlikely":     true,
	"glob":         true,
	"like":         true,
	"instr":        true,
	"unicode":      true,
	"char":         true,
	"printf":       true,
	"format":       true,
	"hex":          true,
	"unhex":        true,
	"quote":        true,
	"substr":       true,
	"substring":    true,
	"replace":      true,
	"trim":         true,
	"ltrim":        true,
	"rtrim":        true,
	"length":       true,
	"lower":        true,
	"upper":        true,
	"soundex":      true,
	"group_concat": true,

	// JSON functions (deterministic with same input)
	"json":              true,
	"json_array":        true,
	"json_object":       true,
	"json_extract":      true,
	"json_insert":       true,
	"json_remove":       true,
	"json_replace":      true,
	"json_set":          true,
	"json_type":         true,
	"json_valid":        true,
	"json_quote":        true,
	"json_group_array":  true,
	"json_group_object": true,

	// Aggregate functions (deterministic for same input set)
	"count": true,
	"sum":   true,
	"avg":   true,
	"total": true,

	// Cast is deterministic
	"cast": true,
}

// nonDeterministicFunctions contains functions that are always non-deterministic.
var nonDeterministicFunctions = map[string]bool{
	"random":            true,
	"randomblob":        true,
	"changes":           true,
	"last_insert_rowid": true,
	"total_changes":     true,
}

// nonDeterministicIdents are SQLite keywords that return current time.
// These are parsed as identifiers, not function calls.
var nonDeterministicIdents = map[string]bool{
	"current_date":      true,
	"current_time":      true,
	"current_timestamp": true,
}

// timeFunctionsWithNow are functions that are non-deterministic only when called with 'now'.
var timeFunctionsWithNow = map[string]bool{
	"datetime":  true,
	"date":      true,
	"time":      true,
	"julianday": true,
	"strftime":  true,
	"unixepoch": true,
	"timediff":  true,
}

// Result holds the determinism check result
type Result struct {
	IsDeterministic bool
	Reason          string
	NonDetFunctions []string // List of non-deterministic function calls found
}

// IsDeterministicFunction checks if a function name is in the deterministic whitelist.
// Case-insensitive. Returns false for unknown functions (fail-safe).
func IsDeterministicFunction(name string) bool {
	lower := strings.ToLower(name)
	if nonDeterministicFunctions[lower] {
		return false
	}
	if timeFunctionsWithNow[lower] {
		return false // These need argument checking
	}
	return deterministicFunctions[lower]
}

// CheckSQL parses SQLite SQL and checks for non-deterministic expressions.
// Returns a Result with determinism status and details.
func CheckSQL(sqlStr string) Result {
	parser := sql.NewParser(strings.NewReader(sqlStr))
	stmt, err := parser.ParseStatement()
	if err != nil {
		// If we can't parse, assume non-deterministic (fail-safe)
		return Result{
			IsDeterministic: false,
			Reason:          "parse error: " + err.Error(),
		}
	}
	return CheckStatement(stmt)
}

// CheckStatement checks a parsed SQLite statement for non-deterministic expressions.
func CheckStatement(stmt sql.Statement) Result {
	checker := &deterministicChecker{}
	sql.Walk(checker, stmt)

	if len(checker.nonDetFuncs) > 0 {
		return Result{
			IsDeterministic: false,
			Reason:          checker.reason,
			NonDetFunctions: checker.nonDetFuncs,
		}
	}

	if checker.hasSubquery {
		return Result{
			IsDeterministic: false,
			Reason:          "contains subquery",
		}
	}

	if checker.hasColumnInSet {
		return Result{
			IsDeterministic: false,
			Reason:          "column reference in SET clause",
		}
	}

	if checker.hasLimitNoOrderBy {
		return Result{
			IsDeterministic: false,
			Reason:          "DELETE with LIMIT but no ORDER BY",
		}
	}

	return Result{IsDeterministic: true}
}

// deterministicChecker implements sql.Visitor to walk the AST
type deterministicChecker struct {
	nonDetFuncs       []string
	reason            string
	hasSubquery       bool
	hasColumnInSet    bool
	hasLimitNoOrderBy bool
	inUpdateSet       bool // Track if we're inside UPDATE SET clause
}

func (c *deterministicChecker) Visit(node sql.Node) (sql.Visitor, sql.Node, error) {
	switch n := node.(type) {
	case *sql.Call:
		c.checkFunctionCall(n)
	case *sql.Ident:
		// Check for non-deterministic identifiers like CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP
		c.checkIdent(n)
	case *sql.SelectStatement:
		// Subquery detection - if we see a SELECT inside another statement
		c.hasSubquery = true
	case *sql.ParenExpr:
		// Check if ParenExpr contains a SELECT (subquery)
		if _, ok := n.X.(sql.SelectExpr); ok {
			c.hasSubquery = true
		}
	case *sql.ExprList:
		// Check if ExprList contains a SELECT (for IN subquery)
		for _, expr := range n.Exprs {
			if _, ok := expr.(sql.SelectExpr); ok {
				c.hasSubquery = true
				break
			}
		}
	case *sql.UpdateStatement:
		// Check SET clause for column references
		for _, assignment := range n.Assignments {
			if c.exprHasColumnRef(assignment.Expr) {
				c.hasColumnInSet = true
				break
			}
		}
	case *sql.DeleteStatement:
		// DELETE with LIMIT but no ORDER BY is non-deterministic
		// (which rows get deleted is implementation-defined)
		if n.LimitExpr != nil && len(n.OrderingTerms) == 0 {
			c.hasLimitNoOrderBy = true
		}
	}
	return c, node, nil
}

func (c *deterministicChecker) VisitEnd(node sql.Node) (sql.Node, error) {
	return node, nil
}

func (c *deterministicChecker) checkIdent(ident *sql.Ident) {
	name := strings.ToLower(ident.Name)
	if nonDeterministicIdents[name] {
		c.nonDetFuncs = append(c.nonDetFuncs, ident.Name)
		if c.reason == "" {
			c.reason = "non-deterministic identifier: " + name
		}
	}
}

func (c *deterministicChecker) checkFunctionCall(call *sql.Call) {
	name := strings.ToLower(call.Name.Name)

	// Check blacklist
	if nonDeterministicFunctions[name] {
		c.nonDetFuncs = append(c.nonDetFuncs, call.String())
		if c.reason == "" {
			c.reason = "non-deterministic function: " + name
		}
		return
	}

	// Check time functions with 'now' argument
	if timeFunctionsWithNow[name] {
		if c.hasNowArgument(call) {
			c.nonDetFuncs = append(c.nonDetFuncs, call.String())
			if c.reason == "" {
				c.reason = name + " with 'now' argument"
			}
		}
		return
	}

	// Check whitelist - unknown functions are non-deterministic
	if !deterministicFunctions[name] {
		c.nonDetFuncs = append(c.nonDetFuncs, call.String())
		if c.reason == "" {
			c.reason = "unknown function: " + name
		}
	}
}

func (c *deterministicChecker) hasNowArgument(call *sql.Call) bool {
	name := strings.ToLower(call.Name.Name)

	// strftime(format, timestring, ...) - format is required, timestring defaults to 'now'
	if name == "strftime" {
		if len(call.Args) <= 1 {
			// Only format provided, timestring defaults to 'now'
			return true
		}
		// Check if second arg (timestring) is 'now'
		if len(call.Args) >= 2 {
			if lit, ok := call.Args[1].(*sql.StringLit); ok {
				if strings.ToLower(lit.Value) == "now" {
					return true
				}
			}
		}
		return false
	}

	// Other time functions: datetime/date/time/julianday/unixepoch(timestring, ...)
	if len(call.Args) == 0 {
		// No args = defaults to 'now'
		return true
	}

	// Check first argument for 'now'
	if lit, ok := call.Args[0].(*sql.StringLit); ok {
		if strings.ToLower(lit.Value) == "now" {
			return true
		}
	}
	return false
}

// columnRefChecker walks an expression to find column references
type columnRefChecker struct {
	hasCol *bool
}

func (v *columnRefChecker) Visit(node sql.Node) (sql.Visitor, sql.Node, error) {
	switch n := node.(type) {
	case *sql.Call:
		// Walk arguments only, skip the function name (which is also an Ident)
		for _, arg := range n.Args {
			sql.Walk(&columnRefChecker{hasCol: v.hasCol}, arg)
		}
		// Return nil to stop recursion into Call's children (Name field)
		return nil, node, nil
	case *sql.Ident:
		*v.hasCol = true
	case *sql.QualifiedRef:
		*v.hasCol = true
	}
	return v, node, nil
}

func (v *columnRefChecker) VisitEnd(node sql.Node) (sql.Node, error) {
	return node, nil
}

func (c *deterministicChecker) exprHasColumnRef(expr sql.Expr) bool {
	hasCol := false
	sql.Walk(&columnRefChecker{hasCol: &hasCol}, expr)
	return hasCol
}

// IsDeterministic is a convenience function that returns just the boolean result.
func IsDeterministic(sqlStr string) bool {
	return CheckSQL(sqlStr).IsDeterministic
}

// IsDeterministicStatement checks a parsed statement.
func IsDeterministicStatement(stmt sql.Statement) bool {
	return CheckStatement(stmt).IsDeterministic
}
