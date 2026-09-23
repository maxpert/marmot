package rules

import (
	"strings"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/transform"
	"vitess.io/vitess/go/vt/sqlparser"
)

// ClaimTableGuardRule refuses client SQL that could mutate one of Marmot's own
// internal tables (see common.IsInternalTableName) - most importantly
// __marmot__autoinc, the AUTO_INCREMENT claim table Marmot keeps in its system
// database keyed by (database, table). db.AutoIncClaimStore, driven by the
// COMMIT handler and by DDL-time seeding, is the ONLY writer of that table; it never goes through this pipeline (see the doc
// comment below). A client able to INSERT, UPDATE, DELETE or DROP/ALTER/
// TRUNCATE/RENAME it, or CREATE TABLE over it, could make a live
// AUTO_INCREMENT table re-mint ids it has already handed out - the entire
// safety argument for narrow AUTO_INCREMENT rests on that row being
// authoritative and untouchable by a client.
//
// SELECT is deliberately NOT rejected: the table is already hidden from SHOW
// TABLES and INFORMATION_SCHEMA (see protocol/handlers/metadata.go and
// protocol/handlers/information_schema.go), and a read cannot corrupt the
// claim - only a write to the row or a schema change to the table can. So
// this rule only inspects statement shapes that write data or schema
// (INSERT/REPLACE/UPDATE/DELETE and DDL), never SELECT.
//
// The check is a structural match on the parsed AST's table name(s), never a
// scan of the raw SQL text, and it is a case-insensitive PREFIX match on
// common.InternalTablePrefix rather than an exact-name comparison: the
// "__marmot__" prefix already means "marmot internal" everywhere else in this
// codebase (the CDC skip in db/preupdate_hook.go, the SHOW TABLES /
// INFORMATION_SCHEMA filters), so this protects that whole class of hidden
// tables, not just the one that exists today. It is NOT a copy of the SQLite
// "name NOT LIKE '__marmot__%'" filters used elsewhere: SQLite's LIKE treats
// "_" as a single-character wildcard, so that pattern is broader than it
// looks and would be the wrong thing to port into Go as an exact match.
//
// This rule only ever sees client-facing SQL. Its one execution path is:
// query.Pipeline.Process -> query.Transpiler.Transpile -> this rule, and
// Pipeline.Process is called only from protocol.ParseStatement* and
// MySQLServer.handleStmtPrepare, both client connection handlers. Replicated
// writes (db/replication_engine.go) and the COMMIT handler that actually
// writes the claim row (db/autoinc_claim.go AutoIncClaimStore.ApplyClaims) apply
// directly against SQLite and never call this pipeline, so this rule cannot
// see - and cannot reject - either of them.
type ClaimTableGuardRule struct{}

func (r *ClaimTableGuardRule) Name() string { return "ClaimTableGuard" }

// Priority is 0 so this rule runs before every other transform rule: a
// rejection must be decided on the AST exactly as the client sent it, before
// anything else rewrites it.
func (r *ClaimTableGuardRule) Priority() int { return 0 }

// Transform refuses the statement if it writes to, or changes the schema of,
// one of Marmot's internal tables; otherwise it reports itself not
// applicable so the transpiler moves on to the next rule unchanged.
func (r *ClaimTableGuardRule) Transform(
	stmt sqlparser.Statement,
	_ []interface{},
	_ transform.SchemaProvider,
	_ string,
	_ transform.Serializer,
) ([]transform.TranspiledStatement, error) {
	table, command := protectedTargetTable(stmt)
	if table == "" {
		return nil, transform.ErrRuleNotApplicable
	}

	return nil, transform.NewCodedError(mysqlcode.ErrCodeTableAccessDenied,
		"%s command denied to user for table '%s'", command, table)
}

// protectedTargetTable returns the internal table name a write or DDL
// statement targets, and the MySQL command name for the error message. It
// returns "" for any statement that does not write to, or change the schema
// of, an internal table - including SELECT, which is never inspected here.
func protectedTargetTable(stmt sqlparser.Statement) (table, command string) {
	switch s := stmt.(type) {
	case sqlparser.DDLStatement:
		// Covers CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE TABLE and
		// RENAME TABLE. AffectedTables() includes RENAME TABLE's destination
		// name too, so renaming an ordinary table ONTO the claim table's name
		// is caught as well as renaming the claim table away.
		if name := firstInternalTable(s.AffectedTables()); name != "" {
			return name, strings.ToUpper(s.GetAction().ToString())
		}
	case *sqlparser.Insert:
		if s.Table == nil {
			return "", ""
		}
		if tn, ok := s.Table.Expr.(sqlparser.TableName); ok {
			if name := tn.Name.String(); common.IsInternalTableName(name) {
				command = "INSERT"
				if s.Action == sqlparser.ReplaceAct {
					command = "REPLACE"
				}
				return name, command
			}
		}
	case *sqlparser.Update:
		if name := firstInternalTableExpr(s.TableExprs); name != "" {
			return name, "UPDATE"
		}
	case *sqlparser.Delete:
		if name := firstInternalTable(s.Targets); name != "" {
			return name, "DELETE"
		}
		if name := firstInternalTableExpr(s.TableExprs); name != "" {
			return name, "DELETE"
		}
	}
	return "", ""
}

// firstInternalTable returns the first internal table name in names, or "".
func firstInternalTable(names sqlparser.TableNames) string {
	for _, tn := range names {
		if name := tn.Name.String(); common.IsInternalTableName(name) {
			return name
		}
	}
	return ""
}

// firstInternalTableExpr walks a FROM-list looking for a reference to an
// internal table, recursing into JOINs and parenthesized lists. A bare table
// reference resolves via AliasedTableExpr.Expr; a derived table (subquery)
// has no TableName and is skipped, since it cannot itself be the internal
// table.
func firstInternalTableExpr(exprs []sqlparser.TableExpr) string {
	for _, expr := range exprs {
		if name := internalTableInExpr(expr); name != "" {
			return name
		}
	}
	return ""
}

func internalTableInExpr(expr sqlparser.TableExpr) string {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := e.Expr.(sqlparser.TableName); ok {
			if name := tn.Name.String(); common.IsInternalTableName(name) {
				return name
			}
		}
	case *sqlparser.JoinTableExpr:
		if name := internalTableInExpr(e.LeftExpr); name != "" {
			return name
		}
		return internalTableInExpr(e.RightExpr)
	case *sqlparser.ParenTableExpr:
		return firstInternalTableExpr(e.Exprs)
	}
	return ""
}
