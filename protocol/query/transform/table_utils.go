package transform

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

// stripMySQLColumnType removes MySQL-specific column type attributes that have no SQLite
// equivalent, in place: display widths on integer types, the COLLATE/COMMENT column
// options, and the CHARACTER SET/COLLATE charset clause. Shared by CreateTableRule (CREATE
// TABLE columns) and AlterTableColumnTypeRule (ALTER TABLE ADD/MODIFY/CHANGE COLUMN).
func stripMySQLColumnType(colType *sqlparser.ColumnType) {
	if colType == nil {
		return
	}

	// Strip display widths from integer types: INTEGER(20) → INTEGER
	if isIntegerType(colType.Type) {
		colType.Length = nil
	}

	if colType.Options != nil {
		// Strip MySQL-specific COLLATE (SQLite only supports NOCASE, BINARY, RTRIM)
		colType.Options.Collate = ""
		// Strip MySQL-specific COMMENT (not supported in SQLite column definitions)
		colType.Options.Comment = nil
	}

	// Strip charset (SQLite doesn't use MySQL charsets)
	colType.Charset = sqlparser.ColumnCharset{}
}

// HasJoin checks if any TableExpr in the slice is a JoinTableExpr.
func HasJoin(tableExprs sqlparser.TableExprs) bool {
	for _, expr := range tableExprs {
		if _, isJoin := expr.(*sqlparser.JoinTableExpr); isJoin {
			return true
		}
	}
	return false
}

// FindTableByAlias searches for a table in TableExprs by alias.
// If targetAlias is empty, it returns the first table found.
// Returns the table name, alias, and an error if not found.
func FindTableByAlias(tableExprs sqlparser.TableExprs, targetAlias string) (tableName, alias string, err error) {
	for _, expr := range tableExprs {
		name, foundAlias, found := searchTableExpr(expr, targetAlias)
		if found {
			return name, foundAlias, nil
		}
	}
	return "", "", fmt.Errorf("unable to find table for alias %q", targetAlias)
}

// searchTableExpr recursively searches a TableExpr for a table matching the target alias.
// If targetAlias is empty, returns the first table found.
func searchTableExpr(expr sqlparser.TableExpr, targetAlias string) (tableName, alias string, found bool) {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := sqlparser.GetTableName(e.Expr).String()
		alias := e.As.String()
		if alias == "" {
			alias = tableName
		}

		if targetAlias == "" || targetAlias == alias {
			return tableName, alias, true
		}

	case *sqlparser.JoinTableExpr:
		if tableName, alias, found := searchTableExpr(e.LeftExpr, targetAlias); found {
			return tableName, alias, true
		}
		if tableName, alias, found := searchTableExpr(e.RightExpr, targetAlias); found {
			return tableName, alias, true
		}
	}

	return "", "", false
}
