package transform

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
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

// collapseIntegerTypeWithMarker rewrites a MySQL integer column type into the
// single type SQLite has, carrying the declared width as a marker comment, and
// strips the modifiers SQLite rejects. It reports whether it changed anything.
//
// floor is the base floor a table-level AUTO_INCREMENT=N option declared (see
// autoIncFloorFromOptions); it is encoded into the marker only when this
// column is the one being declared AUTO_INCREMENT, so an unrelated narrow
// column never carries another column's floor.
//
// Shared by CREATE TABLE (IntTypeRule) and ALTER TABLE ADD/MODIFY/CHANGE COLUMN
// (AlterTableColumnTypeRule). Before it was shared, ALTER only ran
// stripMySQLColumnType, which does not collapse the type and does not remove
// AUTO_INCREMENT, so "ALTER TABLE t ADD COLUMN x INT AUTO_INCREMENT" reached
// SQLite with a keyword it cannot parse.
//
// The stored token must stay exactly "INTEGER": only that spelling makes a
// PRIMARY KEY an alias of the rowid, and "INT PRIMARY KEY" does not, which
// would make LAST_INSERT_ID() report an unrelated internal rowid. BIGINT and
// non-integer types get no marker, so their DDL text is byte-identical to what
// it was before markers existed.
func collapseIntegerTypeWithMarker(colType *sqlparser.ColumnType, floor uint64) bool {
	if colType == nil {
		return false
	}

	upperType := strings.ToUpper(colType.Type)
	if !isIntegerType(upperType) {
		return false
	}

	// Capture the declared width BEFORE the strips below erase it.
	autoInc := colType.Options != nil && colType.Options.Autoincrement
	marker := ""
	if bits, narrow := intmarker.BitsForType(upperType); narrow {
		attrs := intmarker.Attributes{
			Bits:            bits,
			Unsigned:        colType.Unsigned,
			ExplicitAutoInc: autoInc,
		}
		if autoInc {
			attrs.AutoIncFloor = floor
		}
		marker = intmarker.Encode(attrs)
	}

	modified := false
	if colType.Options != nil && colType.Options.Autoincrement {
		colType.Options.Autoincrement = false
		modified = true
	}
	if colType.Unsigned {
		colType.Unsigned = false
		modified = true
	}
	if colType.Zerofill {
		colType.Zerofill = false
		modified = true
	}

	want := "INTEGER"
	if marker != "" {
		// Spaced, matching the form verified against sqlite3 in the design:
		// "id INTEGER /*M:32a*/ PRIMARY KEY".
		want = "INTEGER " + marker
	}
	if colType.Type != want {
		colType.Type = want
		modified = true
	}
	return modified
}

// hasAutoIncOption reports whether a table-option list declares
// AUTO_INCREMENT=N, whatever N is.
func hasAutoIncOption(opts sqlparser.TableOptions) bool {
	for _, opt := range opts {
		if opt != nil && strings.EqualFold(opt.Name, "auto_increment") {
			return true
		}
	}
	return false
}

// autoIncFloorFromOptions returns the base floor a table's declared
// AUTO_INCREMENT=N option implies: N-1, the id MySQL semantics treat as
// already issued just before the next insert. It returns 0 - "no declared
// floor" - when the option is absent, non-numeric, or N itself is 0: a
// client writing AUTO_INCREMENT=0 is not asking for a floor below the first
// id.
//
// Read from a CREATE TABLE's TableSpec.Options (an sqlparser.TableOptions
// directly) or from one ALTER TABLE alter_option of that same type - MySQL
// accepts "ALTER TABLE t ... , AUTO_INCREMENT=N" as an alter option alongside
// a column change in the same statement.
func autoIncFloorFromOptions(opts sqlparser.TableOptions) uint64 {
	for _, opt := range opts {
		if opt == nil || opt.Value == nil || !strings.EqualFold(opt.Name, "auto_increment") {
			continue
		}
		n, err := strconv.ParseUint(opt.Value.Val, 10, 64)
		if err != nil || n == 0 {
			return 0
		}
		return n - 1
	}
	return 0
}
