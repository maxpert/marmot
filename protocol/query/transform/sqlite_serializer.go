package transform

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// SQLiteSerializer converts a Vitess MySQL AST to SQLite-compatible SQL.
// It uses a custom node formatter to handle SQLite-specific syntax differences:
//   - Named parameters (:v1) → positional (?)
//   - INSERT IGNORE → INSERT OR IGNORE
//   - Escape sequences (\') → (”) for string literals
//   - Remove MySQL-specific hints (FORCE INDEX, USE INDEX, FOR UPDATE)
//   - Convert MySQL LIMIT offset,count → LIMIT count OFFSET offset
type SQLiteSerializer struct {
	extractedIndexes []string
	conflictColumns  []string
}

// Serialize converts a Vitess AST to SQLite SQL
func (s *SQLiteSerializer) Serialize(stmt sqlparser.Statement) string {
	buf := sqlparser.NewTrackedBuffer(s.nodeFormatter)
	buf.Myprintf("%v", stmt)
	return buf.String()
}

// SetConflictColumns sets the conflict target columns for INSERT ON CONFLICT
func (s *SQLiteSerializer) SetConflictColumns(columns []string) {
	s.conflictColumns = columns
}

// nodeFormatter is called for each node during serialization.
// It handles SQLite-specific conversions that differ from MySQL.
func (s *SQLiteSerializer) nodeFormatter(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch n := node.(type) {
	case *sqlparser.Insert:
		s.formatInsert(buf, n)
	case *sqlparser.Select:
		s.formatSelect(buf, n)
	case *sqlparser.Update:
		s.formatUpdate(buf, n)
	case *sqlparser.Delete:
		s.formatDelete(buf, n)
	case *sqlparser.IndexDefinition:
		s.formatIndexDefinition(buf, n)
	case *sqlparser.Argument:
		// Convert named parameters (:v1) to positional (?)
		buf.WriteString("?")
	case *sqlparser.Literal:
		// Handle string literal escaping: \' → ''
		if n.Type == sqlparser.StrVal {
			s.formatStringLiteral(buf, []byte(n.Val))
		} else {
			n.Format(buf)
		}
	case *sqlparser.IndexHints:
		// Skip MySQL-specific index hints (FORCE INDEX, USE INDEX, IGNORE INDEX)
		// SQLite doesn't support them
	case *sqlparser.Where:
		// Format WHERE with uppercase keyword
		buf.WriteString(" WHERE ")
		buf.Myprintf("%v", n.Expr)
	case sqlparser.OrderBy:
		// Format ORDER BY with uppercase keywords
		buf.WriteString(" ORDER BY ")
		for i, order := range n {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", order)
		}
	case *sqlparser.GroupBy:
		// Format GROUP BY with uppercase keywords
		buf.WriteString(" GROUP BY ")
		for i, expr := range n.Exprs {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", expr)
		}
	case *sqlparser.Order:
		// Format individual order expressions (strip ASC/DESC as Vitess adds them)
		buf.Myprintf("%v", n.Expr)
		if n.Direction == sqlparser.DescOrder {
			buf.WriteString(" DESC")
		}
		// Don't write ASC explicitly as it's the default
	case *sqlparser.CountStar:
		// Format COUNT(*) with uppercase for consistency with tests
		buf.WriteString("COUNT(*)")
		if n.OverClause != nil {
			buf.WriteString(" OVER()")
		}
	case sqlparser.Values:
		// Format VALUES with uppercase keyword
		buf.WriteString("VALUES ")
		for i, tuple := range n {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", tuple)
		}
	default:
		// Use default Vitess formatting for all other nodes
		node.Format(buf)
	}
}

// formatIndexDefinition converts MySQL UNIQUE KEY syntax to SQLite CONSTRAINT UNIQUE syntax
func (s *SQLiteSerializer) formatIndexDefinition(buf *sqlparser.TrackedBuffer, n *sqlparser.IndexDefinition) {
	if n.Info == nil {
		n.Format(buf)
		return
	}

	// Don't modify PRIMARY KEY - use default formatting
	if n.Info.Type == sqlparser.IndexTypePrimary {
		n.Format(buf)
		return
	}

	// Convert UNIQUE KEY to CONSTRAINT ... UNIQUE for SQLite compatibility
	if n.Info.IsUnique() {
		buf.WriteString("CONSTRAINT ")
		buf.Myprintf("%v", n.Info.Name)
		buf.WriteString(" UNIQUE (")
		for i, col := range n.Columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", col.Column)
		}
		buf.WriteString(")")
		return
	}

	// For all other index types, use default formatting
	n.Format(buf)
}

// formatInsert handles INSERT statement conversion
func (s *SQLiteSerializer) formatInsert(buf *sqlparser.TrackedBuffer, n *sqlparser.Insert) {
	buf.Myprintf("%v", n.Comments)

	// Handle INSERT IGNORE → INSERT OR IGNORE
	if n.Ignore {
		buf.WriteString("INSERT OR IGNORE INTO ")
	} else {
		buf.WriteString("INSERT INTO ")
	}

	buf.Myprintf("%v", n.Table)

	if len(n.Columns) > 0 {
		buf.WriteString(" (")
		for i, col := range n.Columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v", col)
		}
		buf.WriteString(")")
	}

	buf.Myprintf(" %v", n.Rows)

	// Handle ON DUPLICATE KEY UPDATE → ON CONFLICT DO UPDATE
	if len(n.OnDup) > 0 {
		buf.WriteString(" ON CONFLICT (")
		for i, col := range s.conflictColumns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col)
		}
		buf.WriteString(") DO UPDATE SET ")

		for i, updateExpr := range n.OnDup {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.Myprintf("%v = %v", updateExpr.Name, updateExpr.Expr)
		}
	}
}

// formatSelect handles SELECT statement conversion
func (s *SQLiteSerializer) formatSelect(buf *sqlparser.TrackedBuffer, n *sqlparser.Select) {
	buf.Myprintf("%v", n.Comments)
	buf.WriteString("SELECT")

	if n.Distinct {
		buf.WriteString(" DISTINCT")
	}

	buf.Myprintf(" %v", n.SelectExprs)

	if len(n.From) > 0 {
		buf.WriteString(" FROM ")
		s.formatTableExprs(buf, n.From)
	}

	if n.Where != nil {
		buf.Myprintf("%v", n.Where)
	}

	if n.GroupBy != nil {
		buf.Myprintf("%v", n.GroupBy)
	}

	if n.Having != nil {
		buf.Myprintf("%v", n.Having)
	}

	if n.OrderBy != nil {
		buf.Myprintf("%v", n.OrderBy)
	}

	if n.Limit != nil {
		s.formatLimit(buf, n.Limit)
	}

	// Skip n.Lock (FOR UPDATE) - handled by Lock case in nodeFormatter
}

// formatUpdate handles UPDATE statement conversion
func (s *SQLiteSerializer) formatUpdate(buf *sqlparser.TrackedBuffer, n *sqlparser.Update) {
	buf.Myprintf("%v", n.Comments)
	buf.WriteString("UPDATE ")

	// Format table exprs without index hints
	s.formatTableExprs(buf, n.TableExprs)

	buf.Myprintf(" SET %v", n.Exprs)

	if n.Where != nil {
		buf.Myprintf("%v", n.Where)
	}

	if n.OrderBy != nil {
		buf.Myprintf("%v", n.OrderBy)
	}

	if n.Limit != nil {
		buf.Myprintf(" %v", n.Limit)
	}
}

// formatDelete handles DELETE statement conversion
func (s *SQLiteSerializer) formatDelete(buf *sqlparser.TrackedBuffer, n *sqlparser.Delete) {
	buf.Myprintf("%v", n.Comments)
	buf.WriteString("DELETE FROM ")

	s.formatTableExprs(buf, n.TableExprs)

	if n.Where != nil {
		buf.Myprintf("%v", n.Where)
	}

	if n.OrderBy != nil {
		buf.Myprintf("%v", n.OrderBy)
	}

	if n.Limit != nil {
		buf.Myprintf(" %v", n.Limit)
	}
}

// formatTableExprs formats table expressions, stripping MySQL-specific hints
func (s *SQLiteSerializer) formatTableExprs(buf *sqlparser.TrackedBuffer, exprs sqlparser.TableExprs) {
	for i, expr := range exprs {
		if i > 0 {
			buf.WriteString(", ")
		}
		s.formatTableExpr(buf, expr)
	}
}

// formatTableExpr formats a single table expression, removing index hints
func (s *SQLiteSerializer) formatTableExpr(buf *sqlparser.TrackedBuffer, expr sqlparser.TableExpr) {
	switch n := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		// Write table name
		buf.Myprintf("%v", n.Expr)

		// Skip n.Hints (FORCE INDEX, USE INDEX) - handled by IndexHints case

		// Write alias if present
		if !n.As.IsEmpty() {
			buf.Myprintf(" AS %v", n.As)
		}
	default:
		// JoinTableExpr, ParenTableExpr, etc. use default formatting
		buf.Myprintf("%v", n)
	}
}

// formatLimit handles MySQL-style LIMIT offset,count → SQLite LIMIT count OFFSET offset
func (s *SQLiteSerializer) formatLimit(buf *sqlparser.TrackedBuffer, limit *sqlparser.Limit) {
	if limit == nil {
		return
	}

	// Check if this is MySQL-style: LIMIT offset, count
	// Vitess parses this as: Offset=offset, Rowcount=count
	if limit.Offset != nil {
		// SQLite style: LIMIT count OFFSET offset
		buf.Myprintf(" LIMIT %v OFFSET %v", limit.Rowcount, limit.Offset)
	} else {
		// Simple LIMIT: LIMIT count
		buf.Myprintf(" LIMIT %v", limit.Rowcount)
	}
}

// formatStringLiteral converts MySQL string escaping to SQLite
func (s *SQLiteSerializer) formatStringLiteral(buf *sqlparser.TrackedBuffer, val []byte) {
	str := string(val)
	// Vitess always passes unquoted literal values
	// Escape single quotes for SQLite and add quotes
	str = strings.ReplaceAll(str, `'`, `''`)
	buf.WriteString("'")
	buf.WriteString(str)
	buf.WriteString("'")
}

// ExtractedIndexes returns CREATE INDEX statements extracted from CREATE TABLE
func (s *SQLiteSerializer) ExtractedIndexes() []string {
	return s.extractedIndexes
}

// Reset clears state for reuse
func (s *SQLiteSerializer) Reset() {
	s.extractedIndexes = nil
	s.conflictColumns = nil
}
