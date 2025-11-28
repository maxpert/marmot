package rules

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// CreateTableRule transforms MySQL CREATE TABLE syntax to SQLite-compatible syntax
// Handles:
// - AUTO_INCREMENT → removed (SQLite INTEGER PRIMARY KEY auto-increments implicitly)
// - UNSIGNED → removed (SQLite doesn't support UNSIGNED)
// - ENGINE = innodb → removed (SQLite has no storage engines)
// - MySQL comments /*! ... */ → removed
type CreateTableRule struct{}

func (r *CreateTableRule) Name() string  { return "CreateTable" }
func (r *CreateTableRule) Priority() int { return 5 } // Run early, before other rules

func (r *CreateTableRule) ApplyAST(stmt sqlparser.Statement) (sqlparser.Statement, bool, error) {
	create, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return stmt, false, nil
	}

	modified := false

	if create.TableSpec != nil {
		// Process columns
		for _, col := range create.TableSpec.Columns {
			if col.Type != nil {
				// Remove UNSIGNED
				if col.Type.Unsigned {
					col.Type.Unsigned = false
					modified = true
				}

				// Remove AUTO_INCREMENT
				// SQLite's INTEGER PRIMARY KEY automatically becomes ROWID alias
				if col.Type.Options != nil && col.Type.Options.Autoincrement {
					col.Type.Options.Autoincrement = false
					modified = true
				}
			}
		}

		// Clear table options (ENGINE, CHARSET, etc.)
		if len(create.TableSpec.Options) > 0 {
			create.TableSpec.Options = nil
			modified = true
		}
	}

	if !modified {
		return stmt, false, nil
	}

	return create, true, nil
}

func (r *CreateTableRule) ApplyPattern(sql string) (string, bool, error) {
	// Remove MySQL comment hints like /*! ENGINE = innodb */
	// These are the only pattern-based transforms since AST handles the rest
	upperSQL := strings.ToUpper(sql)
	if !strings.Contains(upperSQL, "/*!") {
		return sql, false, nil
	}

	// Remove /*! ... */ comments
	result := sql
	for {
		start := strings.Index(result, "/*!")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "*/")
		if end == -1 {
			break
		}
		result = result[:start] + result[start+end+2:]
	}

	// Clean up multiple spaces
	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}

	return strings.TrimSpace(result), result != sql, nil
}
