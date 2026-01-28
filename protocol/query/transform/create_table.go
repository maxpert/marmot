package transform

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// CreateTableRule transforms MySQL CREATE TABLE to SQLite-compatible form:
//   - Strips MySQL-specific column options (COLLATE, COMMENT)
//   - Strips MySQL-specific table options (ENGINE, CHARSET, COLLATE)
//   - Extracts non-primary, non-unique KEY/INDEX definitions into separate CREATE INDEX statements
//   - Keeps PRIMARY KEY definitions in the CREATE TABLE
//   - Keeps UNIQUE KEY definitions in the CREATE TABLE (converted to CONSTRAINT by serializer)
//   - Strips column length specifications: KEY idx (col(191)) → col
//   - Skips FULLTEXT/SPATIAL indexes (passed through to serializer)
type CreateTableRule struct {
}

func (r *CreateTableRule) Name() string {
	return "CreateTable"
}

func (r *CreateTableRule) Priority() int {
	return 10
}

func (r *CreateTableRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	create, ok := stmt.(*sqlparser.CreateTable)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	if create.TableSpec == nil {
		return nil, ErrRuleNotApplicable
	}

	tableName := create.Table.Name.String()
	var indexesToExtract []*sqlparser.IndexDefinition
	var remainingIndexes []*sqlparser.IndexDefinition

	for _, idx := range create.TableSpec.Indexes {
		if r.shouldExtractIndex(idx) {
			indexesToExtract = append(indexesToExtract, idx)
		} else {
			remainingIndexes = append(remainingIndexes, idx)
		}
	}

	create.TableSpec.Indexes = remainingIndexes

	// Clear MySQL-specific table options (ENGINE, CHARSET, COLLATE)
	create.TableSpec.Options = nil

	// Process columns: strip MySQL-specific options
	for _, col := range create.TableSpec.Columns {
		if col.Type != nil {
			// Strip display widths from integer types: INTEGER(20) → INTEGER
			if isIntegerType(col.Type.Type) {
				col.Type.Length = nil
			}
			// Strip MySQL-specific column options
			if col.Type.Options != nil {
				// Strip MySQL-specific COLLATE (SQLite only supports NOCASE, BINARY, RTRIM)
				col.Type.Options.Collate = ""
				// Strip MySQL-specific COMMENT (not supported in SQLite column definitions)
				col.Type.Options.Comment = nil
			}
			// Also strip charset (SQLite doesn't use MySQL charsets)
			col.Type.Charset = sqlparser.ColumnCharset{}
		}
	}

	// Build results
	var results []TranspiledStatement

	mainSQL := serializer.Serialize(create)
	results = append(results, TranspiledStatement{SQL: mainSQL, Params: params})

	// Add CREATE INDEX statements (no params for DDL)
	for _, idx := range indexesToExtract {
		indexSQL := r.buildCreateIndex(tableName, idx)
		results = append(results, TranspiledStatement{SQL: indexSQL, Params: nil})
	}

	return results, nil
}

func (r *CreateTableRule) shouldExtractIndex(idx *sqlparser.IndexDefinition) bool {
	info := idx.Info
	if info == nil {
		return false
	}

	if info.Type == sqlparser.IndexTypePrimary {
		return false
	}

	if info.Type == sqlparser.IndexTypeFullText || info.Type == sqlparser.IndexTypeSpatial {
		return false
	}

	// Keep UNIQUE indexes inline (don't extract them)
	if info.IsUnique() {
		return false
	}

	return true
}

// buildCreateIndex generates a SQLite CREATE INDEX statement for the given index definition.
// Precondition: idx.Info must not be nil (enforced by shouldExtractIndex filter).
func (r *CreateTableRule) buildCreateIndex(tableName string, idx *sqlparser.IndexDefinition) string {
	if idx.Info == nil {
		return ""
	}

	var sb strings.Builder

	sb.WriteString("CREATE ")

	if idx.Info.IsUnique() {
		sb.WriteString("UNIQUE ")
	}

	sb.WriteString("INDEX IF NOT EXISTS ")

	indexName := idx.Info.Name.String()
	sb.WriteString(indexName)

	sb.WriteString(" ON ")
	sb.WriteString(tableName)

	sb.WriteString(" (")
	for i, col := range idx.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		colName := col.Column.String()
		sb.WriteString(colName)
	}
	sb.WriteString(")")

	return sb.String()
}
