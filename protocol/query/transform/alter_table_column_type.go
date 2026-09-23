package transform

import (
	"strings"

	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
	"vitess.io/vitess/go/vt/sqlparser"
)

// AlterTableColumnTypeRule strips MySQL-specific column type attributes - CHARACTER SET,
// COLLATE, COMMENT, and integer display widths - from ALTER TABLE ADD COLUMN, MODIFY
// COLUMN, and CHANGE COLUMN definitions. SQLite's column type syntax doesn't support any
// of these, and without stripping them SQLite's PREPARE step rejects the statement (e.g.
// "ADD COLUMN c VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci").
//
// CreateTableRule already strips the same attributes for CREATE TABLE columns; both rules
// share the stripMySQLColumnType helper (table_utils.go) rather than duplicating the logic.
//
// This rule always mutates the AST in place and returns ErrRuleNotApplicable, deferring
// serialization to AlterTableConstraintRule (for statements that also add a constraint or
// index) or to the transpiler's default serialization pass otherwise - the same pattern
// IntTypeRule uses for CREATE TABLE column types. It must run before AlterTableConstraintRule
// (lower priority) so the stripped columns are visible whichever rule ends up serializing.
type AlterTableColumnTypeRule struct{}

func (r *AlterTableColumnTypeRule) Name() string {
	return "AlterTableColumnType"
}

func (r *AlterTableColumnTypeRule) Priority() int {
	return 8
}

func (r *AlterTableColumnTypeRule) Transform(stmt sqlparser.Statement, params []interface{}, schema SchemaProvider, database string, serializer Serializer) ([]TranspiledStatement, error) {
	alter, ok := stmt.(*sqlparser.AlterTable)
	if !ok {
		return nil, ErrRuleNotApplicable
	}

	// A table-level "AUTO_INCREMENT=N" rides as its own alter_option (a bare
	// sqlparser.TableOptions), separate from whichever option retypes the
	// column - e.g. "ALTER TABLE t MODIFY id INT AUTO_INCREMENT, AUTO_INCREMENT=N".
	// Order between them in AlterOptions is not guaranteed, so this is a
	// dedicated pass rather than a case in the switch below.
	floor := uint64(0)
	sawTableOptions := false
	declaresFloor := false
	keptOptions := make([]sqlparser.AlterOption, 0, len(alter.AlterOptions))
	for _, opt := range alter.AlterOptions {
		if tableOpts, ok := opt.(sqlparser.TableOptions); ok {
			floor = autoIncFloorFromOptions(tableOpts)
			declaresFloor = declaresFloor || hasAutoIncOption(tableOpts)
			sawTableOptions = true
			continue
		}
		keptOptions = append(keptOptions, opt)
	}

	// The declared floor has exactly one place to go: the width marker of a
	// narrow AUTO_INCREMENT column this same ALTER defines, which is where
	// DDL-time seeding reads it from (db/autoinc_seed.go autoIncSeedFloor).
	// With no such column the floor would be dropped, and MySQL never ignores
	// it: a client reserving the ids below N would later be handed allocator
	// ids inside that range. So the statement is refused rather than run
	// without it - including the option-only "ALTER TABLE t AUTO_INCREMENT=N".
	if declaresFloor && !takesAutoIncFloor(keptOptions) {
		return nil, NewCodedError(ErrCodeNotSupportedYet,
			"This version of MySQL doesn't yet support 'ALTER TABLE `%s` AUTO_INCREMENT=N without a narrow AUTO_INCREMENT column definition in the same statement'",
			alter.Table.Name.String())
	}

	// SQLite's ALTER TABLE grammar has no table options at all, so a
	// "AUTO_INCREMENT=N" (or any other table option) riding on the statement
	// reaches SQLite as a bare ", AUTO_INCREMENT=800" and is a syntax error.
	// They are dropped here for the same reason CREATE TABLE drops
	// TableSpec.Options (protocol/query/transform/create_table.go); a declared
	// floor is carried into the column's width marker below.
	//
	// An ALTER whose only options are other table options has nothing left to
	// serialize, so it is left untouched rather than rewritten into invalid
	// SQL.
	if sawTableOptions && len(keptOptions) > 0 {
		alter.AlterOptions = keptOptions
	}

	for _, colType := range alteredColumnTypes(alter.AlterOptions) {
		stripMySQLColumnType(colType)
		collapseIntegerTypeWithMarker(colType, floor)
	}

	return nil, ErrRuleNotApplicable
}

// alteredColumnTypes returns the type of every column an ALTER defines: added
// columns, and the new definition of a MODIFY or CHANGE (CHANGE renames as well
// as retypes, so a marker follows the new name).
func alteredColumnTypes(opts []sqlparser.AlterOption) []*sqlparser.ColumnType {
	var types []*sqlparser.ColumnType
	for _, opt := range opts {
		switch o := opt.(type) {
		case *sqlparser.AddColumns:
			for _, col := range o.Columns {
				types = append(types, col.Type)
			}
		case *sqlparser.ModifyColumn:
			if o.NewColDefinition != nil {
				types = append(types, o.NewColDefinition.Type)
			}
		case *sqlparser.ChangeColumn:
			if o.NewColDefinition != nil {
				types = append(types, o.NewColDefinition.Type)
			}
		}
	}
	return types
}

// takesAutoIncFloor reports whether any column the ALTER defines is a narrow
// AUTO_INCREMENT integer, the only column whose marker carries a floor
// (collapseIntegerTypeWithMarker). It must run before that function, which
// clears the AUTO_INCREMENT attribute.
func takesAutoIncFloor(opts []sqlparser.AlterOption) bool {
	for _, colType := range alteredColumnTypes(opts) {
		if colType == nil || colType.Options == nil || !colType.Options.Autoincrement {
			continue
		}
		if _, narrow := intmarker.BitsForType(strings.ToUpper(colType.Type)); narrow {
			return true
		}
	}
	return false
}
