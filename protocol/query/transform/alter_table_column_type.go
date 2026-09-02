package transform

import (
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

	for _, opt := range alter.AlterOptions {
		switch o := opt.(type) {
		case *sqlparser.AddColumns:
			for _, col := range o.Columns {
				stripMySQLColumnType(col.Type)
			}
		case *sqlparser.ModifyColumn:
			if o.NewColDefinition != nil {
				stripMySQLColumnType(o.NewColDefinition.Type)
			}
		case *sqlparser.ChangeColumn:
			if o.NewColDefinition != nil {
				stripMySQLColumnType(o.NewColDefinition.Type)
			}
		}
	}

	return nil, ErrRuleNotApplicable
}
