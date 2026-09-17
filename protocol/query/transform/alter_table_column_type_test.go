package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestAlterTableEmitsWidthMarker pins the ALTER path. Before the collapse was
// shared, this rule ran only stripMySQLColumnType, which removes display widths
// and charsets but does NOT collapse the type and does NOT remove
// AUTO_INCREMENT - so "ALTER TABLE t ADD COLUMN x INT AUTO_INCREMENT" reached
// SQLite carrying a keyword it cannot parse, and no width was ever recorded for
// a column added after the table was created.
//
// Mutation: drop the collapseIntegerTypeWithMarker call from any of the three
// arms; that arm's row fires.
func TestAlterTableEmitsWidthMarker(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		column string
		want   string
	}{
		{"ADD COLUMN INT AUTO_INCREMENT", "ALTER TABLE t ADD COLUMN x INT AUTO_INCREMENT", "x", "INTEGER /*M:32a*/"},
		{"ADD COLUMN SMALLINT UNSIGNED", "ALTER TABLE t ADD COLUMN q SMALLINT UNSIGNED", "q", "INTEGER /*M:16u*/"},
		{"MODIFY COLUMN MEDIUMINT", "ALTER TABLE t MODIFY COLUMN n MEDIUMINT", "n", "INTEGER /*M:24*/"},
		// CHANGE renames as well as retypes; the marker must follow the NEW name.
		{"CHANGE COLUMN renames and retypes", "ALTER TABLE t CHANGE COLUMN old new TINYINT UNSIGNED", "new", "INTEGER /*M:8u*/"},
		// BIGINT keeps the existing 64-bit path and is deliberately unmarked.
		{"ADD COLUMN BIGINT is unmarked", "ALTER TABLE t ADD COLUMN big BIGINT AUTO_INCREMENT", "big", "INTEGER"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			rule := &AlterTableColumnTypeRule{}
			if _, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
				t.Fatalf("Transform returned %v, want ErrRuleNotApplicable", err)
			}

			got, ok := alteredColumnType(stmt, tc.column)
			if !ok {
				t.Fatalf("column %q not found in the altered statement", tc.column)
			}
			if got != tc.want {
				t.Errorf("declared type = %q, want %q", got, tc.want)
			}

			// AUTO_INCREMENT must be gone from the AST, or SQLite rejects the
			// statement outright.
			// Mutation: keep Options.Autoincrement set.
			serialized := (&SQLiteSerializer{}).Serialize(stmt)
			if strings.Contains(strings.ToUpper(serialized), "AUTO_INCREMENT") {
				t.Errorf("serialized ALTER still carries AUTO_INCREMENT, which SQLite cannot parse:\n%s", serialized)
			}
		})
	}
}

// alteredColumnType returns the declared type of one column in an ALTER.
func alteredColumnType(stmt sqlparser.Statement, column string) (string, bool) {
	alter, ok := stmt.(*sqlparser.AlterTable)
	if !ok {
		return "", false
	}
	for _, opt := range alter.AlterOptions {
		switch o := opt.(type) {
		case *sqlparser.AddColumns:
			for _, col := range o.Columns {
				if col.Name.String() == column {
					return col.Type.Type, true
				}
			}
		case *sqlparser.ModifyColumn:
			if o.NewColDefinition != nil && o.NewColDefinition.Name.String() == column {
				return o.NewColDefinition.Type.Type, true
			}
		case *sqlparser.ChangeColumn:
			if o.NewColDefinition != nil && o.NewColDefinition.Name.String() == column {
				return o.NewColDefinition.Type.Type, true
			}
		}
	}
	return "", false
}
