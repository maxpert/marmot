package transform

import (
	"errors"
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

// TestAlterTableAutoIncrementOptionBecomesMarkerFloor pins the ALTER side of
// the AUTO_INCREMENT=N floor: "ALTER TABLE t ADD COLUMN id INT AUTO_INCREMENT,
// AUTO_INCREMENT=5000" must carry N-1 into the new column's marker exactly
// like the CREATE TABLE path does, even though the AUTO_INCREMENT=N option
// and the column definition are two separate, unordered AlterOptions.
//
// Mutation: read alter.AlterOptions for the floor only inside the switch's
// *sqlparser.AddColumns case instead of in the dedicated pre-pass; a floor
// option that is not exactly the first AlterOption is missed.
func TestAlterTableAutoIncrementOptionBecomesMarkerFloor(t *testing.T) {
	stmt, err := sqlparser.NewTestParser().Parse(
		"ALTER TABLE t ADD COLUMN id INT AUTO_INCREMENT, AUTO_INCREMENT=5000")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := (&AlterTableColumnTypeRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
		t.Fatalf("Transform returned %v, want ErrRuleNotApplicable", err)
	}
	got, ok := alteredColumnType(stmt, "id")
	if !ok {
		t.Fatal("column id not found")
	}
	want := "INTEGER /*M:32a:4999*/"
	if got != want {
		t.Errorf("declared type = %q, want %q", got, want)
	}
}

// TestAlterTableDropsTableOptionsSQLiteCannotParse pins the other half of the
// same floor: the AUTO_INCREMENT=N option must not merely be READ, it
// must also be REMOVED from the statement. SQLite's ALTER TABLE grammar takes
// no table options, so one left in place arrives as a bare ", AUTO_INCREMENT=800"
// and the whole DDL fails with "near \",\": syntax error" - which is exactly
// how the cluster test TestAutoIncSeed_AlterAddColumnWithFloorOption failed
// before this.
//
// Mutation: keep every AlterOption (drop the keptOptions rewrite in
// AlterTableColumnTypeRule.Transform). The serialized statement then still
// carries AUTO_INCREMENT=800 and the first assertion fires.
func TestAlterTableDropsTableOptionsSQLiteCannotParse(t *testing.T) {
	stmt, err := sqlparser.NewTestParser().Parse(
		"ALTER TABLE t ADD COLUMN cnt INT AUTO_INCREMENT, AUTO_INCREMENT=800")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := (&AlterTableColumnTypeRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
		t.Fatalf("Transform returned %v, want ErrRuleNotApplicable", err)
	}

	// Vitess renders the table option as "AUTO_INCREMENT 800" (no equals
	// sign), and the rule has already stripped the keyword from the column
	// definition, so any occurrence left in the serialized statement is the
	// table option SQLite cannot parse.
	serialized := (&SQLiteSerializer{}).Serialize(stmt)
	if strings.Contains(strings.ToUpper(serialized), "AUTO_INCREMENT") {
		t.Errorf("serialized ALTER still carries a table option SQLite cannot parse:\n%s", serialized)
	}

	// The floor is not lost with the option: it rides into the marker, which
	// is where DDL-time seeding reads it from.
	got, ok := alteredColumnType(stmt, "cnt")
	if !ok {
		t.Fatal("column cnt not found")
	}
	if want := "INTEGER /*M:32a:799*/"; got != want {
		t.Errorf("declared type = %q, want %q", got, want)
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

// TestAlterTableRefusesAFloorNoColumnTakes: MySQL never ignores
// AUTO_INCREMENT=N on an ALTER. The floor can only ride into the marker of a
// narrow AUTO_INCREMENT column the same ALTER defines, so every other shape
// is refused with ER_NOT_SUPPORTED_YET (1235, SQLSTATE 42000: the server
// understands the statement and does not implement this form) rather than run
// with the floor silently dropped.
//
// Mutation: remove the declaresFloor refusal in AlterTableColumnTypeRule.Transform.
// Each case then returns ErrRuleNotApplicable and "the floor was dropped
// instead of refused" fires.
func TestAlterTableRefusesAFloorNoColumnTakes(t *testing.T) {
	for _, sql := range []string{
		"ALTER TABLE t AUTO_INCREMENT=800",
		"ALTER TABLE t ADD COLUMN note TEXT, AUTO_INCREMENT=5000",
		"ALTER TABLE t ADD COLUMN big BIGINT AUTO_INCREMENT, AUTO_INCREMENT=5000",
		"ALTER TABLE t ADD COLUMN plain INT, AUTO_INCREMENT=5000",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			_, err = (&AlterTableColumnTypeRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
			var coded *CodedError
			if !errors.As(err, &coded) {
				t.Fatalf("the floor was dropped instead of refused: Transform returned %v", err)
			}
			if coded.Code != ErrCodeNotSupportedYet {
				t.Errorf("code = %d, want %d (ER_NOT_SUPPORTED_YET)", coded.Code, ErrCodeNotSupportedYet)
			}
		})
	}
}
