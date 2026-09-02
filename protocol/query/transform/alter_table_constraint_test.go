package transform

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAlterTableConstraintRule_Name(t *testing.T) {
	rule := &AlterTableConstraintRule{}
	if rule.Name() != "AlterTableConstraint" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "AlterTableConstraint")
	}
}

func TestAlterTableConstraintRule_Priority(t *testing.T) {
	rule := &AlterTableConstraintRule{}
	if rule.Priority() != 10 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 10)
	}
}

// TestAlterTableConstraintRule_AddUnique verifies MySQL's "ALTER TABLE t ADD [CONSTRAINT
// name] UNIQUE (cols)" - which has no SQLite equivalent as an ALTER TABLE option - is
// rewritten into a standalone CREATE UNIQUE INDEX statement.
func TestAlterTableConstraintRule_AddUnique(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantSQL   string
		wantStmts int
	}{
		{
			name:      "named constraint, unquoted identifier",
			input:     "ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)",
			wantSQL:   `CREATE UNIQUE INDEX "uq_email" ON "users" ("email")`,
			wantStmts: 1,
		},
		{
			name:      "named constraint, backtick-quoted identifier",
			input:     "ALTER TABLE users ADD CONSTRAINT `uq_email` UNIQUE (email)",
			wantSQL:   `CREATE UNIQUE INDEX "uq_email" ON "users" ("email")`,
			wantStmts: 1,
		},
		{
			name:      "named constraint, hyphenated backtick-quoted identifier",
			input:     "ALTER TABLE users ADD CONSTRAINT `unique-user-email` UNIQUE (email)",
			wantSQL:   `CREATE UNIQUE INDEX "unique-user-email" ON "users" ("email")`,
			wantStmts: 1,
		},
		{
			name:      "named constraint, multi-column",
			input:     "ALTER TABLE users ADD CONSTRAINT uq_email_name UNIQUE (email, name)",
			wantSQL:   `CREATE UNIQUE INDEX "uq_email_name" ON "users" ("email", "name")`,
			wantStmts: 1,
		},
		{
			name:      "ADD UNIQUE INDEX with explicit name (no CONSTRAINT keyword)",
			input:     "ALTER TABLE users ADD UNIQUE INDEX uq_email (email)",
			wantSQL:   `CREATE UNIQUE INDEX "uq_email" ON "users" ("email")`,
			wantStmts: 1,
		},
		{
			name:      "ADD UNIQUE with no name at all",
			input:     "ALTER TABLE users ADD UNIQUE (email)",
			wantSQL:   `CREATE UNIQUE INDEX "users_email_unique" ON "users" ("email")`,
			wantStmts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &AlterTableConstraintRule{}
			results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})
			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}

			if len(results) != tt.wantStmts {
				t.Fatalf("statement count = %d, want %d (statements: %+v)", len(results), tt.wantStmts, results)
			}
			if results[0].SQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", results[0].SQL, tt.wantSQL)
			}
		})
	}
}

// TestAlterTableConstraintRule_MixedOptions verifies that when ADD CONSTRAINT UNIQUE is
// combined with other alter options (e.g. ADD COLUMN) in the same statement, the
// non-constraint options remain in a (still valid) ALTER TABLE statement and the unique
// constraint is emitted as an additional CREATE UNIQUE INDEX statement.
func TestAlterTableConstraintRule_MixedOptions(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN age INT, ADD CONSTRAINT uq_email UNIQUE (email)"
	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &AlterTableConstraintRule{}
	results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 statements, got %d: %+v", len(results), results)
	}

	wantAlter := "alter table users add column age INT"
	if results[0].SQL != wantAlter {
		t.Errorf("first statement = %q, want %q", results[0].SQL, wantAlter)
	}

	wantIndex := `CREATE UNIQUE INDEX "uq_email" ON "users" ("email")`
	if results[1].SQL != wantIndex {
		t.Errorf("second statement = %q, want %q", results[1].SQL, wantIndex)
	}
}

// TestAlterTableConstraintRule_NotApplicable verifies the rule is a no-op for
// statements without a UNIQUE constraint addition, so plain column changes keep
// going through the default serializer path unchanged.
func TestAlterTableConstraintRule_NotApplicable(t *testing.T) {
	tests := []string{
		"ALTER TABLE users ADD COLUMN age INT",
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users RENAME COLUMN age TO years",
		"SELECT * FROM users",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &AlterTableConstraintRule{}
			_, err = rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})
			if err != ErrRuleNotApplicable {
				t.Errorf("Transform() err = %v, want ErrRuleNotApplicable", err)
			}
		})
	}
}
