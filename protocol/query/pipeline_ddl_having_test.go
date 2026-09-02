package query

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestPipeline_PartialDDLRejected pins the LLDAP 0.6.3 regression: Vitess's DDL
// fallback path ignores a syntax error and returns a partially-parsed AST
// (e.g. "ALTER TABLE t ADD CONSTRAINT unique-user-email UNIQUE (email)"
// degrades to just "ALTER TABLE t" because an unquoted identifier cannot
// contain a hyphen). Forwarding that AST would silently truncate the
// statement; the pipeline must instead report a parse failure.
func TestPipeline_PartialDDLRejected(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	sql := "alter table users add CONSTRAINT unique-user-email UNIQUE (email)"
	ctx := NewContext(sql, nil)
	if err := pipeline.Process(ctx); err == nil {
		t.Fatalf("expected pipeline.Process to fail on partially-parsed DDL, got statements: %+v", ctx.Output.Statements)
	}
	if ctx.Output.IsValid {
		t.Errorf("ctx.Output.IsValid must be false when parsing failed")
	}
}

// TestPipeline_WellFormedDDLStillParses is the control for the above: the same
// statement with the constraint name properly backtick-quoted (valid MySQL
// syntax) must parse and transpile normally, not be rejected.
func TestPipeline_WellFormedDDLStillParses(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	sql := "alter table users add CONSTRAINT `unique-user-email` UNIQUE (email)"
	ctx := NewContext(sql, nil)
	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("pipeline.Process failed on well-formed DDL: %v", err)
	}
	want := `CREATE UNIQUE INDEX "unique-user-email" ON "users" ("email")`
	if len(ctx.Output.Statements) != 1 || ctx.Output.Statements[0].SQL != want {
		t.Fatalf("got statements %+v, want single statement %q", ctx.Output.Statements, want)
	}
}

// TestPipeline_AlterTableColumnRegressions locks in that plain ADD/DROP/RENAME
// COLUMN still pass through unchanged after introducing AlterTableConstraintRule.
func TestPipeline_AlterTableColumnRegressions(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		sql  string
		want string
	}{
		{"ALTER TABLE users ADD COLUMN age INT", "alter table users add column age INT"},
		{"ALTER TABLE users DROP COLUMN age", "alter table users drop column age"},
		{"ALTER TABLE users RENAME COLUMN age TO years", "alter table users rename column age to years"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}
			if len(ctx.Output.Statements) != 1 || ctx.Output.Statements[0].SQL != tt.want {
				t.Fatalf("got %+v, want single statement %q", ctx.Output.Statements, tt.want)
			}
		})
	}
}

// TestPipeline_SubqueryHavingExecutesInSQLite is the exact LLDAP 0.6.3
// COM_STMT_PREPARE statement that used to be corrupted by transpilation
// (HAVING inside the subquery was rewritten as WHERE, and SQLite's PREPARE
// rejected the result with "near \"WHERE\": syntax error"). This verifies the
// transpiled SQL both preserves the "?" placeholder and actually executes
// against a real SQLite database.
func TestPipeline_SubqueryHavingExecutesInSQLite(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	mysqlSQL := "SELECT `email`, `user_id` FROM `users` WHERE `email` IN " +
		"(SELECT `email` FROM `users` GROUP BY `email` HAVING COUNT(`email`) > ?) " +
		"ORDER BY `email` ASC, `user_id` ASC"

	ctx := NewContext(mysqlSQL, nil)
	ctx.ExtractLiterals = true
	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("pipeline.Process failed: %v", err)
	}
	if len(ctx.Output.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d: %+v", len(ctx.Output.Statements), ctx.Output.Statements)
	}
	transpiled := ctx.Output.Statements[0].SQL

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (email TEXT, user_id INTEGER)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	rows := []struct {
		email  string
		userID int
	}{
		{"dup@example.com", 1},
		{"dup@example.com", 2},
		{"unique@example.com", 3},
	}
	for _, r := range rows {
		if _, err := db.Exec("INSERT INTO users (email, user_id) VALUES (?, ?)", r.email, r.userID); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	result, err := db.Query(transpiled, 1)
	if err != nil {
		t.Fatalf("transpiled SQL failed to execute: %v\nSQL: %s", err, transpiled)
	}
	defer result.Close()

	var got []int
	for result.Next() {
		var email string
		var userID int
		if err := result.Scan(&email, &userID); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if email != "dup@example.com" {
			t.Errorf("unexpected email %q in results", email)
		}
		got = append(got, userID)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("expected user_id [1 2] for the duplicated email, got %v", got)
	}
}
