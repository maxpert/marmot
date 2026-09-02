package query

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/protocol/query/transform"
)

// TestPipeline_CreateIndexTranspiles pins the LLDAP migration regression: standalone
// "CREATE [UNIQUE] INDEX name ON table (cols)" parses in Vitess to the exact same
// *AlterTable/AddIndexDefinition AST shape as "ALTER TABLE t ADD INDEX ..." (see
// create_index_prefix in vitess's sql.y), so without AlterTableConstraintRule handling
// plain (non-unique) index adds too, it fell through to the default serializer and printed
// invalid "ALTER TABLE t ADD INDEX ..." SQL that SQLite's PREPARE step rejects. Verifies
// both the transpiled SQL text and that it actually executes against real SQLite.
func TestPipeline_CreateIndexTranspiles(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		want  string
		table string
	}{
		{
			name:  "plain index, unquoted, single column",
			sql:   "CREATE INDEX idx_email ON users (email)",
			want:  `CREATE INDEX "idx_email" ON "users" ("email")`,
			table: "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)",
		},
		{
			name:  "plain index, backtick-quoted, multi-column",
			sql:   "CREATE INDEX `idx_name_email` ON `users` (`name`, `email`)",
			want:  `CREATE INDEX "idx_name_email" ON "users" ("name", "email")`,
			table: "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)",
		},
		{
			name:  "unique index, unquoted, single column",
			sql:   "CREATE UNIQUE INDEX idx_email ON users (email)",
			want:  `CREATE UNIQUE INDEX "idx_email" ON "users" ("email")`,
			table: "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)",
		},
		{
			name:  "unique index, backtick-quoted, multi-column",
			sql:   "CREATE UNIQUE INDEX `idx_name_email` ON `users` (`name`, `email`)",
			want:  `CREATE UNIQUE INDEX "idx_name_email" ON "users" ("name", "email")`,
			table: "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("Failed to create pipeline: %v", err)
			}
			defer pipeline.Close()

			ctx := NewContext(tt.sql, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}
			if len(ctx.Output.Statements) != 1 || ctx.Output.Statements[0].SQL != tt.want {
				t.Fatalf("got statements %+v, want single statement %q", ctx.Output.Statements, tt.want)
			}

			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("failed to open sqlite: %v", err)
			}
			defer db.Close()

			if _, err := db.Exec(tt.table); err != nil {
				t.Fatalf("failed to create table: %v", err)
			}
			if _, err := db.Exec(ctx.Output.Statements[0].SQL); err != nil {
				t.Fatalf("transpiled SQL failed to execute: %v\nSQL: %s", err, ctx.Output.Statements[0].SQL)
			}
		})
	}
}

// TestPipeline_DropIndexTranspiles pins the second LLDAP migration regression: MySQL's
// "DROP INDEX idx ON table" was classified as Vitess-incompatible (SkipVitess) and passed
// through completely untranspiled, but SQLite's DROP INDEX has no ON clause and rejects it.
func TestPipeline_DropIndexTranspiles(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		want          string
		createIndexOn string // the pre-existing SQLite index name the DROP must actually hit
	}{
		{
			name:          "plain, unquoted",
			sql:           "DROP INDEX idx_email ON users",
			want:          `DROP INDEX "idx_email"`,
			createIndexOn: "idx_email",
		},
		{
			name:          "IF EXISTS preserved",
			sql:           "DROP INDEX IF EXISTS idx_email ON users",
			want:          `DROP INDEX IF EXISTS "idx_email"`,
			createIndexOn: "idx_email",
		},
		{
			name:          "backtick-quoted names",
			sql:           "DROP INDEX `idx_email` ON `users`",
			want:          `DROP INDEX "idx_email"`,
			createIndexOn: "idx_email",
		},
		{
			name:          "backtick-quoted hyphenated name",
			sql:           "DROP INDEX `unique-user-email` ON `users`",
			want:          `DROP INDEX "unique-user-email"`,
			createIndexOn: "unique-user-email",
		},
		{
			name:          "backtick-quoted name with spaces",
			sql:           "DROP INDEX `user email idx` ON `users`",
			want:          `DROP INDEX "user email idx"`,
			createIndexOn: "user email idx",
		},
		{
			name:          "IF EXISTS with backtick-quoted hyphenated name",
			sql:           "DROP INDEX IF EXISTS `unique-user-email` ON `users`",
			want:          `DROP INDEX IF EXISTS "unique-user-email"`,
			createIndexOn: "unique-user-email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("Failed to create pipeline: %v", err)
			}
			defer pipeline.Close()

			ctx := NewContext(tt.sql, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}
			if len(ctx.Output.Statements) != 1 || ctx.Output.Statements[0].SQL != tt.want {
				t.Fatalf("got statements %+v, want single statement %q", ctx.Output.Statements, tt.want)
			}

			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("failed to open sqlite: %v", err)
			}
			defer db.Close()

			if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
				t.Fatalf("failed to create table: %v", err)
			}
			createIndexSQL := `CREATE INDEX ` + transform.QuoteIdentifier(tt.createIndexOn) + ` ON users (email)`
			if _, err := db.Exec(createIndexSQL); err != nil {
				t.Fatalf("failed to create index: %v", err)
			}
			if _, err := db.Exec(ctx.Output.Statements[0].SQL); err != nil {
				t.Fatalf("transpiled SQL failed to execute: %v\nSQL: %s", err, ctx.Output.Statements[0].SQL)
			}
		})
	}
}

// TestPipeline_AlterAddColumnCharsetStripped pins the third LLDAP migration regression:
// ALTER TABLE ADD COLUMN with a MySQL CHARACTER SET/COLLATE clause has no SQLite
// equivalent - CreateTableRule already stripped these for CREATE TABLE columns, but ALTER
// TABLE ADD/MODIFY/CHANGE COLUMN went through the default serializer unstripped and SQLite's
// PREPARE step rejected the CHARACTER SET clause.
func TestPipeline_AlterAddColumnCharsetStripped(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "CHARACTER SET and COLLATE",
			sql:  "ALTER TABLE users ADD COLUMN bio VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
			want: "alter table users add column bio VARCHAR(255)",
		},
		{
			name: "COLLATE only",
			sql:  "ALTER TABLE users ADD COLUMN bio VARCHAR(255) COLLATE utf8mb4_unicode_ci",
			want: "alter table users add column bio VARCHAR(255)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("Failed to create pipeline: %v", err)
			}
			defer pipeline.Close()

			ctx := NewContext(tt.sql, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}
			if len(ctx.Output.Statements) != 1 || ctx.Output.Statements[0].SQL != tt.want {
				t.Fatalf("got statements %+v, want single statement %q", ctx.Output.Statements, tt.want)
			}

			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("failed to open sqlite: %v", err)
			}
			defer db.Close()

			if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatalf("failed to create table: %v", err)
			}
			if _, err := db.Exec(ctx.Output.Statements[0].SQL); err != nil {
				t.Fatalf("transpiled SQL failed to execute: %v\nSQL: %s", err, ctx.Output.Statements[0].SQL)
			}
		})
	}
}

// TestPipeline_AlterAddColumnUniqueCombinedWithCharset verifies AlterTableColumnTypeRule
// (priority 8) runs before AlterTableConstraintRule (priority 10) so a combined ADD COLUMN
// (with charset) + ADD CONSTRAINT UNIQUE statement gets both fixes applied together.
func TestPipeline_AlterAddColumnUniqueCombinedWithCharset(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	sqlText := "ALTER TABLE users ADD COLUMN bio VARCHAR(255) CHARACTER SET utf8mb4, " +
		"ADD CONSTRAINT uq_bio UNIQUE (bio)"
	ctx := NewContext(sqlText, nil)
	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("pipeline.Process failed: %v", err)
	}
	if len(ctx.Output.Statements) != 2 {
		t.Fatalf("expected 2 statements, got %d: %+v", len(ctx.Output.Statements), ctx.Output.Statements)
	}

	wantAlter := "alter table users add column bio VARCHAR(255)"
	if ctx.Output.Statements[0].SQL != wantAlter {
		t.Errorf("first statement = %q, want %q", ctx.Output.Statements[0].SQL, wantAlter)
	}
	wantIndex := `CREATE UNIQUE INDEX "uq_bio" ON "users" ("bio")`
	if ctx.Output.Statements[1].SQL != wantIndex {
		t.Errorf("second statement = %q, want %q", ctx.Output.Statements[1].SQL, wantIndex)
	}

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	for _, stmt := range ctx.Output.Statements {
		if _, err := db.Exec(stmt.SQL); err != nil {
			t.Fatalf("transpiled SQL failed to execute: %v\nSQL: %s", err, stmt.SQL)
		}
	}
}
