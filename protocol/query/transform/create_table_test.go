package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCreateTableRule_Name(t *testing.T) {
	rule := &CreateTableRule{}
	if rule.Name() != "CreateTable" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "CreateTable")
	}
}

func TestCreateTableRule_Priority(t *testing.T) {
	rule := &CreateTableRule{}
	if rule.Priority() != 10 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 10)
	}
}

func TestCreateTableRule_ExtractIndexes(t *testing.T) {
	tests := []struct {
		name               string
		input              string
		wantApplicable     bool
		wantStatementCount int
		checkStatements    func([]TranspiledStatement) error
	}{
		{
			name: "extract single KEY",
			input: `CREATE TABLE t (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				KEY name_idx (name)
			)`,
			wantApplicable:     true,
			wantStatementCount: 2,
			checkStatements: func(stmts []TranspiledStatement) error {
				if len(stmts) != 2 {
					t.Errorf("expected 2 statements, got %d", len(stmts))
				}
				expected := "CREATE INDEX IF NOT EXISTS name_idx ON t (name)"
				if stmts[1].SQL != expected {
					t.Errorf("index statement = %q, want %q", stmts[1].SQL, expected)
				}
				return nil
			},
		},
		{
			name: "keep UNIQUE KEY inline",
			input: `CREATE TABLE t (
				id INT PRIMARY KEY,
				email VARCHAR(100),
				UNIQUE KEY email_idx (email)
			)`,
			wantApplicable:     true, // Rule always applies to CREATE TABLE (strips MySQL-specific options)
			wantStatementCount: 1,    // Just the CREATE TABLE with UNIQUE KEY inline, no indexes extracted
		},
		{
			name: "extract regular KEY, keep UNIQUE KEY inline",
			input: `CREATE TABLE t (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				email VARCHAR(100),
				KEY name_idx (name),
				UNIQUE KEY email_idx (email)
			)`,
			wantApplicable:     true,
			wantStatementCount: 2,
			checkStatements: func(stmts []TranspiledStatement) error {
				if len(stmts) != 2 {
					t.Errorf("expected 2 statements, got %d", len(stmts))
				}
				// First statement should be CREATE TABLE with UNIQUE KEY still in it
				if !strings.Contains(strings.ToLower(stmts[0].SQL), "unique") {
					t.Errorf("CREATE TABLE should contain UNIQUE constraint, got: %q", stmts[0].SQL)
				}
				// Second statement should be CREATE INDEX for regular key
				expected := "CREATE INDEX IF NOT EXISTS name_idx ON t (name)"
				if stmts[1].SQL != expected {
					t.Errorf("index statement = %q, want %q", stmts[1].SQL, expected)
				}
				return nil
			},
		},
		{
			name: "keep PRIMARY KEY",
			input: `CREATE TABLE t (
				id INT,
				PRIMARY KEY (id)
			)`,
			wantApplicable:     true, // Rule always applies to CREATE TABLE (strips MySQL-specific options)
			wantStatementCount: 1,    // Just the CREATE TABLE, no indexes extracted
		},
		{
			name: "composite index",
			input: `CREATE TABLE t (
				id INT PRIMARY KEY,
				first_name VARCHAR(50),
				last_name VARCHAR(50),
				KEY name_idx (first_name, last_name)
			)`,
			wantApplicable:     true,
			wantStatementCount: 2,
			checkStatements: func(stmts []TranspiledStatement) error {
				if len(stmts) != 2 {
					t.Errorf("expected 2 statements, got %d", len(stmts))
				}
				expected := "CREATE INDEX IF NOT EXISTS name_idx ON t (first_name, last_name)"
				if stmts[1].SQL != expected {
					t.Errorf("index statement = %q, want %q", stmts[1].SQL, expected)
				}
				return nil
			},
		},
		{
			name:               "no indexes",
			input:              "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))",
			wantApplicable:     true, // Rule always applies to CREATE TABLE (strips MySQL-specific options)
			wantStatementCount: 1,    // Just the CREATE TABLE, no indexes extracted
		},
		{
			name:               "not a CREATE TABLE",
			input:              "SELECT * FROM t",
			wantApplicable:     false,
			wantStatementCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &CreateTableRule{}
			results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})

			if tt.wantApplicable {
				if err == ErrRuleNotApplicable {
					t.Fatalf("Transform should have been applicable but got ErrRuleNotApplicable")
				}
				if err != nil {
					t.Fatalf("Transform failed: %v", err)
				}

				if len(results) != tt.wantStatementCount {
					t.Errorf("statement count = %d, want %d", len(results), tt.wantStatementCount)
				}

				if tt.checkStatements != nil {
					_ = tt.checkStatements(results)
				}

				// Verify the main CREATE TABLE doesn't have extracted indexes
				mainStmt, parseErr := sqlparser.NewTestParser().Parse(results[0].SQL)
				if parseErr != nil {
					t.Fatalf("failed to parse main statement: %v", parseErr)
				}

				create, ok := mainStmt.(*sqlparser.CreateTable)
				if !ok {
					t.Fatal("expected CreateTable statement")
				}

				for _, idx := range create.TableSpec.Indexes {
					if idx.Info != nil && idx.Info.Type != sqlparser.IndexTypePrimary &&
						idx.Info.Type != sqlparser.IndexTypeFullText &&
						idx.Info.Type != sqlparser.IndexTypeSpatial &&
						!idx.Info.IsUnique() {
						t.Errorf("non-unique index %q should have been extracted", idx.Info.Name.String())
					}
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got %v", err)
				}
				if results != nil {
					t.Errorf("expected nil results, got %d statements", len(results))
				}
			}
		})
	}
}

func TestCreateTableRule_MultipleTransforms(t *testing.T) {
	rule := &CreateTableRule{}

	input1 := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), KEY name_idx (name))"
	stmt1, _ := sqlparser.NewTestParser().Parse(input1)
	results1, err := rule.Transform(stmt1, nil, nil, "", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if results1 == nil {
		t.Fatal("expected first transform to be applied")
	}

	if len(results1) != 2 {
		t.Fatalf("expected 2 statements after first transform, got %d", len(results1))
	}

	input2 := "CREATE TABLE t2 (id INT PRIMARY KEY, email VARCHAR(100), UNIQUE KEY email_idx (email))"
	stmt2, _ := sqlparser.NewTestParser().Parse(input2)
	results2, err := rule.Transform(stmt2, nil, nil, "", &SQLiteSerializer{})

	// Rule now always applies to CREATE TABLE (to strip MySQL-specific options)
	// UNIQUE KEY is kept inline, not extracted
	if err != nil {
		t.Fatalf("expected rule to apply, got: %v", err)
	}

	if len(results2) != 1 {
		t.Errorf("expected 1 statement for UNIQUE KEY only table, got %d statements", len(results2))
	}
}

func TestCreateTableRule_UniqueConstraintFormat(t *testing.T) {
	input := `CREATE TABLE t (
		id INT PRIMARY KEY,
		email VARCHAR(100),
		UNIQUE KEY email_idx (email)
	)`

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &CreateTableRule{}
	results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})

	// Rule now always applies to CREATE TABLE (to strip MySQL-specific options)
	if err != nil {
		t.Fatalf("expected rule to apply, got: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(results))
	}

	sql := results[0].SQL
	// Should contain CONSTRAINT ... UNIQUE, not UNIQUE KEY
	lowerSQL := strings.ToLower(sql)
	if !strings.Contains(lowerSQL, "constraint") {
		t.Errorf("serialized SQL should contain CONSTRAINT, got: %q", sql)
	}
	if !strings.Contains(lowerSQL, "unique") {
		t.Errorf("serialized SQL should contain UNIQUE, got: %q", sql)
	}
	if strings.Contains(lowerSQL, "key") && strings.Contains(lowerSQL, "unique key") {
		t.Errorf("serialized SQL should not contain 'UNIQUE KEY', got: %q", sql)
	}
}

func TestCreateTableRule_StripTableOptions(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantApplicable bool
		checkOutput    func(*testing.T, string)
	}{
		{
			name: "strip ENGINE, CHARSET, COLLATE",
			input: `CREATE TABLE t (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				KEY name_idx (name)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci`,
			wantApplicable: true,
			checkOutput: func(t *testing.T, sql string) {
				lowerSQL := strings.ToLower(sql)
				if strings.Contains(lowerSQL, "charset") {
					t.Errorf("CREATE TABLE should not contain CHARSET, got: %q", sql)
				}
				if strings.Contains(lowerSQL, "collate") {
					t.Errorf("CREATE TABLE should not contain COLLATE, got: %q", sql)
				}
				if strings.Contains(lowerSQL, "engine") {
					t.Errorf("CREATE TABLE should not contain ENGINE, got: %q", sql)
				}
			},
		},
		{
			name: "strip multiple table options",
			input: `CREATE TABLE users (
				id BIGINT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				UNIQUE KEY email_idx (email),
				KEY name_idx (name)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC`,
			wantApplicable: true,
			checkOutput: func(t *testing.T, sql string) {
				lowerSQL := strings.ToLower(sql)
				if strings.Contains(lowerSQL, "engine") {
					t.Errorf("CREATE TABLE should not contain ENGINE, got: %q", sql)
				}
				if strings.Contains(lowerSQL, "charset") {
					t.Errorf("CREATE TABLE should not contain CHARSET, got: %q", sql)
				}
				if strings.Contains(lowerSQL, "collate") {
					t.Errorf("CREATE TABLE should not contain COLLATE, got: %q", sql)
				}
				if strings.Contains(lowerSQL, "row_format") {
					t.Errorf("CREATE TABLE should not contain ROW_FORMAT, got: %q", sql)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &CreateTableRule{}
			results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})

			if tt.wantApplicable {
				if err == ErrRuleNotApplicable {
					t.Fatalf("Transform should have been applicable but got ErrRuleNotApplicable")
				}
				if err != nil {
					t.Fatalf("Transform failed: %v", err)
				}

				if len(results) == 0 {
					t.Fatal("expected at least one statement")
				}

				if tt.checkOutput != nil {
					tt.checkOutput(t, results[0].SQL)
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got %v", err)
				}
			}
		})
	}
}

func TestCreateTableRule_StripIntegerWidths(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantApplicable bool
		checkOutput    func(*testing.T, string)
	}{
		{
			name: "strip BIGINT(20) width",
			input: `CREATE TABLE t (
				id BIGINT(20) PRIMARY KEY,
				count INT(11),
				KEY count_idx (count)
			)`,
			wantApplicable: true,
			checkOutput: func(t *testing.T, sql string) {
				// Should not contain integer type widths like (20) or (11)
				if strings.Contains(sql, "BIGINT(20)") || strings.Contains(sql, "bigint(20)") {
					t.Errorf("CREATE TABLE should not contain BIGINT(20), got: %q", sql)
				}
				if strings.Contains(sql, "INT(11)") || strings.Contains(sql, "int(11)") {
					t.Errorf("CREATE TABLE should not contain INT(11), got: %q", sql)
				}
				// Should contain integer types without widths
				lowerSQL := strings.ToLower(sql)
				if !strings.Contains(lowerSQL, "bigint") {
					t.Errorf("CREATE TABLE should contain BIGINT, got: %q", sql)
				}
				if !strings.Contains(lowerSQL, "int") {
					t.Errorf("CREATE TABLE should contain INT, got: %q", sql)
				}
			},
		},
		{
			name: "strip all integer type widths",
			input: `CREATE TABLE t (
				tiny TINYINT(4),
				small SMALLINT(6),
				medium MEDIUMINT(9),
				normal INT(11),
				big BIGINT(20),
				KEY normal_idx (normal)
			)`,
			wantApplicable: true,
			checkOutput: func(t *testing.T, sql string) {
				// Should not contain any integer widths
				if strings.Contains(sql, "(4)") || strings.Contains(sql, "(6)") ||
					strings.Contains(sql, "(9)") || strings.Contains(sql, "(11)") ||
					strings.Contains(sql, "(20)") {
					t.Errorf("CREATE TABLE should not contain integer widths, got: %q", sql)
				}
			},
		},
		{
			name: "preserve VARCHAR widths",
			input: `CREATE TABLE t (
				id INT(11),
				name VARCHAR(100),
				KEY name_idx (name)
			)`,
			wantApplicable: true,
			checkOutput: func(t *testing.T, sql string) {
				// VARCHAR widths should be preserved
				if !strings.Contains(sql, "VARCHAR(100)") && !strings.Contains(sql, "varchar(100)") {
					t.Errorf("CREATE TABLE should preserve VARCHAR(100), got: %q", sql)
				}
				// INT width should be removed
				if strings.Contains(sql, "INT(11)") || strings.Contains(sql, "int(11)") {
					t.Errorf("CREATE TABLE should not contain INT(11), got: %q", sql)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &CreateTableRule{}
			results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})

			if tt.wantApplicable {
				if err == ErrRuleNotApplicable {
					t.Fatalf("Transform should have been applicable but got ErrRuleNotApplicable")
				}
				if err != nil {
					t.Fatalf("Transform failed: %v", err)
				}

				if len(results) == 0 {
					t.Fatal("expected at least one statement")
				}

				if tt.checkOutput != nil {
					tt.checkOutput(t, results[0].SQL)
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got %v", err)
				}
			}
		})
	}
}

func TestCreateTableRule_CombinedOptionsAndWidths(t *testing.T) {
	input := `CREATE TABLE users (
		id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
		username VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE KEY username_idx (username),
		KEY created_idx (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &CreateTableRule{}
	results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})

	if err == ErrRuleNotApplicable {
		t.Fatalf("Transform should have been applicable")
	}
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// Should have 2 statements: CREATE TABLE (with UNIQUE inline) + 1 regular index
	if len(results) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(results))
	}

	mainSQL := results[0].SQL
	lowerSQL := strings.ToLower(mainSQL)

	// Check table options are stripped
	if strings.Contains(lowerSQL, "engine") {
		t.Errorf("CREATE TABLE should not contain ENGINE, got: %q", mainSQL)
	}
	if strings.Contains(lowerSQL, "charset") {
		t.Errorf("CREATE TABLE should not contain CHARSET, got: %q", mainSQL)
	}
	if strings.Contains(lowerSQL, "collate") {
		t.Errorf("CREATE TABLE should not contain COLLATE, got: %q", mainSQL)
	}

	// Check integer widths are stripped
	if strings.Contains(lowerSQL, "bigint(20)") {
		t.Errorf("CREATE TABLE should not contain BIGINT(20), got: %q", mainSQL)
	}

	// Check VARCHAR widths are preserved
	if !strings.Contains(lowerSQL, "varchar(50)") {
		t.Errorf("CREATE TABLE should preserve VARCHAR(50), got: %q", mainSQL)
	}

	// Check UNIQUE constraint is kept in CREATE TABLE
	if !strings.Contains(lowerSQL, "constraint") || !strings.Contains(lowerSQL, "unique") {
		t.Errorf("CREATE TABLE should contain UNIQUE constraint inline, got: %q", mainSQL)
	}

	// Check only regular index is extracted
	if results[1].SQL != "CREATE INDEX IF NOT EXISTS created_idx ON users (created_at)" {
		t.Errorf("unexpected index statement: %q", results[1].SQL)
	}
}

// TestCreateTableRule_StripColumnCollateAndComment tests that MySQL-specific
// column options (COLLATE, COMMENT) are stripped from CREATE TABLE statements.
func TestCreateTableRule_StripColumnCollateAndComment(t *testing.T) {
	input := `CREATE TABLE wp_litespeed_url_file (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		vary varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'md5 of final vary',
		filename varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'md5 of file content',
		type tinyint(4) NOT NULL COMMENT 'css=1,js=2,ccss=3,ucss=4',
		PRIMARY KEY (id),
		KEY filename (filename),
		KEY type (type)
	) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci`

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &CreateTableRule{}
	results, err := rule.Transform(stmt, nil, nil, "", &SQLiteSerializer{})

	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// Should have 3 statements: CREATE TABLE + 2 CREATE INDEX
	if len(results) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(results))
	}

	mainSQL := results[0].SQL
	lowerSQL := strings.ToLower(mainSQL)

	// Check column-level COLLATE is stripped
	if strings.Contains(lowerSQL, "utf8mb4_unicode_ci") {
		t.Errorf("CREATE TABLE should not contain column COLLATE, got: %q", mainSQL)
	}

	// Check COMMENT is stripped
	if strings.Contains(lowerSQL, "comment") {
		t.Errorf("CREATE TABLE should not contain COMMENT, got: %q", mainSQL)
	}

	// Check table-level COLLATE is stripped
	if strings.Contains(lowerSQL, "utf8mb4_unicode_520_ci") {
		t.Errorf("CREATE TABLE should not contain table COLLATE, got: %q", mainSQL)
	}

	// Check integer widths are stripped
	if strings.Contains(lowerSQL, "bigint(20)") || strings.Contains(lowerSQL, "tinyint(4)") {
		t.Errorf("CREATE TABLE should not contain integer widths, got: %q", mainSQL)
	}

	// Check VARCHAR widths are preserved
	if !strings.Contains(lowerSQL, "varchar(32)") {
		t.Errorf("CREATE TABLE should preserve VARCHAR(32), got: %q", mainSQL)
	}

	// Verify it's valid SQLite SQL (no MySQL-specific syntax)
	if strings.Contains(lowerSQL, "character set") {
		t.Errorf("CREATE TABLE should not contain CHARACTER SET, got: %q", mainSQL)
	}
}
