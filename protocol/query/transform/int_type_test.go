package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestIntTypeRule_Name(t *testing.T) {
	rule := &IntTypeRule{}
	if rule.Name() != "IntType" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "IntType")
	}
}

func TestIntTypeRule_Priority(t *testing.T) {
	rule := &IntTypeRule{}
	if rule.Priority() != 5 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 5)
	}
}

func TestIntTypeRule_Transform(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		wantModified    bool
		wantContains    string
		wantNotContains []string
	}{
		{
			name:            "INT AUTO_INCREMENT to INTEGER without auto_increment keyword",
			input:           "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
			wantModified:    true,
			wantContains:    "integer",
			wantNotContains: []string{"auto_increment"},
		},
		{
			name:            "BIGINT UNSIGNED AUTO_INCREMENT stripped",
			input:           "CREATE TABLE wp_users (ID BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, name VARCHAR(100))",
			wantModified:    true,
			wantNotContains: []string{"unsigned", "auto_increment"},
		},
		{
			name:            "INT UNSIGNED stripped",
			input:           "CREATE TABLE items (id INT UNSIGNED NOT NULL, data TEXT)",
			wantModified:    true,
			wantNotContains: []string{"unsigned"},
		},
		{
			name:            "TINYINT UNSIGNED stripped",
			input:           "CREATE TABLE flags (active TINYINT UNSIGNED DEFAULT 0)",
			wantModified:    true,
			wantNotContains: []string{"unsigned"},
		},
		{
			name:         "INT converted to INTEGER",
			input:        "CREATE TABLE orders (id INT PRIMARY KEY, total REAL)",
			wantModified: true,
			wantContains: "integer",
		},
		{
			name:         "VARCHAR column unchanged",
			input:        "CREATE TABLE names (id VARCHAR(50) PRIMARY KEY, value TEXT)",
			wantModified: false,
		},
		{
			name:         "not a CREATE TABLE",
			input:        "INSERT INTO users (id, name) VALUES (1, 'test')",
			wantModified: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &IntTypeRule{}
			result, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			if tt.wantModified {
				if err != nil {
					t.Fatalf("Transform failed: %v", err)
				}
				if len(result) == 0 {
					t.Fatal("expected result, got nil or empty")
				}
				if len(result) != 1 {
					t.Errorf("expected 1 statement, got %d", len(result))
				}
				lowerOutput := strings.ToLower(result[0].SQL)
				if tt.wantContains != "" && !strings.Contains(lowerOutput, tt.wantContains) {
					t.Errorf("output doesn't contain expected %q:\n%s", tt.wantContains, result[0].SQL)
				}
				for _, notContains := range tt.wantNotContains {
					if strings.Contains(lowerOutput, notContains) {
						t.Errorf("output should not contain %q:\n%s", notContains, result[0].SQL)
					}
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got %v", err)
				}
				if result != nil {
					t.Errorf("expected nil result, got %v", result)
				}
			}
		})
	}
}

func TestIntTypeRule_WordPressSchema(t *testing.T) {
	// Real WordPress CREATE TABLE statement
	input := `CREATE TABLE wp_users (
		ID bigint(20) unsigned NOT NULL auto_increment,
		user_login varchar(60) NOT NULL default '',
		user_pass varchar(255) NOT NULL default '',
		user_status int(11) NOT NULL default '0',
		PRIMARY KEY (ID)
	)`

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &IntTypeRule{}
	result, err := rule.Transform(stmt, nil, nil, "wordpress", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if len(result) == 0 {
		t.Fatal("expected result, got nil or empty")
	}

	lowerOutput := strings.ToLower(result[0].SQL)

	// Should NOT contain unsigned
	if strings.Contains(lowerOutput, "unsigned") {
		t.Errorf("output should not contain 'unsigned':\n%s", result[0].SQL)
	}

	// Should NOT contain auto_increment
	if strings.Contains(lowerOutput, "auto_increment") {
		t.Errorf("output should not contain 'auto_increment':\n%s", result[0].SQL)
	}

	// AUTO_INCREMENT column should be INTEGER
	if !strings.Contains(lowerOutput, "integer") {
		t.Errorf("expected 'integer' for auto_increment column, got:\n%s", result[0].SQL)
	}
}

func TestIntTypeRule_AllIntTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"TINYINT", "CREATE TABLE t (col TINYINT UNSIGNED)"},
		{"SMALLINT", "CREATE TABLE t (col SMALLINT UNSIGNED)"},
		{"MEDIUMINT", "CREATE TABLE t (col MEDIUMINT UNSIGNED)"},
		{"INT", "CREATE TABLE t (col INT UNSIGNED)"},
		{"INTEGER", "CREATE TABLE t (col INTEGER UNSIGNED)"},
		{"BIGINT", "CREATE TABLE t (col BIGINT UNSIGNED)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := sqlparser.NewTestParser().Parse(tt.input)

			rule := &IntTypeRule{}
			result, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
			if err != nil {
				t.Fatalf("Transform failed for %s: %v", tt.name, err)
			}

			if len(result) == 0 {
				t.Fatalf("expected result for %s, got nil or empty", tt.name)
			}

			lowerOutput := strings.ToLower(result[0].SQL)
			if strings.Contains(lowerOutput, "unsigned") {
				t.Errorf("%s: output should not contain 'unsigned':\n%s", tt.name, result[0].SQL)
			}
		})
	}
}

func TestIntTypeRule_DeferToCreateTableRule(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "With non-primary KEY definition",
			input: "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(100), KEY idx_email (email))",
		},
		{
			name:  "With UNIQUE KEY definition",
			input: "CREATE TABLE users (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), UNIQUE KEY idx_name (name))",
		},
		{
			name:  "With multiple KEY definitions",
			input: "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, user_id INT UNSIGNED, title VARCHAR(100), KEY idx_user (user_id), KEY idx_title (title))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &IntTypeRule{}
			result, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			// Should return ErrRuleNotApplicable to defer to CreateTableRule
			if err != ErrRuleNotApplicable {
				t.Errorf("expected ErrRuleNotApplicable to defer to CreateTableRule, got %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result when deferring to CreateTableRule, got %v", result)
			}

			// Verify that the AST was modified (AUTO_INCREMENT removed, type changed to INTEGER)
			create := stmt.(*sqlparser.CreateTable)
			for _, col := range create.TableSpec.Columns {
				if col.Type == nil {
					continue
				}
				// Check that AUTO_INCREMENT was stripped
				if col.Type.Options != nil && col.Type.Options.Autoincrement {
					t.Errorf("AUTO_INCREMENT should have been removed from column %s", col.Name.String())
				}
				// Check that integer types were converted to INTEGER
				if isIntegerType(strings.ToUpper(col.Type.Type)) {
					if col.Type.Type != "INTEGER" {
						t.Errorf("integer type should have been converted to INTEGER for column %s, got %s", col.Name.String(), col.Type.Type)
					}
				}
			}
		})
	}
}

func TestIntTypeRule_WithPrimaryKeyOnly(t *testing.T) {
	input := "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), PRIMARY KEY (id))"

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &IntTypeRule{}
	result, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

	// Should serialize since there are no non-primary indexes
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
	if len(result) == 0 {
		t.Fatal("expected result, got nil or empty")
	}

	lowerOutput := strings.ToLower(result[0].SQL)
	if strings.Contains(lowerOutput, "auto_increment") {
		t.Errorf("output should not contain 'auto_increment':\n%s", result[0].SQL)
	}
	if !strings.Contains(lowerOutput, "integer") {
		t.Errorf("expected 'integer' for auto_increment column, got:\n%s", result[0].SQL)
	}
}
