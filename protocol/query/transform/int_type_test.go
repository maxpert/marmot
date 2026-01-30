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

// TestIntTypeRule_ModifiesAST verifies that IntTypeRule modifies the AST correctly
// even though it always defers to CreateTableRule for serialization.
func TestIntTypeRule_ModifiesAST(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		shouldModify    bool
		checkUnsigned   bool
		checkAutoInc    bool
		checkIntType    bool
	}{
		{
			name:          "INT AUTO_INCREMENT stripped",
			input:         "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
			shouldModify:  true,
			checkAutoInc:  true,
			checkIntType:  true,
		},
		{
			name:          "BIGINT UNSIGNED AUTO_INCREMENT stripped",
			input:         "CREATE TABLE wp_users (ID BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, name VARCHAR(100))",
			shouldModify:  true,
			checkUnsigned: true,
			checkAutoInc:  true,
		},
		{
			name:          "INT UNSIGNED stripped",
			input:         "CREATE TABLE items (id INT UNSIGNED NOT NULL, data TEXT)",
			shouldModify:  true,
			checkUnsigned: true,
		},
		{
			name:          "TINYINT UNSIGNED stripped",
			input:         "CREATE TABLE flags (active TINYINT UNSIGNED DEFAULT 0)",
			shouldModify:  true,
			checkUnsigned: true,
			checkIntType:  true,
		},
		{
			name:         "INT converted to INTEGER",
			input:        "CREATE TABLE orders (id INT PRIMARY KEY, total REAL)",
			shouldModify: true,
			checkIntType: true,
		},
		{
			name:         "VARCHAR column unchanged - no modification",
			input:        "CREATE TABLE names (id VARCHAR(50) PRIMARY KEY, value TEXT)",
			shouldModify: false,
		},
		{
			name:         "not a CREATE TABLE - returns immediately",
			input:        "INSERT INTO users (id, name) VALUES (1, 'test')",
			shouldModify: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &IntTypeRule{}
			_, err = rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			// IntTypeRule always returns ErrRuleNotApplicable for CREATE TABLE
			// (defers to CreateTableRule for serialization)
			if err != ErrRuleNotApplicable {
				t.Errorf("expected ErrRuleNotApplicable, got %v", err)
			}

			if !tt.shouldModify {
				return
			}

			// Verify AST was modified
			create, ok := stmt.(*sqlparser.CreateTable)
			if !ok {
				return // Non-CREATE TABLE statement
			}

			for _, col := range create.TableSpec.Columns {
				if col.Type == nil {
					continue
				}

				// Check UNSIGNED was stripped
				if tt.checkUnsigned && col.Type.Unsigned {
					t.Errorf("UNSIGNED should have been stripped from column %s", col.Name.String())
				}

				// Check AUTO_INCREMENT was stripped
				if tt.checkAutoInc && col.Type.Options != nil && col.Type.Options.Autoincrement {
					t.Errorf("AUTO_INCREMENT should have been stripped from column %s", col.Name.String())
				}

				// Check integer type was converted to INTEGER
				if tt.checkIntType && isIntegerType(strings.ToUpper(col.Type.Type)) {
					if col.Type.Type != "INTEGER" {
						t.Errorf("integer type should have been converted to INTEGER for column %s, got %s", col.Name.String(), col.Type.Type)
					}
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
	_, err = rule.Transform(stmt, nil, nil, "wordpress", &SQLiteSerializer{})

	// Should defer to CreateTableRule
	if err != ErrRuleNotApplicable {
		t.Errorf("expected ErrRuleNotApplicable, got %v", err)
	}

	// Verify AST was modified
	create := stmt.(*sqlparser.CreateTable)
	for _, col := range create.TableSpec.Columns {
		if col.Type == nil {
			continue
		}

		// Check UNSIGNED was stripped
		if col.Type.Unsigned {
			t.Errorf("UNSIGNED should have been stripped from column %s", col.Name.String())
		}

		// Check AUTO_INCREMENT was stripped
		if col.Type.Options != nil && col.Type.Options.Autoincrement {
			t.Errorf("AUTO_INCREMENT should have been stripped from column %s", col.Name.String())
		}

		// Check integer types were converted to INTEGER
		if isIntegerType(strings.ToUpper(col.Type.Type)) {
			if col.Type.Type != "INTEGER" {
				t.Errorf("integer type should be INTEGER for column %s, got %s", col.Name.String(), col.Type.Type)
			}
		}
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
			_, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			// Should defer to CreateTableRule
			if err != ErrRuleNotApplicable {
				t.Errorf("expected ErrRuleNotApplicable for %s, got %v", tt.name, err)
			}

			// Verify AST was modified
			create := stmt.(*sqlparser.CreateTable)
			for _, col := range create.TableSpec.Columns {
				if col.Type == nil {
					continue
				}
				if col.Type.Unsigned {
					t.Errorf("%s: UNSIGNED should have been stripped", tt.name)
				}
				if col.Type.Type != "INTEGER" {
					t.Errorf("%s: type should be INTEGER, got %s", tt.name, col.Type.Type)
				}
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
		{
			name:  "With PRIMARY KEY only",
			input: "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
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
