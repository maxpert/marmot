package rules

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

// parseSQL is a helper to parse SQL for tests - Vitess uses NewTestParser()
func parseSQL(sql string) (sqlparser.Statement, error) {
	return sqlparser.NewTestParser().Parse(sql)
}

func TestIntToBigintRule(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSQL  string
		modified bool
	}{
		{
			name:     "INT AUTO_INCREMENT to BIGINT",
			input:    "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
			wantSQL:  "bigint",
			modified: true,
		},
		{
			name:     "INTEGER AUTO_INCREMENT to BIGINT",
			input:    "CREATE TABLE items (id INTEGER AUTO_INCREMENT PRIMARY KEY, data TEXT)",
			wantSQL:  "bigint",
			modified: true,
		},
		{
			name:     "int lowercase AUTO_INCREMENT to BIGINT",
			input:    "CREATE TABLE logs (id int AUTO_INCREMENT, msg TEXT)",
			wantSQL:  "bigint",
			modified: true,
		},
		{
			name:     "INT without AUTO_INCREMENT unchanged",
			input:    "CREATE TABLE orders (id INT PRIMARY KEY, total REAL)",
			wantSQL:  "int",
			modified: false,
		},
		{
			name:     "BIGINT AUTO_INCREMENT unchanged",
			input:    "CREATE TABLE big (id BIGINT AUTO_INCREMENT PRIMARY KEY, data TEXT)",
			wantSQL:  "bigint",
			modified: false,
		},
		{
			name:     "VARCHAR column unchanged",
			input:    "CREATE TABLE names (id VARCHAR(50) PRIMARY KEY, value TEXT)",
			wantSQL:  "varchar",
			modified: false,
		},
		{
			name:     "Non-CREATE statement unchanged",
			input:    "INSERT INTO users (id, name) VALUES (1, 'test')",
			wantSQL:  "",
			modified: false,
		},
	}

	rule := &IntToBigintRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parseSQL(tt.input)
			if err != nil {
				if tt.wantSQL == "" {
					// Expected to not be a CREATE TABLE
					return
				}
				t.Fatalf("failed to parse SQL: %v", err)
			}

			newStmt, modified, err := rule.ApplyAST(stmt)
			if err != nil {
				t.Fatalf("ApplyAST failed: %v", err)
			}

			if modified != tt.modified {
				t.Errorf("modified = %v, want %v", modified, tt.modified)
			}

			if tt.wantSQL != "" {
				output := sqlparser.String(newStmt)
				if !strings.Contains(strings.ToLower(output), tt.wantSQL) {
					t.Errorf("output SQL doesn't contain %q:\n%s", tt.wantSQL, output)
				}
			}
		})
	}
}

func TestIntToBigintRule_Priority(t *testing.T) {
	rule := &IntToBigintRule{}

	// IntToBigintRule should run before CreateTableRule (priority 5)
	if rule.Priority() >= 5 {
		t.Errorf("Priority = %d, want < 5 (must run before CreateTableRule)", rule.Priority())
	}
}

func TestIntToBigintRule_Name(t *testing.T) {
	rule := &IntToBigintRule{}
	if rule.Name() != "IntToBigint" {
		t.Errorf("Name = %q, want %q", rule.Name(), "IntToBigint")
	}
}

func TestIntToBigintRule_MultipleColumns(t *testing.T) {
	rule := &IntToBigintRule{}

	input := "CREATE TABLE test (id INT AUTO_INCREMENT PRIMARY KEY, ref_id INT, counter INT AUTO_INCREMENT)"
	stmt, err := parseSQL(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	newStmt, modified, err := rule.ApplyAST(stmt)
	if err != nil {
		t.Fatalf("ApplyAST failed: %v", err)
	}

	if !modified {
		t.Error("expected statement to be modified")
	}

	output := sqlparser.String(newStmt)

	// Both AUTO_INCREMENT columns should be BIGINT now
	// ref_id without AUTO_INCREMENT should remain INT
	lowerOutput := strings.ToLower(output)

	// Count occurrences of "bigint"
	bigintCount := strings.Count(lowerOutput, "bigint")
	if bigintCount != 2 {
		t.Errorf("expected 2 bigint columns, got %d in: %s", bigintCount, output)
	}
}
