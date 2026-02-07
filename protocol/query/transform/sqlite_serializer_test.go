package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestSQLiteSerializer_StringLiterals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // case-insensitive match
	}{
		{
			name:     "empty string default",
			input:    "CREATE TABLE t (col VARCHAR(50) DEFAULT '')",
			expected: "default ''",
		},
		{
			name:     "string with single quote",
			input:    "CREATE TABLE t (col VARCHAR(50) DEFAULT 'it''s ok')",
			expected: "default 'it''s ok'",
		},
		{
			name:     "regular string",
			input:    "CREATE TABLE t (col VARCHAR(50) DEFAULT 'hello')",
			expected: "default 'hello'",
		},
		{
			name:     "string with multiple quotes",
			input:    "CREATE TABLE t (col VARCHAR(50) DEFAULT 'can''t won''t don''t')",
			expected: "default 'can''t won''t don''t'",
		},
		{
			name:     "insert with empty string",
			input:    "INSERT INTO t (col) VALUES ('')",
			expected: "values ('')",
		},
		{
			name:     "insert with string containing quote",
			input:    "INSERT INTO t (col) VALUES ('it''s ok')",
			expected: "values ('it''s ok')",
		},
		{
			name:     "update with empty string",
			input:    "UPDATE t SET col = ''",
			expected: "set col = ''",
		},
		{
			name:     "update with string containing quote",
			input:    "UPDATE t SET col = 'it''s ok'",
			expected: "set col = 'it''s ok'",
		},
		{
			name:     "where clause with empty string",
			input:    "SELECT * FROM t WHERE col = ''",
			expected: "where col = ''",
		},
		{
			name:     "where clause with string containing quote",
			input:    "SELECT * FROM t WHERE col = 'it''s ok'",
			expected: "where col = 'it''s ok'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			serializer := &SQLiteSerializer{}
			result := serializer.Serialize(stmt)
			lowerResult := strings.ToLower(result)

			if !strings.Contains(lowerResult, tt.expected) {
				t.Errorf("expected output to contain %q (case-insensitive), got: %s", tt.expected, result)
			}

			// Verify no malformed defaults like "default ,"
			if strings.Contains(lowerResult, "default ,") {
				t.Errorf("malformed default found in output: %s", result)
			}
		})
	}
}

func TestSQLiteSerializer_EmptyStringEdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		shouldContain    string // case-insensitive
		shouldNotContain string // case-insensitive
	}{
		{
			name:             "empty string in multi-column table",
			input:            "CREATE TABLE t (id INT, col1 VARCHAR(50) DEFAULT '', col2 INT DEFAULT 0)",
			shouldContain:    "default ''",
			shouldNotContain: "default ,",
		},
		{
			name:             "multiple empty string defaults",
			input:            "CREATE TABLE t (col1 VARCHAR(50) DEFAULT '', col2 VARCHAR(100) DEFAULT '')",
			shouldContain:    "default ''",
			shouldNotContain: "default ,",
		},
		{
			name:             "empty string with NOT NULL",
			input:            "CREATE TABLE t (col VARCHAR(50) NOT NULL DEFAULT '')",
			shouldContain:    "default ''",
			shouldNotContain: "default ,",
		},
		{
			name:             "multiple values with empty strings",
			input:            "INSERT INTO t (col1, col2, col3) VALUES ('', 'hello', '')",
			shouldContain:    "''",
			shouldNotContain: "(,",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			serializer := &SQLiteSerializer{}
			result := serializer.Serialize(stmt)
			lowerResult := strings.ToLower(result)

			if !strings.Contains(lowerResult, tt.shouldContain) {
				t.Errorf("expected output to contain %q (case-insensitive), got: %s", tt.shouldContain, result)
			}

			if tt.shouldNotContain != "" && strings.Contains(lowerResult, tt.shouldNotContain) {
				t.Errorf("output should not contain %q, got: %s", tt.shouldNotContain, result)
			}
		})
	}
}

func TestSQLiteSerializer_SerializeBasic(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		shouldContain   []string // case-insensitive
		shouldTransform bool     // whether statement requires transformation
	}{
		{
			name:          "simple select",
			input:         "SELECT * FROM users",
			shouldContain: []string{"select", "from users"},
		},
		{
			name:          "insert with named parameters",
			input:         "INSERT INTO users (name, age) VALUES (:v1, :v2)",
			shouldContain: []string{"insert", "into users", "values (?, ?)"},
		},
		{
			name:          "insert ignore",
			input:         "INSERT IGNORE INTO users (name) VALUES ('Alice')",
			shouldContain: []string{"insert or ignore", "into users", "'alice'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			serializer := &SQLiteSerializer{}
			result := serializer.Serialize(stmt)
			lowerResult := strings.ToLower(result)

			for _, expected := range tt.shouldContain {
				if !strings.Contains(lowerResult, expected) {
					t.Errorf("expected output to contain %q (case-insensitive), got: %s", expected, result)
				}
			}
		})
	}
}
