package rules

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAutoIncrementZeroRule(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		applied  bool
	}{
		{
			name:     "id=0 should become NULL",
			input:    "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 1, 'test', 'pad')",
			expected: "insert into sbtest1(id, k, c, pad) values (null, 1, 'test', 'pad')",
			applied:  true,
		},
		{
			name:     "id=1 should not change",
			input:    "INSERT INTO sbtest1 (id, k, c, pad) VALUES (1, 1, 'test', 'pad')",
			expected: "insert into sbtest1(id, k, c, pad) values (1, 1, 'test', 'pad')",
			applied:  false,
		},
		{
			name:     "no id column should not change",
			input:    "INSERT INTO sbtest1 (k, c, pad) VALUES (1, 'test', 'pad')",
			expected: "insert into sbtest1(k, c, pad) values (1, 'test', 'pad')",
			applied:  false,
		},
		{
			name:     "multiple rows with id=0",
			input:    "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 1, 'a', 'b'), (0, 2, 'c', 'd')",
			expected: "insert into sbtest1(id, k, c, pad) values (null, 1, 'a', 'b'), (null, 2, 'c', 'd')",
			applied:  true,
		},
	}

	rule := &AutoIncrementZeroRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().ParseStrictDDL(tt.input)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			newStmt, applied, err := rule.ApplyAST(stmt)
			if err != nil {
				t.Fatalf("Rule error: %v", err)
			}

			if applied != tt.applied {
				t.Errorf("applied = %v, want %v", applied, tt.applied)
			}

			result := sqlparser.String(newStmt)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}
