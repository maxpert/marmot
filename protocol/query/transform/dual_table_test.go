package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestDualTableRule_Name(t *testing.T) {
	rule := &DualTableRule{}
	if rule.Name() != "DualTable" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "DualTable")
	}
}

func TestDualTableRule_Priority(t *testing.T) {
	rule := &DualTableRule{}
	if rule.Priority() != 1 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 1)
	}
}

func TestDualTableRule_Transform(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantModified bool
		wantSQL      string
	}{
		{
			name:         "SELECT 1 FROM dual",
			input:        "SELECT 1 FROM dual",
			wantModified: true,
			wantSQL:      "SELECT 1",
		},
		{
			name:         "SELECT @@version FROM dual",
			input:        "SELECT @@version FROM dual",
			wantModified: true,
			wantSQL:      "SELECT @@version",
		},
		{
			name:         "SELECT NOW() FROM dual",
			input:        "SELECT NOW() FROM dual",
			wantModified: true,
			wantSQL:      "SELECT now()",
		},
		{
			name:         "SELECT 1+1 FROM dual",
			input:        "SELECT 1+1 FROM dual",
			wantModified: true,
			wantSQL:      "SELECT 1 + 1",
		},
		{
			name:         "SELECT 'test' AS value FROM dual",
			input:        "SELECT 'test' AS value FROM dual",
			wantModified: true,
			wantSQL:      "SELECT 'test' as `value`",
		},
		{
			name:         "SELECT * FROM users (not dual)",
			input:        "SELECT * FROM users",
			wantModified: false,
		},
		{
			name:         "SELECT * FROM actual_table",
			input:        "SELECT * FROM actual_table",
			wantModified: false,
		},
		{
			name:         "SELECT * FROM dual_table (contains dual but not exact match)",
			input:        "SELECT * FROM dual_table",
			wantModified: false,
		},
		{
			name:         "SELECT * FROM users, dual (multiple tables)",
			input:        "SELECT * FROM users, dual",
			wantModified: false,
		},
		{
			name:         "INSERT INTO dual VALUES (1) (not SELECT)",
			input:        "INSERT INTO dual VALUES (1)",
			wantModified: false,
		},
	}

	parser := sqlparser.NewTestParser()
	serializer := &SQLiteSerializer{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &DualTableRule{}
			results, err := rule.Transform(stmt, nil, nil, "", serializer)

			if tt.wantModified {
				if err != nil {
					t.Fatalf("expected rule to apply, got error: %v", err)
				}
				if len(results) != 1 {
					t.Fatalf("expected 1 result, got %d", len(results))
				}

				got := strings.TrimSpace(results[0].SQL)
				want := strings.TrimSpace(tt.wantSQL)
				if got != want {
					t.Errorf("SQL mismatch:\ngot:  %q\nwant: %q", got, want)
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got: %v", err)
				}
			}
		})
	}
}

func TestDualTableRule_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"lowercase dual", "SELECT 1 FROM dual"},
		{"uppercase DUAL", "SELECT 1 FROM DUAL"},
		{"mixed Dual", "SELECT 1 FROM Dual"},
		{"mixed dUaL", "SELECT 1 FROM dUaL"},
	}

	parser := sqlparser.NewTestParser()
	serializer := &SQLiteSerializer{}
	rule := &DualTableRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			results, err := rule.Transform(stmt, nil, nil, "", serializer)
			if err != nil {
				t.Fatalf("expected rule to apply, got error: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}

			got := strings.TrimSpace(results[0].SQL)
			want := "SELECT 1"
			if got != want {
				t.Errorf("SQL mismatch:\ngot:  %q\nwant: %q", got, want)
			}
		})
	}
}
