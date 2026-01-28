package transform

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestLastInsertIDRule(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
		applies  bool
	}{
		{
			name:     "Simple LAST_INSERT_ID in SELECT",
			sql:      "SELECT LAST_INSERT_ID()",
			expected: "SELECT last_insert_rowid() FROM dual",
			applies:  true,
		},
		{
			name:     "LAST_INSERT_ID in INSERT VALUES",
			sql:      "INSERT INTO wp_usermeta (user_id, meta_key) VALUES (LAST_INSERT_ID(), 'foo')",
			expected: "INSERT INTO wp_usermeta (user_id, meta_key) VALUES (last_insert_rowid(), 'foo')",
			applies:  true,
		},
		{
			name:     "LAST_INSERT_ID lowercase",
			sql:      "SELECT last_insert_id()",
			expected: "SELECT last_insert_rowid() FROM dual",
			applies:  true,
		},
		{
			name:     "Mixed case LAST_INSERT_ID",
			sql:      "SELECT Last_Insert_Id()",
			expected: "SELECT last_insert_rowid() FROM dual",
			applies:  true,
		},
		{
			name:     "No LAST_INSERT_ID - should not apply",
			sql:      "SELECT * FROM users",
			expected: "",
			applies:  false,
		},
		{
			name:     "Multiple LAST_INSERT_ID calls",
			sql:      "SELECT LAST_INSERT_ID(), LAST_INSERT_ID()",
			expected: "SELECT last_insert_rowid(), last_insert_rowid() FROM dual",
			applies:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			stmt, err := parser.Parse(tc.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			rule := &LastInsertIDRule{}
			serializer := &SQLiteSerializer{}

			results, err := rule.Transform(stmt, nil, nil, "", serializer)

			if !tc.applies {
				if err != ErrRuleNotApplicable {
					t.Errorf("Expected ErrRuleNotApplicable, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(results))
			}

			if results[0].SQL != tc.expected {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", tc.expected, results[0].SQL)
			}
		})
	}
}
