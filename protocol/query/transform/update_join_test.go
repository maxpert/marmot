package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestUpdateJoinRule_Name(t *testing.T) {
	rule := &UpdateJoinRule{}
	if rule.Name() != "UpdateJoin" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "UpdateJoin")
	}
}

func TestUpdateJoinRule_Priority(t *testing.T) {
	rule := &UpdateJoinRule{}
	if rule.Priority() != 40 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 40)
	}
}

func TestUpdateJoinRule_Transform(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantApplicable bool
		wantError      bool
		checkOutput    func(string) error
	}{
		{
			name:           "simple UPDATE with JOIN",
			input:          "UPDATE t1 JOIN t2 ON t1.id = t2.ref_id SET t1.name = t2.name WHERE t2.active = 1",
			wantApplicable: true,
			checkOutput: func(sql string) error {
				lowerSQL := strings.ToLower(sql)
				if !strings.Contains(lowerSQL, "rowid in") {
					t.Errorf("expected 'rowid in' in output, got: %s", sql)
				}
				if !strings.Contains(lowerSQL, "select") {
					t.Errorf("expected subquery SELECT in output, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:           "UPDATE with correlated subquery",
			input:          "UPDATE orders o JOIN customers c ON o.customer_id = c.id SET o.status = c.default_status WHERE c.active = 1",
			wantApplicable: true,
			checkOutput: func(sql string) error {
				lowerSQL := strings.ToLower(sql)
				if strings.Count(lowerSQL, "select") < 2 {
					t.Errorf("expected at least 2 SELECT (correlated + rowid subquery) in output, got: %s", sql)
				}
				if !strings.Contains(lowerSQL, "limit 1") {
					t.Errorf("expected LIMIT 1 in correlated subquery, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:           "UPDATE with self-referencing columns",
			input:          "UPDATE t1 JOIN t2 ON t1.id = t2.ref_id SET t1.count = t1.count + 1 WHERE t2.active = 1",
			wantApplicable: true,
			checkOutput: func(sql string) error {
				lowerSQL := strings.ToLower(sql)
				if !strings.Contains(lowerSQL, "rowid in") {
					t.Errorf("expected 'rowid in' in output, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:           "UPDATE with WHERE, ORDER BY, and LIMIT",
			input:          "UPDATE t1 JOIN t2 ON t1.id = t2.ref_id SET t1.value = t2.value WHERE t2.priority > 5 ORDER BY t1.created LIMIT 10",
			wantApplicable: true,
			checkOutput: func(sql string) error {
				lowerSQL := strings.ToLower(sql)
				if !strings.Contains(lowerSQL, "order by") {
					t.Errorf("expected ORDER BY preserved in subquery, got: %s", sql)
				}
				if !strings.Contains(lowerSQL, "limit") {
					t.Errorf("expected LIMIT preserved in subquery, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:           "simple UPDATE without JOIN",
			input:          "UPDATE users SET status = 'active' WHERE id = 1",
			wantApplicable: false,
		},
		{
			name:           "not an UPDATE",
			input:          "SELECT * FROM users",
			wantApplicable: false,
		},
		{
			name:           "case insensitive alias matching",
			input:          "UPDATE t1 AS T1 JOIN t2 ON T1.id = t2.ref_id SET T1.name = t2.name WHERE t2.active = 1",
			wantApplicable: true,
			checkOutput: func(sql string) error {
				lowerSQL := strings.ToLower(sql)
				if !strings.Contains(lowerSQL, "rowid in") {
					t.Errorf("expected 'rowid in' in output, got: %s", sql)
				}
				if !strings.Contains(lowerSQL, "select") {
					t.Errorf("expected subquery SELECT in output, got: %s", sql)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &UpdateJoinRule{}
			results, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if !tt.wantApplicable {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got: %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}

			if tt.checkOutput != nil {
				_ = tt.checkOutput(results[0].SQL)
			}
		})
	}
}

func TestUpdateJoinRule_PreservesTargetTable(t *testing.T) {
	input := "UPDATE orders JOIN customers ON orders.customer_id = customers.id SET orders.status = 'verified' WHERE customers.verified = 1"
	stmt, _ := sqlparser.NewTestParser().Parse(input)

	rule := &UpdateJoinRule{}
	results, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	lowerSQL := strings.ToLower(results[0].SQL)
	if !strings.Contains(lowerSQL, "update orders") {
		t.Errorf("expected 'update orders' in output, got: %s", results[0].SQL)
	}
}

func TestUpdateJoinRule_NeedsSubquery(t *testing.T) {
	rule := &UpdateJoinRule{}

	tests := []struct {
		name        string
		sql         string
		targetAlias string
		wantSubq    bool
	}{
		{
			name:        "reference to other table needs subquery",
			sql:         "UPDATE t1 JOIN t2 ON t1.id = t2.ref_id SET t1.name = t2.name",
			targetAlias: "t1",
			wantSubq:    true,
		},
		{
			name:        "self-reference doesn't need subquery",
			sql:         "UPDATE t1 JOIN t2 ON t1.id = t2.ref_id SET t1.count = t1.count + 1",
			targetAlias: "t1",
			wantSubq:    false,
		},
		{
			name:        "case insensitive self-reference doesn't need subquery",
			sql:         "UPDATE t1 AS T1 JOIN t2 ON T1.id = t2.ref_id SET T1.count = T1.count + 1",
			targetAlias: "t1",
			wantSubq:    false,
		},
		{
			name:        "case insensitive other table reference needs subquery",
			sql:         "UPDATE t1 AS T1 JOIN t2 ON T1.id = t2.ref_id SET T1.name = t2.name",
			targetAlias: "t1",
			wantSubq:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := sqlparser.NewTestParser().Parse(tt.sql)
			upd := stmt.(*sqlparser.Update)

			needsSubq := rule.needsSubquery(upd.Exprs[0].Expr, tt.targetAlias)
			if needsSubq != tt.wantSubq {
				t.Errorf("needsSubquery() = %v, want %v", needsSubq, tt.wantSubq)
			}
		})
	}
}
