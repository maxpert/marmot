package transform

import (
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestDeleteJoinRule_Name(t *testing.T) {
	rule := &DeleteJoinRule{}
	if rule.Name() != "DeleteJoin" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "DeleteJoin")
	}
}

func TestDeleteJoinRule_Priority(t *testing.T) {
	rule := &DeleteJoinRule{}
	if rule.Priority() != 30 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 30)
	}
}

func TestDeleteJoinRule_Transform(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantApplied  bool
		wantError    bool
		wantNumStmts int
		checkOutput  func([]TranspiledStatement) error
	}{
		{
			name:         "simple DELETE with JOIN",
			input:        "DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t2.status = 'old'",
			wantApplied:  true,
			wantNumStmts: 1,
			checkOutput: func(stmts []TranspiledStatement) error {
				if len(stmts) != 1 {
					return fmt.Errorf("expected 1 statement, got %d", len(stmts))
				}
				output := stmts[0].SQL
				lowerOutput := strings.ToLower(output)
				if !strings.Contains(lowerOutput, "rowid in") {
					return fmt.Errorf("expected 'rowid in' in output, got: %s", output)
				}
				if !strings.Contains(lowerOutput, "select") {
					return fmt.Errorf("expected subquery SELECT in output, got: %s", output)
				}
				return nil
			},
		},
		{
			name:         "DELETE with table alias",
			input:        "DELETE a FROM accounts a JOIN users u ON a.user_id = u.id WHERE u.deleted = 1",
			wantApplied:  true,
			wantNumStmts: 1,
			checkOutput: func(stmts []TranspiledStatement) error {
				if len(stmts) != 1 {
					return fmt.Errorf("expected 1 statement, got %d", len(stmts))
				}
				output := stmts[0].SQL
				lowerOutput := strings.ToLower(output)
				if !strings.Contains(lowerOutput, "rowid in") {
					return fmt.Errorf("expected 'rowid in' in output, got: %s", output)
				}
				return nil
			},
		},
		{
			name:        "simple DELETE without JOIN",
			input:       "DELETE FROM users WHERE status = 'inactive'",
			wantApplied: false,
		},
		{
			name:         "multi-table DELETE with two targets",
			input:        "DELETE a, b FROM wp_options a, wp_options b WHERE a.option_name = 'test' AND b.option_name = 'test2'",
			wantApplied:  true,
			wantNumStmts: 2,
			checkOutput: func(stmts []TranspiledStatement) error {
				if len(stmts) != 2 {
					return fmt.Errorf("expected 2 statements, got %d", len(stmts))
				}
				output := stmts[0].SQL
				lowerOutput := strings.ToLower(output)
				if !strings.Contains(lowerOutput, "rowid in") {
					return fmt.Errorf("expected 'rowid in' in output, got: %s", output)
				}
				if !strings.Contains(lowerOutput, "select a.rowid") {
					return fmt.Errorf("expected 'select a.rowid' in output, got: %s", output)
				}
				return nil
			},
		},
		{
			name:        "not a DELETE",
			input:       "SELECT * FROM users",
			wantApplied: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &DeleteJoinRule{}
			results, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if !tt.wantApplied {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got: %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}

			if len(results) != tt.wantNumStmts {
				t.Errorf("got %d statements, want %d", len(results), tt.wantNumStmts)
			}

			if tt.checkOutput != nil {
				if err := tt.checkOutput(results); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

func TestDeleteJoinRule_PreservesTargetTable(t *testing.T) {
	input := "DELETE orders FROM orders JOIN customers ON orders.customer_id = customers.id WHERE customers.inactive = 1"
	stmt, _ := sqlparser.NewTestParser().Parse(input)

	rule := &DeleteJoinRule{}
	results, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(results))
	}

	output := results[0].SQL
	lowerOutput := strings.ToLower(output)
	if !strings.Contains(lowerOutput, "delete from orders") {
		t.Errorf("expected 'delete from orders' in output, got: %s", output)
	}
}

func TestDeleteJoinRule_MultiTableDelete(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantNumStmts int
		checkResults func([]TranspiledStatement) error
	}{
		{
			name:         "multi-table DELETE with two targets same table",
			input:        "DELETE a, b FROM wp_options a, wp_options b WHERE a.option_name = 'test'",
			wantNumStmts: 2,
			checkResults: func(results []TranspiledStatement) error {
				if len(results) != 2 {
					return fmt.Errorf("expected 2 statements, got %d", len(results))
				}

				// Check first statement
				lowerOutput := strings.ToLower(results[0].SQL)
				if !strings.Contains(lowerOutput, "delete from wp_options") {
					return fmt.Errorf("expected 'DELETE FROM wp_options' in first stmt, got: %s", results[0].SQL)
				}
				if !strings.Contains(lowerOutput, "rowid in") {
					return fmt.Errorf("expected 'rowid in' in first stmt, got: %s", results[0].SQL)
				}
				if !strings.Contains(lowerOutput, "select a.rowid") {
					return fmt.Errorf("expected 'select a.rowid' in first stmt, got: %s", results[0].SQL)
				}

				// Check second statement
				lowerStmt := strings.ToLower(results[1].SQL)
				if !strings.Contains(lowerStmt, "delete from wp_options") {
					return fmt.Errorf("expected 'DELETE FROM wp_options' in second stmt, got: %s", results[1].SQL)
				}
				if !strings.Contains(lowerStmt, "select b.rowid") {
					return fmt.Errorf("expected 'select b.rowid' in second stmt, got: %s", results[1].SQL)
				}
				return nil
			},
		},
		{
			name:         "multi-table DELETE with three targets",
			input:        "DELETE t1, t2, t3 FROM table1 t1, table2 t2, table3 t3 WHERE t1.id = t2.ref_id",
			wantNumStmts: 3,
			checkResults: func(results []TranspiledStatement) error {
				if len(results) != 3 {
					return fmt.Errorf("expected 3 statements, got %d", len(results))
				}

				// Check first statement
				lowerOutput := strings.ToLower(results[0].SQL)
				if !strings.Contains(lowerOutput, "delete from table1") {
					return fmt.Errorf("expected 'DELETE FROM table1' in first stmt, got: %s", results[0].SQL)
				}
				if !strings.Contains(lowerOutput, "select t1.rowid") {
					return fmt.Errorf("expected 'select t1.rowid' in first stmt, got: %s", results[0].SQL)
				}

				// Check second statement
				lowerStmt1 := strings.ToLower(results[1].SQL)
				if !strings.Contains(lowerStmt1, "delete from table2") {
					return fmt.Errorf("expected 'DELETE FROM table2' in second stmt, got: %s", results[1].SQL)
				}
				if !strings.Contains(lowerStmt1, "select t2.rowid") {
					return fmt.Errorf("expected 'select t2.rowid' in second stmt, got: %s", results[1].SQL)
				}

				// Check third statement
				lowerStmt2 := strings.ToLower(results[2].SQL)
				if !strings.Contains(lowerStmt2, "delete from table3") {
					return fmt.Errorf("expected 'DELETE FROM table3' in third stmt, got: %s", results[2].SQL)
				}
				if !strings.Contains(lowerStmt2, "select t3.rowid") {
					return fmt.Errorf("expected 'select t3.rowid' in third stmt, got: %s", results[2].SQL)
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

			rule := &DeleteJoinRule{}
			results, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}

			if len(results) != tt.wantNumStmts {
				t.Fatalf("got %d statements, want %d", len(results), tt.wantNumStmts)
			}

			if tt.checkResults != nil {
				if err := tt.checkResults(results); err != nil {
					t.Error(err)
				}
			}
		})
	}
}
