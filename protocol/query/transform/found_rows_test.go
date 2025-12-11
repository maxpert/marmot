//go:build sqlite_preupdate_hook

package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestSQLCalcFoundRowsRule_Name(t *testing.T) {
	rule := &SQLCalcFoundRowsRule{}
	if rule.Name() != "SQLCalcFoundRows" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "SQLCalcFoundRows")
	}
}

func TestSQLCalcFoundRowsRule_Priority(t *testing.T) {
	rule := &SQLCalcFoundRowsRule{}
	if rule.Priority() != 10 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 10)
	}
}

func TestSQLCalcFoundRowsRule_Transform(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantModified bool
		checkOutput  func(t *testing.T, sql string)
	}{
		{
			name:         "basic SQL_CALC_FOUND_ROWS with explicit columns",
			input:        "SELECT SQL_CALC_FOUND_ROWS id, name FROM users LIMIT 2",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
				if !strings.Contains(sql, "__marmot_found_rows") {
					t.Errorf("expected __marmot_found_rows alias, got: %s", sql)
				}
				// Verify column order: id, name, COUNT(*) OVER() AS __marmot_found_rows
				// Handle both quoted and unquoted column names
				if !strings.Contains(sql, "id,") || !(strings.Contains(sql, "name,") || strings.Contains(sql, "`name`,")) {
					t.Errorf("original columns should be preserved, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with SELECT *",
			input:        "SELECT SQL_CALC_FOUND_ROWS * FROM posts LIMIT 1",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
				if !strings.Contains(sql, "__marmot_found_rows") {
					t.Errorf("expected __marmot_found_rows alias, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with WHERE clause",
			input:        "SELECT SQL_CALC_FOUND_ROWS id, status FROM posts WHERE status = 'publish' LIMIT 5",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
				// Handle both quoted and unquoted column names
				if !strings.Contains(sql, "WHERE") || !strings.Contains(sql, "= 'publish'") {
					t.Errorf("WHERE clause should be preserved, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with ORDER BY",
			input:        "SELECT SQL_CALC_FOUND_ROWS id, created_at FROM users ORDER BY created_at DESC LIMIT 10",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
				if !strings.Contains(sql, "ORDER BY created_at") {
					t.Errorf("ORDER BY clause should be preserved, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with complex WHERE and ORDER BY",
			input:        "SELECT SQL_CALC_FOUND_ROWS u.id, u.name FROM users u WHERE u.active = 1 AND u.age > 18 ORDER BY u.name ASC LIMIT 20 OFFSET 10",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
				if !strings.Contains(sql, "__marmot_found_rows") {
					t.Errorf("expected __marmot_found_rows alias, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with JOIN",
			input:        "SELECT SQL_CALC_FOUND_ROWS u.id, p.title FROM users u JOIN posts p ON u.id = p.author_id LIMIT 5",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with DISTINCT",
			input:        "SELECT DISTINCT SQL_CALC_FOUND_ROWS category FROM products LIMIT 10",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
				if !strings.Contains(sql, "DISTINCT") {
					t.Errorf("DISTINCT should be preserved, got: %s", sql)
				}
			},
		},
		{
			name:         "regular SELECT without hint",
			input:        "SELECT id, name FROM users LIMIT 3",
			wantModified: false,
		},
		{
			name:         "UPDATE statement",
			input:        "UPDATE users SET name = 'Alice' WHERE id = 1",
			wantModified: false,
		},
		{
			name:         "INSERT statement",
			input:        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
			wantModified: false,
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with UNION should not be transformed",
			input:        "SELECT SQL_CALC_FOUND_ROWS id FROM users UNION SELECT id FROM admins",
			wantModified: false,
			checkOutput: func(t *testing.T, sql string) {
				// UNION queries should reject SQL_CALC_FOUND_ROWS
				// The hint should either be ignored or cause a graceful degradation
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with subquery in FROM",
			input:        "SELECT SQL_CALC_FOUND_ROWS * FROM (SELECT id, name FROM users WHERE active = 1) AS u LIMIT 5",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS with GROUP BY",
			input:        "SELECT SQL_CALC_FOUND_ROWS category, COUNT(*) FROM products GROUP BY category LIMIT 5",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				if strings.Contains(sql, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sql, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
				}
			},
		},
		{
			name:         "SQL_CALC_FOUND_ROWS case insensitive",
			input:        "select sql_calc_found_rows id, name from users limit 2",
			wantModified: true,
			checkOutput: func(t *testing.T, sql string) {
				sqlUpper := strings.ToUpper(sql)
				if strings.Contains(sqlUpper, "SQL_CALC_FOUND_ROWS") {
					t.Errorf("SQL_CALC_FOUND_ROWS hint should be stripped, got: %s", sql)
				}
				if !strings.Contains(sqlUpper, "COUNT(*) OVER()") {
					t.Errorf("expected COUNT(*) OVER() to be appended, got: %s", sql)
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

			rule := &SQLCalcFoundRowsRule{}
			serializer := &SQLiteSerializer{}
			result, err := rule.Transform(stmt, nil, nil, "testdb", serializer)

			if tt.wantModified {
				if err != nil {
					t.Fatalf("Transform failed: %v", err)
				}
				if result == nil {
					t.Fatal("expected result, got nil")
				}
				if len(result) != 1 {
					t.Fatalf("expected 1 transpiled statement, got %d", len(result))
				}
				if tt.checkOutput != nil {
					tt.checkOutput(t, result[0].SQL)
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got: %v", err)
				}
			}
		})
	}
}

func TestSQLCalcFoundRowsRule_PreservesParameters(t *testing.T) {
	input := "SELECT SQL_CALC_FOUND_ROWS id, name FROM users WHERE active = ? LIMIT ?"
	params := []interface{}{1, 10}

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &SQLCalcFoundRowsRule{}
	serializer := &SQLiteSerializer{}
	result, err := rule.Transform(stmt, params, nil, "testdb", serializer)

	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result == nil || len(result) == 0 {
		t.Fatal("expected result, got nil or empty")
	}

	// Parameters should be preserved unchanged
	if len(result[0].Params) != len(params) {
		t.Errorf("expected %d parameters, got %d", len(params), len(result[0].Params))
	}

	for i, param := range params {
		if i >= len(result[0].Params) {
			t.Errorf("parameter %d missing", i)
			continue
		}
		if result[0].Params[i] != param {
			t.Errorf("parameter %d: got %v, want %v", i, result[0].Params[i], param)
		}
	}
}

func TestSQLCalcFoundRowsRule_ThreadSafety(t *testing.T) {
	t.Parallel()

	input := "SELECT SQL_CALC_FOUND_ROWS id, name FROM users WHERE active = 1 LIMIT 10"
	rule := &SQLCalcFoundRowsRule{}

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()
			stmt, err := sqlparser.NewTestParser().Parse(input)
			if err != nil {
				t.Errorf("Parse failed: %v", err)
				return
			}
			serializer := &SQLiteSerializer{}
			result, err := rule.Transform(stmt, nil, nil, "testdb", serializer)
			if err != nil {
				t.Errorf("Transform failed: %v", err)
				return
			}
			if result == nil || len(result) == 0 {
				t.Error("expected result, got nil or empty")
				return
			}
			if strings.Contains(result[0].SQL, "SQL_CALC_FOUND_ROWS") {
				t.Errorf("SQL_CALC_FOUND_ROWS should be stripped: %s", result[0].SQL)
			}
			if !strings.Contains(result[0].SQL, "COUNT(*) OVER()") {
				t.Errorf("expected COUNT(*) OVER() in SQL: %s", result[0].SQL)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// Integration test cases for end-to-end behavior verification.
// These test cases document expected behavior but don't execute against a real database.
// They serve as specification for the actual integration tests.
type foundRowsIntegrationTestCase struct {
	name          string
	setupSQL      []string // CREATE TABLE, INSERT statements
	selectSQL     string   // SELECT SQL_CALC_FOUND_ROWS ... or regular SELECT
	foundRowsSQL  string   // SELECT FOUND_ROWS()
	expectedRows  int      // Rows returned by selectSQL
	expectedFound int64    // Value FOUND_ROWS() should return
	description   string   // What this test verifies
}

var foundRowsIntegrationTests = []foundRowsIntegrationTestCase{
	{
		name: "basic SQL_CALC_FOUND_ROWS with LIMIT",
		setupSQL: []string{
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Dave'), (5, 'Eve')",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS id, name FROM users LIMIT 2",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  2,
		expectedFound: 5,
		description:   "FOUND_ROWS() returns total matching rows (5) even though LIMIT returned only 2",
	},
	{
		name: "SQL_CALC_FOUND_ROWS with WHERE clause",
		setupSQL: []string{
			"CREATE TABLE posts (id INTEGER PRIMARY KEY, status TEXT)",
			"INSERT INTO posts VALUES (1, 'publish'), (2, 'draft'), (3, 'publish'), (4, 'publish')",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS * FROM posts WHERE status = 'publish' LIMIT 1",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  1,
		expectedFound: 3,
		description:   "FOUND_ROWS() respects WHERE clause - returns count of matching rows (3), not all rows (4)",
	},
	{
		name: "regular SELECT without hint overwrites FoundRowsCount",
		setupSQL: []string{
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			"INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		},
		selectSQL:     "SELECT * FROM users LIMIT 3",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  3,
		expectedFound: 3,
		description:   "Without SQL_CALC_FOUND_ROWS hint, FOUND_ROWS() returns actual rows returned (3)",
	},
	{
		name: "SQL_CALC_FOUND_ROWS with OFFSET",
		setupSQL: []string{
			"CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)",
			"INSERT INTO items VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS id FROM items LIMIT 2 OFFSET 2",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  2,
		expectedFound: 6,
		description:   "FOUND_ROWS() ignores OFFSET - returns total rows (6), not remaining rows",
	},
	{
		name: "SQL_CALC_FOUND_ROWS with zero results",
		setupSQL: []string{
			"CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT)",
			"INSERT INTO products VALUES (1, 'electronics'), (2, 'books')",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS * FROM products WHERE category = 'toys' LIMIT 10",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  0,
		expectedFound: 0,
		description:   "FOUND_ROWS() returns 0 when WHERE clause matches no rows",
	},
	{
		name: "SQL_CALC_FOUND_ROWS without LIMIT",
		setupSQL: []string{
			"CREATE TABLE data (id INTEGER PRIMARY KEY)",
			"INSERT INTO data VALUES (1), (2), (3)",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS * FROM data",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  3,
		expectedFound: 3,
		description:   "SQL_CALC_FOUND_ROWS without LIMIT: FOUND_ROWS() equals rows returned",
	},
	{
		name: "FOUND_ROWS() persists across multiple regular SELECTs in same session",
		setupSQL: []string{
			"CREATE TABLE test (id INTEGER PRIMARY KEY)",
			"INSERT INTO test VALUES (1), (2), (3), (4), (5)",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS * FROM test LIMIT 2",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  2,
		expectedFound: 5,
		description:   "After SQL_CALC_FOUND_ROWS, FOUND_ROWS() returns 5",
	},
	{
		name: "regular SELECT updates FOUND_ROWS count",
		setupSQL: []string{
			"CREATE TABLE test (id INTEGER PRIMARY KEY)",
			"INSERT INTO test VALUES (1), (2), (3)",
		},
		selectSQL:     "SELECT * FROM test",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  3,
		expectedFound: 3,
		description:   "Regular SELECT updates session's FoundRowsCount to rows returned",
	},
	{
		name: "SQL_CALC_FOUND_ROWS with JOIN and WHERE",
		setupSQL: []string{
			"CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
			"CREATE TABLE books (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT)",
			"INSERT INTO authors VALUES (1, 'Alice'), (2, 'Bob')",
			"INSERT INTO books VALUES (1, 1, 'Book A'), (2, 1, 'Book B'), (3, 2, 'Book C'), (4, 1, 'Book D')",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS b.title FROM books b JOIN authors a ON b.author_id = a.id WHERE a.name = 'Alice' LIMIT 2",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  2,
		expectedFound: 3,
		description:   "FOUND_ROWS() works with JOIN - returns total matching join results (3) despite LIMIT 2",
	},
	{
		name: "SQL_CALC_FOUND_ROWS with ORDER BY",
		setupSQL: []string{
			"CREATE TABLE scores (id INTEGER PRIMARY KEY, value INTEGER)",
			"INSERT INTO scores VALUES (1, 100), (2, 200), (3, 150), (4, 175), (5, 225)",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS id, value FROM scores ORDER BY value DESC LIMIT 3",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  3,
		expectedFound: 5,
		description:   "ORDER BY doesn't affect FOUND_ROWS() count - returns all rows (5), not limited rows (3)",
	},
	{
		name: "SQL_CALC_FOUND_ROWS with complex WHERE expression",
		setupSQL: []string{
			"CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER, active INTEGER)",
			"INSERT INTO users VALUES (1, 25, 1), (2, 30, 1), (3, 35, 0), (4, 28, 1), (5, 22, 1), (6, 40, 1)",
		},
		selectSQL:     "SELECT SQL_CALC_FOUND_ROWS id FROM users WHERE age > 20 AND age < 35 AND active = 1 LIMIT 2",
		foundRowsSQL:  "SELECT FOUND_ROWS()",
		expectedRows:  2,
		expectedFound: 3,
		description:   "Complex WHERE clause: FOUND_ROWS() counts all matching rows (id 1, 2, 4 = 3 rows)",
	},
}

// TestFoundRowsIntegrationSpec documents the expected behavior for integration tests.
// This is not an executable test - it serves as specification documentation.
func TestFoundRowsIntegrationSpec(t *testing.T) {
	t.Skip("This is a specification test - documents expected behavior for actual integration tests")

	for _, tc := range foundRowsIntegrationTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Description: %s", tc.description)
			t.Logf("Setup SQL: %v", tc.setupSQL)
			t.Logf("SELECT: %s", tc.selectSQL)
			t.Logf("FOUND_ROWS: %s", tc.foundRowsSQL)
			t.Logf("Expected rows returned: %d", tc.expectedRows)
			t.Logf("Expected FOUND_ROWS() value: %d", tc.expectedFound)
		})
	}
}

// TestFoundRowsQueryContextIntegration verifies that QueryContext.HasFoundRowsCalc
// is set correctly during transpilation. This is a unit test for the flag propagation.
func TestFoundRowsQueryContextIntegration(t *testing.T) {
	t.Skip("Requires QueryContext.HasFoundRowsCalc field to be implemented")

	tests := []struct {
		name            string
		sql             string
		expectHasFlag   bool
		expectTransform bool
	}{
		{
			name:            "SQL_CALC_FOUND_ROWS sets HasFoundRowsCalc flag",
			sql:             "SELECT SQL_CALC_FOUND_ROWS id FROM users LIMIT 10",
			expectHasFlag:   true,
			expectTransform: true,
		},
		{
			name:            "regular SELECT does not set HasFoundRowsCalc flag",
			sql:             "SELECT id FROM users LIMIT 10",
			expectHasFlag:   false,
			expectTransform: false,
		},
		{
			name:            "UPDATE does not set HasFoundRowsCalc flag",
			sql:             "UPDATE users SET name = 'test' WHERE id = 1",
			expectHasFlag:   false,
			expectTransform: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test will be implemented once QueryContext.HasFoundRowsCalc field exists
			// It should verify that the transpiler sets ctx.HasFoundRowsCalc = true
			// when SQL_CALC_FOUND_ROWS is detected and transformed
			t.Logf("SQL: %s", tt.sql)
			t.Logf("Expected HasFoundRowsCalc: %v", tt.expectHasFlag)
			t.Logf("Expected transformation: %v", tt.expectTransform)
		})
	}
}
