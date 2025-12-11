package protocol

import (
	"testing"

	"github.com/maxpert/marmot/protocol/query"
)

// TestDetectFoundRowsFunction tests parser detection of FOUND_ROWS() function calls
func TestDetectFoundRowsFunction(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		shouldDetect bool
	}{
		{
			name:         "basic FOUND_ROWS()",
			sql:          "SELECT FOUND_ROWS()",
			shouldDetect: true,
		},
		{
			name:         "FOUND_ROWS() with alias",
			sql:          "SELECT FOUND_ROWS() AS total",
			shouldDetect: true,
		},
		{
			name:         "FOUND_ROWS() mixed with other expressions",
			sql:          "SELECT 1, FOUND_ROWS()",
			shouldDetect: true,
		},
		{
			name:         "lowercase found_rows()",
			sql:          "select found_rows()",
			shouldDetect: true,
		},
		{
			name:         "FOUND_ROWS() with mixed case",
			sql:          "SeLeCt FoUnD_RoWs()",
			shouldDetect: true,
		},
		{
			name:         "FOUND_ROWS() with AS keyword",
			sql:          "SELECT FOUND_ROWS() AS total_count",
			shouldDetect: true,
		},
		{
			name:         "FOUND_ROWS() with other columns",
			sql:          "SELECT FOUND_ROWS(), 'test', 123",
			shouldDetect: true,
		},
		{
			name:         "regular SELECT should not detect FOUND_ROWS",
			sql:          "SELECT * FROM users",
			shouldDetect: false,
		},
		{
			name:         "system variable should not be FOUND_ROWS",
			sql:          "SELECT @@version",
			shouldDetect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := query.NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			ctx := query.NewContext(tt.sql, nil)
			err = pipeline.Process(ctx)
			if err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}

			// Check if FOUND_ROWS() was detected
			hasFoundRows := false
			for _, varName := range ctx.SystemVarNames {
				if varName == "FOUND_ROWS()" {
					hasFoundRows = true
					break
				}
			}

			if tt.shouldDetect && !hasFoundRows {
				t.Errorf("Expected FOUND_ROWS() to be detected in SystemVarNames, got: %v", ctx.SystemVarNames)
			}

			if !tt.shouldDetect && hasFoundRows {
				t.Errorf("FOUND_ROWS() should not be detected, but found in SystemVarNames: %v", ctx.SystemVarNames)
			}

			// If FOUND_ROWS() is detected, statement type should be StatementSystemVariable
			if hasFoundRows && ctx.StatementType != query.StatementSystemVariable {
				t.Errorf("StatementType = %d, want StatementSystemVariable (%d)", ctx.StatementType, query.StatementSystemVariable)
			}
		})
	}
}

// foundRowsHandlerTest defines test cases for FOUND_ROWS() handler behavior
type foundRowsHandlerTest struct {
	name             string
	sessionFoundRows int64
	query            string
	expectedResult   int64
	expectedColumns  []string
}

// TestFoundRowsHandlerBehavior tests the expected behavior of FOUND_ROWS() handler
// This test defines the contract that the handler implementation must follow
func TestFoundRowsHandlerBehavior(t *testing.T) {
	tests := []foundRowsHandlerTest{
		{
			name:             "basic FOUND_ROWS()",
			sessionFoundRows: 100,
			query:            "SELECT FOUND_ROWS()",
			expectedResult:   100,
			expectedColumns:  []string{"FOUND_ROWS()"},
		},
		{
			name:             "FOUND_ROWS() with alias",
			sessionFoundRows: 42,
			query:            "SELECT FOUND_ROWS() AS total_count",
			expectedResult:   42,
			expectedColumns:  []string{"total_count"},
		},
		{
			name:             "FOUND_ROWS() with zero rows",
			sessionFoundRows: 0,
			query:            "SELECT FOUND_ROWS()",
			expectedResult:   0,
			expectedColumns:  []string{"FOUND_ROWS()"},
		},
		{
			name:             "FOUND_ROWS() with large count",
			sessionFoundRows: 999999,
			query:            "SELECT FOUND_ROWS()",
			expectedResult:   999999,
			expectedColumns:  []string{"FOUND_ROWS()"},
		},
		{
			name:             "lowercase found_rows()",
			sessionFoundRows: 50,
			query:            "select found_rows()",
			expectedResult:   50,
			expectedColumns:  []string{"found_rows()"},
		},
		{
			name:             "FOUND_ROWS() with AS keyword and alias",
			sessionFoundRows: 25,
			query:            "SELECT FOUND_ROWS() AS row_count",
			expectedResult:   25,
			expectedColumns:  []string{"row_count"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test parsing to ensure query is properly detected
			pipeline, err := query.NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			ctx := query.NewContext(tt.query, nil)
			err = pipeline.Process(ctx)
			if err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}

			// Verify FOUND_ROWS() is detected as system variable
			hasFoundRows := false
			for _, varName := range ctx.SystemVarNames {
				if varName == "FOUND_ROWS()" {
					hasFoundRows = true
					break
				}
			}

			if !hasFoundRows {
				t.Errorf("FOUND_ROWS() not detected in SystemVarNames: %v", ctx.SystemVarNames)
			}

			if ctx.StatementType != query.StatementSystemVariable {
				t.Errorf("StatementType = %d, want StatementSystemVariable (%d)", ctx.StatementType, query.StatementSystemVariable)
			}

			// Handler implementation will be tested here once implemented
			// Expected behavior:
			// 1. Handler should NOT execute SQLite query
			// 2. Handler should return session.FoundRowsCount
			// 3. Column names should match expectedColumns
			// 4. Result value should match expectedResult
			t.Logf("Handler contract: session.FoundRowsCount=%d should return %d with columns %v",
				tt.sessionFoundRows, tt.expectedResult, tt.expectedColumns)
		})
	}
}

// TestFoundRowsEdgeCases tests edge cases for FOUND_ROWS() handling
func TestFoundRowsEdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		query            string
		sessionFoundRows int64
		expectedResult   int64
		description      string
	}{
		{
			name:             "FOUND_ROWS() without prior SELECT",
			query:            "SELECT FOUND_ROWS()",
			sessionFoundRows: 0,
			expectedResult:   0,
			description:      "FOUND_ROWS() should return 0 when called without prior SELECT",
		},
		{
			name:             "FOUND_ROWS() after successful SELECT",
			query:            "SELECT FOUND_ROWS()",
			sessionFoundRows: 15,
			expectedResult:   15,
			description:      "FOUND_ROWS() should return count from previous SELECT",
		},
		{
			name:             "FOUND_ROWS() multiple calls",
			query:            "SELECT FOUND_ROWS()",
			sessionFoundRows: 42,
			expectedResult:   42,
			description:      "Multiple FOUND_ROWS() calls should return same value until next SELECT",
		},
		{
			name:             "FOUND_ROWS() with negative edge (should not occur)",
			query:            "SELECT FOUND_ROWS()",
			sessionFoundRows: -1,
			expectedResult:   -1,
			description:      "Handler should return session value even if negative (error case)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := query.NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			ctx := query.NewContext(tt.query, nil)
			err = pipeline.Process(ctx)
			if err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}

			// Verify FOUND_ROWS() is detected
			hasFoundRows := false
			for _, varName := range ctx.SystemVarNames {
				if varName == "FOUND_ROWS()" {
					hasFoundRows = true
					break
				}
			}

			if !hasFoundRows {
				t.Errorf("FOUND_ROWS() not detected for test case: %s", tt.description)
			}

			t.Logf("%s - Expected behavior: session.FoundRowsCount=%d → return %d",
				tt.description, tt.sessionFoundRows, tt.expectedResult)
		})
	}
}

// TestFoundRowsMixedQueries tests FOUND_ROWS() mixed with other query types
func TestFoundRowsMixedQueries(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		shouldDetect bool
		description  string
	}{
		{
			name:         "FOUND_ROWS() with constant",
			query:        "SELECT 1, FOUND_ROWS()",
			shouldDetect: true,
			description:  "FOUND_ROWS() mixed with constant expression",
		},
		{
			name:         "FOUND_ROWS() with string",
			query:        "SELECT 'total', FOUND_ROWS()",
			shouldDetect: true,
			description:  "FOUND_ROWS() mixed with string literal",
		},
		{
			name:         "FOUND_ROWS() with multiple aliases",
			query:        "SELECT FOUND_ROWS() AS cnt, 'test' AS label",
			shouldDetect: true,
			description:  "FOUND_ROWS() with multiple aliased expressions",
		},
		{
			name:         "FOUND_ROWS() in subquery is not a system var query",
			query:        "SELECT * FROM (SELECT FOUND_ROWS()) AS t",
			shouldDetect: false,
			description:  "FOUND_ROWS() in subquery - outer query is regular SELECT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := query.NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			ctx := query.NewContext(tt.query, nil)
			err = pipeline.Process(ctx)
			if err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}

			hasFoundRows := false
			for _, varName := range ctx.SystemVarNames {
				if varName == "FOUND_ROWS()" {
					hasFoundRows = true
					break
				}
			}

			if tt.shouldDetect && !hasFoundRows {
				t.Errorf("%s: Expected FOUND_ROWS() to be detected", tt.description)
			}

			if !tt.shouldDetect && hasFoundRows {
				t.Errorf("%s: FOUND_ROWS() should not be detected", tt.description)
			}

			t.Logf("%s - Detection: %v", tt.description, hasFoundRows)
		})
	}
}

// TestFoundRowsResultSetFormat tests expected result set format
func TestFoundRowsResultSetFormat(t *testing.T) {
	tests := []struct {
		name               string
		query              string
		sessionFoundRows   int64
		expectedColumnName string
		description        string
	}{
		{
			name:               "default column name",
			query:              "SELECT FOUND_ROWS()",
			sessionFoundRows:   100,
			expectedColumnName: "FOUND_ROWS()",
			description:        "Default column name should be 'FOUND_ROWS()'",
		},
		{
			name:               "aliased column name",
			query:              "SELECT FOUND_ROWS() AS total",
			sessionFoundRows:   50,
			expectedColumnName: "total",
			description:        "Aliased column name should be used",
		},
		{
			name:               "aliased with AS keyword",
			query:              "SELECT FOUND_ROWS() AS row_count",
			sessionFoundRows:   25,
			expectedColumnName: "row_count",
			description:        "Column alias with AS keyword",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query
			pipeline, err := query.NewPipeline(100, nil)
			if err != nil {
				t.Fatalf("failed to create pipeline: %v", err)
			}

			ctx := query.NewContext(tt.query, nil)
			err = pipeline.Process(ctx)
			if err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}

			// Verify detection
			hasFoundRows := false
			for _, varName := range ctx.SystemVarNames {
				if varName == "FOUND_ROWS()" {
					hasFoundRows = true
					break
				}
			}

			if !hasFoundRows {
				t.Errorf("FOUND_ROWS() not detected for test: %s", tt.description)
			}

			// Expected result set format (to be implemented by handler):
			// ResultSet{
			//   Columns: []ColumnDef{{Name: expectedColumnName, Type: 0x08}},
			//   Rows:    [][]interface{}{{sessionFoundRows}},
			// }
			t.Logf("%s - Expected column: %s, value: %d",
				tt.description, tt.expectedColumnName, tt.sessionFoundRows)
		})
	}
}
