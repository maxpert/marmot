package query

import (
	"testing"
)

// TestPipelineLiteralExtraction verifies that literal extraction works when enabled
func TestPipelineLiteralExtraction(t *testing.T) {
	pipeline, err := NewPipeline(1000, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name            string
		sql             string
		expectSQL       string
		expectParams    int
		expectParamVals []interface{}
	}{
		{
			name:            "INSERT with string and integer literals",
			sql:             "INSERT INTO users (name, age) VALUES ('alice', 25)",
			expectSQL:       "INSERT INTO users (name, age) VALUES (?, ?)",
			expectParams:    2,
			expectParamVals: []interface{}{"alice", int64(25)},
		},
		{
			name:            "UPDATE with literals",
			sql:             "UPDATE users SET name = 'bob', age = 30 WHERE id = 5",
			expectSQL:       "UPDATE users SET name = ?, age = ? WHERE id = ?",
			expectParams:    3,
			expectParamVals: []interface{}{"bob", int64(30), int64(5)},
		},
		{
			name:            "SELECT with literals",
			sql:             "SELECT * FROM users WHERE age > 18 AND name = 'test'",
			expectSQL:       "SELECT * FROM users WHERE age > ? and name = ?",
			expectParams:    2,
			expectParamVals: []interface{}{int64(18), "test"},
		},
		{
			name:            "DELETE with literals",
			sql:             "DELETE FROM users WHERE id = 100",
			expectSQL:       "DELETE FROM users WHERE id = ?",
			expectParams:    1,
			expectParamVals: []interface{}{int64(100)},
		},
		{
			name:            "INSERT with mixed types including float",
			sql:             "INSERT INTO data (str, num, flt) VALUES ('text', 42, 3.14)",
			expectSQL:       "INSERT INTO data (str, num, flt) VALUES (?, ?, ?)",
			expectParams:    3,
			expectParamVals: []interface{}{"text", int64(42), float64(3.14)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			ctx.ExtractLiterals = true // Enable literal extraction

			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if !ctx.Output.IsValid {
				t.Fatalf("Query validation failed: %v", ctx.Output.ValidationErr)
			}

			if len(ctx.Output.Statements) == 0 {
				t.Fatal("No statements produced")
			}

			stmt := ctx.Output.Statements[0]

			// Verify SQL has placeholders
			if stmt.SQL != tt.expectSQL {
				t.Errorf("SQL mismatch\nGot:      %s\nExpected: %s", stmt.SQL, tt.expectSQL)
			}

			// Verify params were extracted
			if len(stmt.Params) != tt.expectParams {
				t.Errorf("Expected %d params, got %d", tt.expectParams, len(stmt.Params))
			}

			// Verify param values
			for i, expected := range tt.expectParamVals {
				if i >= len(stmt.Params) {
					break
				}

				actual := stmt.Params[i]

				// Type-specific comparisons
				switch exp := expected.(type) {
				case string:
					if act, ok := actual.(string); ok {
						if act != exp {
							t.Errorf("Param[%d]: expected string(%q), got string(%q)", i, exp, act)
						}
					} else {
						t.Errorf("Param[%d]: expected string, got %T", i, actual)
					}
				case []byte:
					if act, ok := actual.([]byte); ok {
						if string(act) != string(exp) {
							t.Errorf("Param[%d]: expected []byte(%q), got []byte(%q)", i, string(exp), string(act))
						}
					} else {
						t.Errorf("Param[%d]: expected []byte, got %T", i, actual)
					}
				case int64:
					if act, ok := actual.(int64); ok {
						if act != exp {
							t.Errorf("Param[%d]: expected int64(%d), got int64(%d)", i, exp, act)
						}
					} else {
						t.Errorf("Param[%d]: expected int64, got %T", i, actual)
					}
				case float64:
					if act, ok := actual.(float64); ok {
						if act != exp {
							t.Errorf("Param[%d]: expected float64(%f), got float64(%f)", i, exp, act)
						}
					} else {
						t.Errorf("Param[%d]: expected float64, got %T", i, actual)
					}
				}
			}
		})
	}
}

// TestPipelineLiteralExtraction_Disabled verifies that literals remain when extraction is disabled
func TestPipelineLiteralExtraction_Disabled(t *testing.T) {
	pipeline, err := NewPipeline(1000, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	sql := "INSERT INTO users (name, age) VALUES ('alice', 25)"
	ctx := NewContext(sql, nil)
	// ExtractLiterals defaults to false

	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	if len(ctx.Output.Statements) == 0 {
		t.Fatal("No statements produced")
	}

	stmt := ctx.Output.Statements[0]

	// Verify literals remain in SQL
	if stmt.SQL != "INSERT INTO users (name, age) VALUES ('alice', 25)" {
		t.Errorf("Expected literals to remain in SQL, got: %s", stmt.SQL)
	}

	// Verify no params were extracted
	if len(stmt.Params) != 0 {
		t.Errorf("Expected 0 params, got %d", len(stmt.Params))
	}
}

// TestPipelineLiteralExtraction_WithExistingParams verifies that extraction is skipped when params provided
func TestPipelineLiteralExtraction_WithExistingParams(t *testing.T) {
	pipeline, err := NewPipeline(1000, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	sql := "INSERT INTO users (name, age) VALUES (?, ?)"
	params := []interface{}{"bob", 30}
	ctx := NewContext(sql, params)
	ctx.ExtractLiterals = true // Enable extraction

	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	if len(ctx.Output.Statements) == 0 {
		t.Fatal("No statements produced")
	}

	stmt := ctx.Output.Statements[0]

	// Verify placeholders remain
	if stmt.SQL != "INSERT INTO users (name, age) VALUES (?, ?)" {
		t.Errorf("Expected placeholders to remain, got: %s", stmt.SQL)
	}

	// Input.Parameters are passed through to Output.Statements[0].Params
	// Verify params are passed through from input
	if len(stmt.Params) != 2 {
		t.Errorf("Expected 2 params (from Input.Parameters), got %d", len(stmt.Params))
	}
}

// TestPipelineLiteralExtraction_DDL verifies that DDL statements don't get literal extraction
func TestPipelineLiteralExtraction_DDL(t *testing.T) {
	pipeline, err := NewPipeline(1000, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	sql := "CREATE TABLE users (id INT, name VARCHAR(100) DEFAULT 'guest')"
	ctx := NewContext(sql, nil)
	ctx.ExtractLiterals = true // Enable extraction

	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	if len(ctx.Output.Statements) == 0 {
		t.Fatal("No statements produced")
	}

	stmt := ctx.Output.Statements[0]

	// Verify literal 'guest' remains in SQL (DDL should not have literals extracted)
	if !contains(stmt.SQL, "guest") {
		t.Errorf("Expected 'guest' literal to remain in DDL, got: %s", stmt.SQL)
	}

	// Verify no params were extracted
	if len(stmt.Params) != 0 {
		t.Errorf("Expected 0 params for DDL, got %d", len(stmt.Params))
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
