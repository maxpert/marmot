package query

import (
	"testing"
)

// TestYCSB_LoadPhase tests YCSB load phase query patterns
func TestYCSB_LoadPhase(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name: "INSERT IGNORE with 11 fields (YCSB standard)",
			query: "INSERT IGNORE INTO usertable (YCSB_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			expected: "INSERT OR IGNORE INTO usertable (YCSB_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		},
		{
			name:     "INSERT IGNORE with multiple value sets",
			query:    "INSERT IGNORE INTO usertable (YCSB_KEY, FIELD0) VALUES ('user1', 'data1'), ('user2', 'data2')",
			expected: "INSERT OR IGNORE INTO usertable (YCSB_KEY, FIELD0) VALUES ('user1', 'data1'), ('user2', 'data2')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.TranspiledSQL != tt.expected {
				t.Errorf("Transpilation mismatch\nGot:      %s\nExpected: %s", ctx.TranspiledSQL, tt.expected)
			}

			if !ctx.IsValid {
				t.Errorf("Query validation failed: %v", ctx.ValidationErr)
			}

			if ctx.StatementType != StatementInsert {
				t.Errorf("Expected INSERT statement type, got %d", ctx.StatementType)
			}
		})
	}
}

// TestYCSB_ReadWorkload tests YCSB read workload patterns
func TestYCSB_ReadWorkload(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "SELECT by primary key",
			query:    "SELECT * FROM usertable WHERE YCSB_KEY = ?",
			expected: "SELECT * FROM usertable WHERE YCSB_KEY = ?",
		},
		{
			name:     "SELECT specific fields",
			query:    "SELECT FIELD0, FIELD1, FIELD2 FROM usertable WHERE YCSB_KEY = ?",
			expected: "SELECT FIELD0, FIELD1, FIELD2 FROM usertable WHERE YCSB_KEY = ?",
		},
		{
			name:     "SELECT with FORCE INDEX (should be removed)",
			query:    "SELECT * FROM usertable FORCE INDEX (PRIMARY) WHERE YCSB_KEY = ?",
			expected: "SELECT * FROM usertable WHERE YCSB_KEY = ?",
		},
		{
			name:     "SELECT with USE INDEX (should be removed)",
			query:    "SELECT * FROM usertable USE INDEX (idx_field0) WHERE FIELD0 = ?",
			expected: "SELECT * FROM usertable WHERE FIELD0 = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.TranspiledSQL != tt.expected {
				t.Errorf("Transpilation mismatch\nGot:      %s\nExpected: %s", ctx.TranspiledSQL, tt.expected)
			}

			if ctx.StatementType != StatementSelect {
				t.Errorf("Expected SELECT statement type, got %d", ctx.StatementType)
			}

			if !ctx.IsReadOnly {
				t.Errorf("Expected read-only query")
			}
		})
	}
}

// TestYCSB_UpdateWorkload tests YCSB update workload patterns
func TestYCSB_UpdateWorkload(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "UPDATE single field",
			query:    "UPDATE usertable SET FIELD0 = ? WHERE YCSB_KEY = ?",
			expected: "UPDATE usertable SET FIELD0 = ? WHERE YCSB_KEY = ?",
		},
		{
			name:     "UPDATE multiple fields",
			query:    "UPDATE usertable SET FIELD0 = ?, FIELD1 = ?, FIELD2 = ? WHERE YCSB_KEY = ?",
			expected: "UPDATE usertable SET FIELD0 = ?, FIELD1 = ?, FIELD2 = ? WHERE YCSB_KEY = ?",
		},
		{
			name:     "UPDATE with FORCE INDEX (should be removed)",
			query:    "UPDATE usertable FORCE INDEX (PRIMARY) SET FIELD0 = ? WHERE YCSB_KEY = ?",
			expected: "UPDATE usertable SET FIELD0 = ? WHERE YCSB_KEY = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.TranspiledSQL != tt.expected {
				t.Errorf("Transpilation mismatch\nGot:      %s\nExpected: %s", ctx.TranspiledSQL, tt.expected)
			}

			if ctx.StatementType != StatementUpdate {
				t.Errorf("Expected UPDATE statement type, got %d", ctx.StatementType)
			}

			if !ctx.IsMutation {
				t.Errorf("Expected mutation query")
			}
		})
	}
}

// TestYCSB_DeleteWorkload tests YCSB delete workload patterns (if used)
func TestYCSB_DeleteWorkload(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "DELETE by primary key",
			query:    "DELETE FROM usertable WHERE YCSB_KEY = ?",
			expected: "DELETE FROM usertable WHERE YCSB_KEY = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.TranspiledSQL != tt.expected {
				t.Errorf("Transpilation mismatch\nGot:      %s\nExpected: %s", ctx.TranspiledSQL, tt.expected)
			}

			if ctx.StatementType != StatementDelete {
				t.Errorf("Expected DELETE statement type, got %d", ctx.StatementType)
			}
		})
	}
}

// TestYCSB_ScanWorkload tests YCSB scan workload patterns
func TestYCSB_ScanWorkload(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "SELECT with LIMIT",
			query:    "SELECT * FROM usertable WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT ?",
			expected: "SELECT * FROM usertable WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT ?",
		},
		{
			name:     "SELECT with MySQL-style LIMIT offset,count",
			query:    "SELECT * FROM usertable WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT 10, 100",
			expected: "SELECT * FROM usertable WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT 100 OFFSET 10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.TranspiledSQL != tt.expected {
				t.Errorf("Transpilation mismatch\nGot:      %s\nExpected: %s", ctx.TranspiledSQL, tt.expected)
			}

			if ctx.StatementType != StatementSelect {
				t.Errorf("Expected SELECT statement type, got %d", ctx.StatementType)
			}
		})
	}
}

// TestYCSB_ComplexScenarios tests complex query patterns YCSB might generate
func TestYCSB_ComplexScenarios(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "INSERT IGNORE with escaped quotes in values",
			query:    "INSERT IGNORE INTO usertable (YCSB_KEY, FIELD0) VALUES ('user\\'s key', 'data\\'s value')",
			expected: "INSERT OR IGNORE INTO usertable (YCSB_KEY, FIELD0) VALUES ('user''s key', 'data''s value')",
		},
		{
			name:     "UPDATE with special characters",
			query:    "UPDATE usertable SET FIELD0 = 'O\\'Brien' WHERE YCSB_KEY = ?",
			expected: "UPDATE usertable SET FIELD0 = 'O''Brien' WHERE YCSB_KEY = ?",
		},
		{
			name:     "SELECT with FOR UPDATE (should be removed)",
			query:    "SELECT * FROM usertable WHERE YCSB_KEY = ? FOR UPDATE",
			expected: "SELECT * FROM usertable WHERE YCSB_KEY = ? ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.TranspiledSQL != tt.expected {
				t.Errorf("Transpilation mismatch\nGot:      %s\nExpected: %s", ctx.TranspiledSQL, tt.expected)
			}

			if !ctx.IsValid {
				t.Errorf("Query validation failed: %v", ctx.ValidationErr)
			}
		})
	}
}

// TestYCSB_PreparedStatementParameters tests parameter binding for YCSB workloads
func TestYCSB_PreparedStatementParameters(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name           string
		query          string
		expectedParams int
	}{
		{
			name:           "INSERT with 11 parameters (YCSB standard)",
			query:          "INSERT IGNORE INTO usertable (YCSB_KEY, FIELD0, FIELD1, FIELD2, FIELD3, FIELD4, FIELD5, FIELD6, FIELD7, FIELD8, FIELD9) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			expectedParams: 11,
		},
		{
			name:           "UPDATE with 4 parameters",
			query:          "UPDATE usertable SET FIELD0=?, FIELD1=?, FIELD2=? WHERE YCSB_KEY=?",
			expectedParams: 4,
		},
		{
			name:           "SELECT with 1 parameter",
			query:          "SELECT * FROM usertable WHERE YCSB_KEY=?",
			expectedParams: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			// Count ? placeholders in transpiled SQL
			paramCount := 0
			for _, ch := range ctx.TranspiledSQL {
				if ch == '?' {
					paramCount++
				}
			}

			if paramCount != tt.expectedParams {
				t.Errorf("Expected %d parameters, found %d in: %s", tt.expectedParams, paramCount, ctx.TranspiledSQL)
			}
		})
	}
}

// TestYCSB_TableCreation tests YCSB table creation DDL
func TestYCSB_TableCreation(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	query := `CREATE TABLE IF NOT EXISTS usertable (
		YCSB_KEY VARCHAR(255) PRIMARY KEY,
		FIELD0 TEXT,
		FIELD1 TEXT,
		FIELD2 TEXT,
		FIELD3 TEXT,
		FIELD4 TEXT,
		FIELD5 TEXT,
		FIELD6 TEXT,
		FIELD7 TEXT,
		FIELD8 TEXT,
		FIELD9 TEXT
	)`

	ctx := NewContext(query, nil)
	if err := pipeline.Process(ctx); err != nil {
		t.Fatalf("Pipeline processing failed: %v", err)
	}

	if ctx.StatementType != StatementDDL {
		t.Errorf("Expected DDL statement type, got %d", ctx.StatementType)
	}

	if ctx.TableName != "usertable" {
		t.Errorf("Expected table name 'usertable', got '%s'", ctx.TableName)
	}
}

// TestYCSB_CacheEffectiveness tests that repeated queries hit the cache
func TestYCSB_CacheEffectiveness(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	query := "INSERT IGNORE INTO usertable (YCSB_KEY, FIELD0) VALUES (?, ?)"

	// First execution - should not be cached
	ctx1 := NewContext(query, nil)
	if err := pipeline.Process(ctx1); err != nil {
		t.Fatalf("First pipeline processing failed: %v", err)
	}
	if ctx1.WasCached {
		t.Error("First execution should not be cached")
	}

	// Second execution - should be cached
	ctx2 := NewContext(query, nil)
	if err := pipeline.Process(ctx2); err != nil {
		t.Fatalf("Second pipeline processing failed: %v", err)
	}
	if !ctx2.WasCached {
		t.Error("Second execution should be cached")
	}

	// Verify results are identical
	if ctx1.TranspiledSQL != ctx2.TranspiledSQL {
		t.Errorf("Cached result differs from original\nOriginal: %s\nCached:   %s", ctx1.TranspiledSQL, ctx2.TranspiledSQL)
	}
}

// TestDDL_BothDialects tests that DDL statements are properly classified and have table name extracted
// DDL uses SQL-based replication (no row-level CDC), so we verify proper classification
func TestDDL_BothDialects(t *testing.T) {
	pipeline, err := NewPipeline(1000, 4)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}
	defer pipeline.Close()

	tests := []struct {
		name              string
		query             string
		expectedType      StatementType
		expectedTableName string
		expectMutation    bool
	}{
		// MySQL dialect DDL
		{
			name:              "CREATE TABLE (MySQL)",
			query:             "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
			expectedType:      StatementDDL,
			expectedTableName: "users",
			expectMutation:    true,
		},
		{
			name:              "CREATE TABLE IF NOT EXISTS (MySQL)",
			query:             "CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY)",
			expectedType:      StatementDDL,
			expectedTableName: "orders",
			expectMutation:    true,
		},
		{
			name:              "ALTER TABLE ADD COLUMN (MySQL)",
			query:             "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			expectedType:      StatementDDL,
			expectedTableName: "users",
			expectMutation:    true,
		},
		{
			name:              "DROP TABLE (MySQL)",
			query:             "DROP TABLE IF EXISTS temp_table",
			expectedType:      StatementDDL,
			expectedTableName: "temp_table",
			expectMutation:    true,
		},
		// SQLite dialect DDL (PRAGMA statements are SQLite-specific)
		{
			name:              "PRAGMA (SQLite)",
			query:             "PRAGMA table_info(users)",
			expectedType:      StatementUnsupported, // PRAGMA isn't DDL
			expectedTableName: "",
			expectMutation:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.query, nil)
			if err := pipeline.Process(ctx); err != nil {
				t.Fatalf("Pipeline processing failed: %v", err)
			}

			if ctx.StatementType != tt.expectedType {
				t.Errorf("StatementType mismatch: got %d, want %d", ctx.StatementType, tt.expectedType)
			}

			if tt.expectedTableName != "" && ctx.TableName != tt.expectedTableName {
				t.Errorf("TableName mismatch: got %s, want %s", ctx.TableName, tt.expectedTableName)
			}

			if ctx.IsMutation != tt.expectMutation {
				t.Errorf("IsMutation mismatch: got %v, want %v", ctx.IsMutation, tt.expectMutation)
			}
		})
	}
}
