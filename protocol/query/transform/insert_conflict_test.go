package transform

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

// mockSerializer implements Serializer for testing
type mockSerializer struct{}

func (m *mockSerializer) Serialize(stmt sqlparser.Statement) string {
	return sqlparser.String(stmt)
}

func TestInsertOnDuplicateKeyRule_Name(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	if rule.Name() != "InsertOnDuplicateKey" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "InsertOnDuplicateKey")
	}
}

func TestInsertOnDuplicateKeyRule_Priority(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	if rule.Priority() != 20 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 20)
	}
}

func TestInsertOnDuplicateKeyRule_TransformValuesFunction(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantModified bool
		checkOutput  func(string) error
	}{
		{
			name:         "transform VALUES() to excluded",
			input:        "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE count = VALUES(count)",
			wantModified: true,
			checkOutput: func(sql string) error {
				if !strings.Contains(sql, "excluded") {
					t.Errorf("expected 'excluded' in output, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:         "transform complex expression",
			input:        "INSERT INTO stats (id, visits) VALUES (1, 10) ON DUPLICATE KEY UPDATE visits = visits + VALUES(visits)",
			wantModified: true,
			checkOutput: func(sql string) error {
				if !strings.Contains(sql, "excluded") {
					t.Errorf("expected 'excluded' in output, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:         "multiple update expressions",
			input:        "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE name = VALUES(name), count = VALUES(count)",
			wantModified: true,
			checkOutput: func(sql string) error {
				if !strings.Contains(sql, "excluded") {
					t.Errorf("expected 'excluded' in output, got: %s", sql)
				}
				return nil
			},
		},
		{
			name:         "no ON DUPLICATE KEY",
			input:        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
			wantModified: false,
		},
		{
			name:         "not an INSERT",
			input:        "SELECT * FROM users",
			wantModified: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &InsertOnDuplicateKeyRule{}
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
				// Rule now returns metadata instead of serialized SQL
				if result[0].Metadata == nil {
					t.Fatal("expected metadata, got nil")
				}
				if _, ok := result[0].Metadata["conflictColumns"]; !ok {
					t.Fatal("expected conflictColumns in metadata")
				}
				// Verify AST was modified by serializing it
				if tt.checkOutput != nil {
					serializer := &SQLiteSerializer{}
					cols, _ := result[0].Metadata["conflictColumns"].([]string)
					sql := serializer.SerializeWithOpts(stmt, SerializeOpts{ConflictColumns: cols})
					_ = tt.checkOutput(sql)
				}
			} else {
				if err != ErrRuleNotApplicable {
					t.Errorf("expected ErrRuleNotApplicable, got: %v", err)
				}
			}
		})
	}
}

func TestInsertOnDuplicateKeyRule_ConflictTargetWithSchema(t *testing.T) {
	mockSchema := func(database, table string) *SchemaInfo {
		if table == "users" {
			return &SchemaInfo{
				PrimaryKey: []string{"id", "tenant_id"},
			}
		}
		return nil
	}

	input := "INSERT INTO users (id, tenant_id, name) VALUES (1, 100, 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)"
	stmt, _ := sqlparser.NewTestParser().Parse(input)

	rule := &InsertOnDuplicateKeyRule{}
	serializer := &SQLiteSerializer{}
	result, err := rule.Transform(stmt, nil, mockSchema, "testdb", serializer)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 transpiled statement, got %d", len(result))
	}

	// Rule now returns metadata with conflict columns
	if result[0].Metadata == nil {
		t.Fatal("expected metadata, got nil")
	}

	cols, ok := result[0].Metadata["conflictColumns"].([]string)
	if !ok {
		t.Fatal("expected conflictColumns in metadata")
	}

	// Verify conflict columns match schema primary key
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "tenant_id" {
		t.Errorf("expected conflict columns [id, tenant_id], got: %v", cols)
	}

	// Verify serialization produces correct SQL
	sql := serializer.SerializeWithOpts(stmt, SerializeOpts{ConflictColumns: cols})
	if !strings.Contains(sql, "ON CONFLICT") {
		t.Errorf("expected SQL to contain ON CONFLICT, got: %s", sql)
	}

	if !strings.Contains(sql, "id") || !strings.Contains(sql, "tenant_id") {
		t.Errorf("expected SQL to contain conflict columns id and tenant_id, got: %s", sql)
	}
}

func TestInsertOnDuplicateKeyRule_ConflictTargetFallback(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)"
	stmt, _ := sqlparser.NewTestParser().Parse(input)

	rule := &InsertOnDuplicateKeyRule{}
	serializer := &SQLiteSerializer{}
	result, err := rule.Transform(stmt, nil, nil, "testdb", serializer)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 transpiled statement, got %d", len(result))
	}

	// Rule now returns metadata with conflict columns
	if result[0].Metadata == nil {
		t.Fatal("expected metadata, got nil")
	}

	cols, ok := result[0].Metadata["conflictColumns"].([]string)
	if !ok {
		t.Fatal("expected conflictColumns in metadata")
	}

	// Verify fallback to first column
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("expected conflict column [id], got: %v", cols)
	}

	// Verify serialization produces correct SQL
	sql := serializer.SerializeWithOpts(stmt, SerializeOpts{ConflictColumns: cols})
	if !strings.Contains(sql, "ON CONFLICT") {
		t.Errorf("expected SQL to contain ON CONFLICT, got: %s", sql)
	}

	if !strings.Contains(sql, "id") {
		t.Errorf("expected SQL to contain fallback column 'id', got: %s", sql)
	}
}

func TestInsertOnDuplicateKeyRule_NoConflictTargetError(t *testing.T) {
	input := "INSERT INTO users VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)"
	stmt, _ := sqlparser.NewTestParser().Parse(input)

	rule := &InsertOnDuplicateKeyRule{}
	serializer := &SQLiteSerializer{}
	result, err := rule.Transform(stmt, nil, nil, "testdb", serializer)

	if err == nil {
		t.Fatal("expected error when no conflict target can be determined, got nil")
	}

	if result != nil {
		t.Errorf("expected nil result on error, got: %v", result)
	}

	if !strings.Contains(err.Error(), "cannot determine conflict target") {
		t.Errorf("expected 'cannot determine conflict target' in error, got: %v", err)
	}
}

func TestInsertOnDuplicateKeyRule_WrongSerializerError(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)"
	stmt, _ := sqlparser.NewTestParser().Parse(input)

	rule := &InsertOnDuplicateKeyRule{}
	serializer := &mockSerializer{}
	result, err := rule.Transform(stmt, nil, nil, "testdb", serializer)

	// Rule now returns metadata instead of serializing, so serializer type doesn't matter
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 transpiled statement, got %d", len(result))
	}

	// Verify metadata is returned
	if result[0].Metadata == nil {
		t.Fatal("expected metadata, got nil")
	}

	if _, ok := result[0].Metadata["conflictColumns"]; !ok {
		t.Fatal("expected conflictColumns in metadata")
	}
}

func TestInsertOnDuplicateKeyRule_ThreadSafety(t *testing.T) {
	t.Parallel()

	mockSchema := func(database, table string) *SchemaInfo {
		if table == "users" {
			return &SchemaInfo{
				PrimaryKey: []string{"id", "email"},
			}
		}
		return nil
	}

	input := "INSERT INTO users (id, email, name) VALUES (1, 'test@test.com', 'Alice') ON DUPLICATE KEY UPDATE name = VALUES(name)"
	rule := &InsertOnDuplicateKeyRule{}

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
			result, err := rule.Transform(stmt, nil, mockSchema, "testdb", serializer)
			if err != nil {
				t.Errorf("Transform failed: %v", err)
				return
			}
			if len(result) == 0 {
				t.Error("expected result, got nil or empty")
				return
			}
			// Rule now returns metadata with conflict columns instead of serialized SQL
			if result[0].Metadata == nil {
				t.Error("expected metadata, got nil")
				return
			}
			if _, ok := result[0].Metadata["conflictColumns"]; !ok {
				t.Error("expected conflictColumns in metadata")
			}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
