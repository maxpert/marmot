package query

import (
	"strings"
	"sync/atomic"
	"testing"
)

// mockIDGenerator is a test ID generator that returns predictable IDs
type mockIDGenerator struct {
	counter atomic.Uint64
}

func (m *mockIDGenerator) NextID() uint64 {
	return m.counter.Add(1)
}

// mockSchemaLookup returns a SchemaLookup function that knows about specific tables
func mockSchemaLookup(tableColumns map[string]string) func(string) string {
	return func(table string) string {
		return tableColumns[table]
	}
}


func getTranspiledSQL(ctx *QueryContext) string {
	if len(ctx.Output.Statements) > 0 {
		return ctx.Output.Statements[0].SQL
	}
	return ""
}

func getTransformations(ctx *QueryContext) []Transformation {
	if ctx.MySQLState != nil {
		return ctx.MySQLState.Transformations
	}
	return nil
}

func getWasCached(ctx *QueryContext) bool {
	if ctx.MySQLState != nil {
		return ctx.MySQLState.WasCached
	}
	return false
}

func TestPipelineAutoIncrementIDInjection(t *testing.T) {
	// Use mock ID generator
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Set up schema lookup for sbtest1 table
	schemaLookup := mockSchemaLookup(map[string]string{
		"sbtest1": "id",
	})

	// Now test INSERT with 0 value
	sql := "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 1, 'test', 'pad')"
	ctx := NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	if ctx.Input.Dialect != DialectMySQL {
		t.Errorf("Expected DialectMySQL, got %v", ctx.Input.Dialect)
	}

	if !(ctx.Input.Dialect == DialectMySQL) {
		t.Errorf("Expected NeedsTranspile to be true")
	}

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Original SQL: %s", ctx.Input.SQL)
	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))
	t.Logf("Transformations: %+v", getTransformations(ctx))

	// The transpiled SQL should contain the generated ID (1) instead of 0
	if !strings.Contains(getTranspiledSQL(ctx), "(1, 1,") {
		t.Errorf("Expected transpiled SQL to contain generated ID '1', got: %s", getTranspiledSQL(ctx))
	}

	// Should have AutoIncrementID transformation
	found := false
	for _, tr := range getTransformations(ctx) {
		if tr.Rule == "AutoIncrementID" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected AutoIncrementID transformation, got: %+v", getTransformations(ctx))
	}
}

func TestPipelineAutoIncrementIDInjection_NullValue(t *testing.T) {
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Set up schema lookup for users table
	schemaLookup := mockSchemaLookup(map[string]string{
		"users": "id",
	})

	// Test INSERT with NULL value
	sql := "INSERT INTO users (id, name) VALUES (NULL, 'test')"
	ctx := NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))

	// The NULL should be replaced with generated ID
	if strings.Contains(strings.ToLower(getTranspiledSQL(ctx)), "null") {
		t.Errorf("Expected NULL to be replaced with ID, got: %s", getTranspiledSQL(ctx))
	}
}

func TestPipelineAutoIncrementIDInjection_NoGenerator(t *testing.T) {
	sql := "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 1, 'test', 'pad')"

	ctx := NewContext(sql, nil)

	// No ID generator - should not modify the 0
	pipeline, err := NewPipeline(1000, nil)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))

	// Without ID generator, the 0 should remain as 0
	if !strings.Contains(getTranspiledSQL(ctx), "(0, 1,") {
		t.Errorf("Expected 0 to remain unchanged without ID generator, got: %s", getTranspiledSQL(ctx))
	}
}

func TestPipelineAutoIncrementIDInjection_ExplicitID(t *testing.T) {
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Set up schema lookup for users table
	schemaLookup := mockSchemaLookup(map[string]string{
		"users": "id",
	})

	// When user provides explicit non-zero ID, it should not be modified
	sql := "INSERT INTO users (id, name) VALUES (12345, 'test')"
	ctx := NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))

	// The explicit ID 12345 should remain unchanged
	if !strings.Contains(getTranspiledSQL(ctx), "12345") {
		t.Errorf("Expected explicit ID 12345 to remain unchanged, got: %s", getTranspiledSQL(ctx))
	}
}

func TestPipelineAutoIncrementIDInjection_MultiRow(t *testing.T) {
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Set up schema lookup for users table
	schemaLookup := mockSchemaLookup(map[string]string{
		"users": "id",
	})

	sql := "INSERT INTO users (id, name) VALUES (0, 'alice'), (0, 'bob'), (100, 'charlie')"
	ctx := NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))

	// First two rows should have generated IDs (1, 2), third row keeps explicit 100
	if !strings.Contains(getTranspiledSQL(ctx), "(1, 'alice')") {
		t.Errorf("Expected first row to have ID 1, got: %s", getTranspiledSQL(ctx))
	}
	if !strings.Contains(getTranspiledSQL(ctx), "(2, 'bob')") {
		t.Errorf("Expected second row to have ID 2, got: %s", getTranspiledSQL(ctx))
	}
	if !strings.Contains(getTranspiledSQL(ctx), "(100, 'charlie')") {
		t.Errorf("Expected third row to keep explicit ID 100, got: %s", getTranspiledSQL(ctx))
	}
}

func TestPipelineAutoIncrementIDInjection_UnregisteredTable(t *testing.T) {
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// No schema lookup - should not inject IDs
	sql := "INSERT INTO unknown_table (id, name) VALUES (0, 'test')"
	ctx := NewContext(sql, nil)
	// ctx.SchemaLookup is nil - table not known

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))

	// Without schema lookup, the 0 should remain as 0
	if !strings.Contains(getTranspiledSQL(ctx), "(0, 'test')") {
		t.Errorf("Expected 0 to remain unchanged for unregistered table, got: %s", getTranspiledSQL(ctx))
	}
}

func TestPipelineAutoIncrementIDInjection_CacheBypassForIDInjection(t *testing.T) {
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Set up schema lookup for users table
	schemaLookup := mockSchemaLookup(map[string]string{
		"users": "id",
	})

	// Process the same INSERT twice - each should get unique ID
	sql := "INSERT INTO users (id, name) VALUES (0, 'test')"

	ctx1 := NewContext(sql, nil)
	ctx1.SchemaLookup = schemaLookup
	if err := pipeline.Process(ctx1); err != nil {
		t.Fatalf("Pipeline error (first): %v", err)
	}

	ctx2 := NewContext(sql, nil)
	ctx2.SchemaLookup = schemaLookup
	if err := pipeline.Process(ctx2); err != nil {
		t.Fatalf("Pipeline error (second): %v", err)
	}

	t.Logf("First transpiled SQL: %s", getTranspiledSQL(ctx1))
	t.Logf("Second transpiled SQL: %s", getTranspiledSQL(ctx2))

	// First should get ID 1, second should get ID 2
	if !strings.Contains(getTranspiledSQL(ctx1), "(1, 'test')") {
		t.Errorf("Expected first query to have ID 1, got: %s", getTranspiledSQL(ctx1))
	}
	if !strings.Contains(getTranspiledSQL(ctx2), "(2, 'test')") {
		t.Errorf("Expected second query to have ID 2, got: %s", getTranspiledSQL(ctx2))
	}

	// Neither should be cached (ID injection bypasses cache)
	if getWasCached(ctx1) {
		t.Errorf("First query should not be cached")
	}
	if getWasCached(ctx2) {
		t.Errorf("Second query should not be cached")
	}
}

func TestPipelineAutoIncrementIDInjection_InsertIgnoreWithPatternTransform(t *testing.T) {
	idGen := &mockIDGenerator{}
	pipeline, err := NewPipeline(1000, idGen)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Set up schema lookup for users table
	schemaLookup := mockSchemaLookup(map[string]string{
		"users": "id",
	})

	// Test INSERT IGNORE with 0 value - should apply both ID injection AND pattern transform
	sql := "INSERT IGNORE INTO users (id, name) VALUES (0, 'test')"
	ctx := NewContext(sql, nil)
	ctx.SchemaLookup = schemaLookup

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Transpiled SQL: %s", getTranspiledSQL(ctx))

	// Should have generated ID
	if !strings.Contains(getTranspiledSQL(ctx), "(1, 'test')") {
		t.Errorf("Expected ID to be injected, got: %s", getTranspiledSQL(ctx))
	}

	// Should have INSERT OR IGNORE (pattern transformation from InsertIgnoreRule)
	if !strings.Contains(strings.ToUpper(getTranspiledSQL(ctx)), "INSERT OR IGNORE") {
		t.Errorf("Expected INSERT OR IGNORE transformation, got: %s", getTranspiledSQL(ctx))
	}
}
