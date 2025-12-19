package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
)

// TestHandlerCDCPipeline_MultipleRows verifies that multiple CDC entries
// with different intent keys produce multiple statements (NOT collapsed into one)
func TestHandlerCDCPipeline_MultipleRows(t *testing.T) {
	// Create 100 CDC entries for different rows
	entries := make([]common.CDCEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = common.CDCEntry{
			Table:     "wp_posts",
			IntentKey: string(rune('A' + (i % 26))), // A-Z cycling
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":      []byte{byte(i)},
				"content": []byte("post content"),
			},
		}
	}

	// Process through pipeline
	config := DefaultCDCPipelineConfig()
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Verify we get statements for unique intent keys (26 unique: A-Z)
	expectedCount := 26
	if len(result.Statements) != expectedCount {
		t.Errorf("Expected %d statements (unique intent keys), got %d", expectedCount, len(result.Statements))
	}

	// Verify all statements are INSERT type
	for i, stmt := range result.Statements {
		if stmt.Type != protocol.StatementInsert {
			t.Errorf("Statement[%d] type = %v, want StatementInsert", i, stmt.Type)
		}
		if stmt.TableName != "wp_posts" {
			t.Errorf("Statement[%d] TableName = %q, want %q", i, stmt.TableName, "wp_posts")
		}
		if len(stmt.NewValues) == 0 {
			t.Errorf("Statement[%d] has empty NewValues", i)
		}
	}
}

// TestHandlerCDCPipeline_SameRowMerges verifies that multiple operations
// on the same row are correctly merged
func TestHandlerCDCPipeline_SameRowMerges(t *testing.T) {
	// Create CDC entries: DELETE row1, INSERT row1 (UPSERT pattern)
	entries := []common.CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("old_name"),
			},
			NewValues: nil, // DELETE
		},
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("new_name"),
			}, // INSERT
		},
	}

	// Process through pipeline
	config := DefaultCDCPipelineConfig()
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Verify 1 UPDATE statement produced (DELETE+INSERT merged)
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement (merged UPDATE), got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementUpdate {
		t.Errorf("Statement type = %v, want StatementUpdate (UPSERT)", stmt.Type)
	}
	if stmt.TableName != "users" {
		t.Errorf("TableName = %q, want %q", stmt.TableName, "users")
	}
	if stmt.IntentKey != "1" {
		t.Errorf("IntentKey = %q, want %q", stmt.IntentKey, "1")
	}

	// Verify OldValues from DELETE
	if len(stmt.OldValues) == 0 {
		t.Error("OldValues is empty, should have DELETE data")
	}
	if oldName, ok := stmt.OldValues["name"]; !ok || string(oldName) != "old_name" {
		t.Errorf("OldValues[name] = %q, want %q", oldName, "old_name")
	}

	// Verify NewValues from INSERT
	if len(stmt.NewValues) == 0 {
		t.Error("NewValues is empty, should have INSERT data")
	}
	if newName, ok := stmt.NewValues["name"]; !ok || string(newName) != "new_name" {
		t.Errorf("NewValues[name] = %q, want %q", newName, "new_name")
	}
}

// TestHandlerCDCPipeline_InsertDeleteCancelsOut verifies that
// INSERT followed by DELETE on the same row cancels out
func TestHandlerCDCPipeline_InsertDeleteCancelsOut(t *testing.T) {
	entries := []common.CDCEntry{
		{
			Table:     "temp_table",
			IntentKey: "99",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id": []byte("99"),
			}, // INSERT
		},
		{
			Table:     "temp_table",
			IntentKey: "99",
			OldValues: map[string][]byte{
				"id": []byte("99"),
			},
			NewValues: nil, // DELETE
		},
	}

	// Process through pipeline
	config := DefaultCDCPipelineConfig()
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Verify 0 statements (cancelled out)
	if len(result.Statements) != 0 {
		t.Errorf("Expected 0 statements (cancelled out), got %d", len(result.Statements))
	}

	if result.DroppedCount != 1 {
		t.Errorf("DroppedCount = %d, want 1", result.DroppedCount)
	}
}

// TestHandlerCDCPipeline_EmptyEntries verifies empty input produces empty output
func TestHandlerCDCPipeline_EmptyEntries(t *testing.T) {
	config := DefaultCDCPipelineConfig()
	result, err := ProcessCDCEntries([]common.CDCEntry{}, config)

	if err != nil {
		t.Fatalf("Empty entries should not error: %v", err)
	}

	if len(result.Statements) != 0 {
		t.Errorf("Expected 0 statements for empty input, got %d", len(result.Statements))
	}
}

// TestHandlerCDCPipeline_MixedOperations tests a realistic mix of operations
func TestHandlerCDCPipeline_MixedOperations(t *testing.T) {
	entries := []common.CDCEntry{
		// Row 1: Simple INSERT
		{
			Table:     "products",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"name": []byte("Product A")},
		},
		// Row 2: UPSERT (DELETE + INSERT)
		{
			Table:     "products",
			IntentKey: "2",
			OldValues: map[string][]byte{"name": []byte("Old Product B")},
			NewValues: nil,
		},
		{
			Table:     "products",
			IntentKey: "2",
			OldValues: nil,
			NewValues: map[string][]byte{"name": []byte("New Product B")},
		},
		// Row 3: UPDATE
		{
			Table:     "products",
			IntentKey: "3",
			OldValues: map[string][]byte{"name": []byte("Product C v1")},
			NewValues: map[string][]byte{"name": []byte("Product C v2")},
		},
		// Row 4: DELETE
		{
			Table:     "products",
			IntentKey: "4",
			OldValues: map[string][]byte{"name": []byte("Product D")},
			NewValues: nil,
		},
	}

	config := DefaultCDCPipelineConfig()
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Verify 4 statements produced (one per row)
	if len(result.Statements) != 4 {
		t.Fatalf("Expected 4 statements, got %d", len(result.Statements))
	}

	// Verify statement types
	expectedTypes := []protocol.StatementCode{
		protocol.StatementInsert, // Row 1: INSERT
		protocol.StatementUpdate, // Row 2: UPSERT merged to UPDATE
		protocol.StatementUpdate, // Row 3: UPDATE
		protocol.StatementDelete, // Row 4: DELETE
	}

	for i, stmt := range result.Statements {
		if stmt.Type != expectedTypes[i] {
			t.Errorf("Statement[%d] type = %v, want %v", i, stmt.Type, expectedTypes[i])
		}
		if stmt.TableName != "products" {
			t.Errorf("Statement[%d] TableName = %q, want %q", i, stmt.TableName, "products")
		}
	}
}
