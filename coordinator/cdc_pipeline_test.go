package coordinator

import (
	"strconv"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// TestPipeline_SingleInsert tests processing a single INSERT operation
func TestPipeline_SingleInsert(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}
	if result.TotalEntries != 1 {
		t.Errorf("TotalEntries = %d, want 1", result.TotalEntries)
	}
	if result.MergedCount != 1 {
		t.Errorf("MergedCount = %d, want 1", result.MergedCount)
	}
	if result.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", result.DroppedCount)
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementInsert {
		t.Errorf("Statement type = %v, want StatementInsert", stmt.Type)
	}
	if stmt.TableName != "users" {
		t.Errorf("TableName = %q, want %q", stmt.TableName, "users")
	}
	if stmt.IntentKey != "1" {
		t.Errorf("IntentKey = %q, want %q", stmt.IntentKey, "1")
	}
	if len(stmt.OldValues) != 0 {
		t.Errorf("OldValues should be empty for INSERT, got %d entries", len(stmt.OldValues))
	}
	if len(stmt.NewValues) != 2 {
		t.Errorf("NewValues should have 2 entries, got %d", len(stmt.NewValues))
	}
}

// TestPipeline_SingleUpdate tests processing a single UPDATE operation
func TestPipeline_SingleUpdate(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice Updated"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementUpdate {
		t.Errorf("Statement type = %v, want StatementUpdate", stmt.Type)
	}
	if stmt.TableName != "users" {
		t.Errorf("TableName = %q, want %q", stmt.TableName, "users")
	}
	if len(stmt.OldValues) != 2 {
		t.Errorf("OldValues should have 2 entries, got %d", len(stmt.OldValues))
	}
	if len(stmt.NewValues) != 2 {
		t.Errorf("NewValues should have 2 entries, got %d", len(stmt.NewValues))
	}
	if string(stmt.OldValues["name"]) != "Alice" {
		t.Errorf("OldValues[name] = %q, want %q", stmt.OldValues["name"], "Alice")
	}
	if string(stmt.NewValues["name"]) != "Alice Updated" {
		t.Errorf("NewValues[name] = %q, want %q", stmt.NewValues["name"], "Alice Updated")
	}
}

// TestPipeline_SingleDelete tests processing a single DELETE operation
func TestPipeline_SingleDelete(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			},
			NewValues: nil,
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementDelete {
		t.Errorf("Statement type = %v, want StatementDelete", stmt.Type)
	}
	if stmt.TableName != "users" {
		t.Errorf("TableName = %q, want %q", stmt.TableName, "users")
	}
	if len(stmt.OldValues) != 2 {
		t.Errorf("OldValues should have 2 entries, got %d", len(stmt.OldValues))
	}
	if len(stmt.NewValues) != 0 {
		t.Errorf("NewValues should be empty for DELETE, got %d entries", len(stmt.NewValues))
	}
}

// TestPipeline_MultiRowInsert_100Rows tests the WordPress scenario:
// 100 INSERT entries with different intent keys should produce 100 statements
func TestPipeline_MultiRowInsert_100Rows(t *testing.T) {
	entries := make([]CDCEntry, 100)
	for i := 0; i < 100; i++ {
		intentKey := string(rune('A' + (i % 26))) // A-Z cycling
		entries[i] = CDCEntry{
			Table:     "wp_posts",
			IntentKey: intentKey,
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":    []byte(intentKey),
				"title": []byte("Post " + intentKey),
			},
		}
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Each unique intent key produces one statement
	// Since we cycle through 26 unique keys (A-Z), we expect 26 statements
	expectedStatements := 26 // Because we cycle A-Z (26 unique intent keys)
	if len(result.Statements) != expectedStatements {
		t.Errorf("Expected %d statements (26 unique intent keys from 100 entries), got %d",
			expectedStatements, len(result.Statements))
	}

	if result.TotalEntries != 100 {
		t.Errorf("TotalEntries = %d, want 100", result.TotalEntries)
	}

	// Verify all statements are INSERTs
	for i, stmt := range result.Statements {
		if stmt.Type != protocol.StatementInsert {
			t.Errorf("Statement[%d] type = %v, want StatementInsert", i, stmt.Type)
		}
		if stmt.TableName != "wp_posts" {
			t.Errorf("Statement[%d] TableName = %q, want %q", i, stmt.TableName, "wp_posts")
		}
	}
}

// TestPipeline_MultiRowInsert_PreservesAllRows verifies that different intent keys
// are all preserved (not merged together)
func TestPipeline_MultiRowInsert_PreservesAllRows(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
		},
		{
			Table:     "users",
			IntentKey: "2",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("2"), "name": []byte("Bob")},
		},
		{
			Table:     "users",
			IntentKey: "3",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("3"), "name": []byte("Charlie")},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 3 {
		t.Fatalf("Expected 3 statements (3 unique rows), got %d", len(result.Statements))
	}
	if result.TotalEntries != 3 {
		t.Errorf("TotalEntries = %d, want 3", result.TotalEntries)
	}
	if result.MergedCount != 3 {
		t.Errorf("MergedCount = %d, want 3", result.MergedCount)
	}
	if result.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", result.DroppedCount)
	}

	// Verify each row is preserved
	intentKeysSeen := make(map[string]bool)
	for _, stmt := range result.Statements {
		intentKeysSeen[stmt.IntentKey] = true
	}
	if !intentKeysSeen["1"] || !intentKeysSeen["2"] || !intentKeysSeen["3"] {
		t.Errorf("Not all intent keys preserved. Seen: %v", intentKeysSeen)
	}
}

// TestPipeline_MultiRowInsert_UniqueIntentKeys verifies that each unique intent key
// produces exactly one statement, even with 100 entries
func TestPipeline_MultiRowInsert_UniqueIntentKeys(t *testing.T) {
	// Create 100 entries for 100 unique intent keys
	entries := make([]CDCEntry, 100)
	for i := 0; i < 100; i++ {
		intentKey := string(rune(i)) // Unique intent key for each
		entries[i] = CDCEntry{
			Table:     "products",
			IntentKey: intentKey,
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":   []byte(intentKey),
				"name": []byte("Product " + intentKey),
			},
		}
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// 100 unique intent keys = 100 statements
	if len(result.Statements) != 100 {
		t.Errorf("Expected 100 statements (100 unique intent keys), got %d", len(result.Statements))
	}
	if result.TotalEntries != 100 {
		t.Errorf("TotalEntries = %d, want 100", result.TotalEntries)
	}
}

// TestPipeline_Upsert_DeleteThenInsert_MergedToUpdate tests the CRITICAL UPSERT scenario:
// SQLite fires DELETE then INSERT for UPSERT on existing row
// Pipeline must merge them into a single UPDATE statement
func TestPipeline_Upsert_DeleteThenInsert_MergedToUpdate(t *testing.T) {
	entries := []CDCEntry{
		// DELETE hook fires first
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("old_name"),
			},
			NewValues: nil,
		},
		// INSERT hook fires second
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("new_name"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// CRITICAL: Two entries (DELETE + INSERT) must merge into ONE statement
	if len(result.Statements) != 1 {
		t.Fatalf("UPSERT must merge to 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]

	// CRITICAL: Must be UPDATE, not DELETE or INSERT
	if stmt.Type != protocol.StatementUpdate {
		t.Errorf("UPSERT (DELETE+INSERT) must produce UPDATE, got %v", stmt.Type)
	}

	// CRITICAL: OldValues from DELETE
	if string(stmt.OldValues["name"]) != "old_name" {
		t.Errorf("OldValues[name] = %q, want %q (from DELETE)", stmt.OldValues["name"], "old_name")
	}

	// CRITICAL: NewValues from INSERT
	if string(stmt.NewValues["name"]) != "new_name" {
		t.Errorf("NewValues[name] = %q, want %q (from INSERT)", stmt.NewValues["name"], "new_name")
	}

	if result.TotalEntries != 2 {
		t.Errorf("TotalEntries = %d, want 2", result.TotalEntries)
	}
	if result.MergedCount != 1 {
		t.Errorf("MergedCount = %d, want 1", result.MergedCount)
	}
	if result.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", result.DroppedCount)
	}
}

// TestPipeline_Upsert_PreservesOldValues_FromDelete verifies that
// OldValues in the merged UPDATE come from the DELETE hook
func TestPipeline_Upsert_PreservesOldValues_FromDelete(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "products",
			IntentKey: "42",
			OldValues: map[string][]byte{
				"id":    []byte("42"),
				"price": []byte("9.99"),
				"stock": []byte("100"),
			},
			NewValues: nil,
		},
		{
			Table:     "products",
			IntentKey: "42",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":    []byte("42"),
				"price": []byte("19.99"),
				"stock": []byte("50"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 merged statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if string(stmt.OldValues["price"]) != "9.99" {
		t.Errorf("OldValues[price] = %q, want %q (from DELETE)", stmt.OldValues["price"], "9.99")
	}
	if string(stmt.OldValues["stock"]) != "100" {
		t.Errorf("OldValues[stock] = %q, want %q (from DELETE)", stmt.OldValues["stock"], "100")
	}
}

// TestPipeline_Upsert_PreservesNewValues_FromInsert verifies that
// NewValues in the merged UPDATE come from the INSERT hook
func TestPipeline_Upsert_PreservesNewValues_FromInsert(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "products",
			IntentKey: "42",
			OldValues: map[string][]byte{
				"id":    []byte("42"),
				"price": []byte("9.99"),
			},
			NewValues: nil,
		},
		{
			Table:     "products",
			IntentKey: "42",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":    []byte("42"),
				"price": []byte("19.99"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 merged statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if string(stmt.NewValues["price"]) != "19.99" {
		t.Errorf("NewValues[price] = %q, want %q (from INSERT)", stmt.NewValues["price"], "19.99")
	}
}

// TestPipeline_InsertThenDelete_CancelsOut tests INSERT followed by DELETE
// on same row - should cancel out and be dropped
func TestPipeline_InsertThenDelete_CancelsOut(t *testing.T) {
	entries := []CDCEntry{
		// INSERT
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("temp"),
			},
		},
		// DELETE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("temp"),
			},
			NewValues: nil,
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// INSERT->DELETE cancels out, no statements produced
	if len(result.Statements) != 0 {
		t.Errorf("INSERT->DELETE should cancel out, got %d statements", len(result.Statements))
	}
	if result.TotalEntries != 2 {
		t.Errorf("TotalEntries = %d, want 2", result.TotalEntries)
	}
	if result.DroppedCount != 1 {
		t.Errorf("DroppedCount = %d, want 1 (INSERT->DELETE cancelled)", result.DroppedCount)
	}
}

// TestPipeline_UpdateThenUpdate_MergesValues tests multiple UPDATEs
// on the same row - OldValues from first, NewValues from last
func TestPipeline_UpdateThenUpdate_MergesValues(t *testing.T) {
	entries := []CDCEntry{
		// First UPDATE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v1"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v2"),
			},
		},
		// Second UPDATE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v2"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v3"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 merged statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementUpdate {
		t.Errorf("Statement type = %v, want StatementUpdate", stmt.Type)
	}

	// OldValues from FIRST update
	if string(stmt.OldValues["name"]) != "v1" {
		t.Errorf("OldValues[name] = %q, want %q (from first UPDATE)", stmt.OldValues["name"], "v1")
	}

	// NewValues from LAST update
	if string(stmt.NewValues["name"]) != "v3" {
		t.Errorf("NewValues[name] = %q, want %q (from last UPDATE)", stmt.NewValues["name"], "v3")
	}

	if result.TotalEntries != 2 {
		t.Errorf("TotalEntries = %d, want 2", result.TotalEntries)
	}
}

// TestPipeline_UpdateThenDelete_BecomesDelete tests UPDATE followed by DELETE
// Should become a DELETE with OldValues from the original UPDATE
func TestPipeline_UpdateThenDelete_BecomesDelete(t *testing.T) {
	entries := []CDCEntry{
		// UPDATE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("original"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("updated"),
			},
		},
		// DELETE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("updated"),
			},
			NewValues: nil,
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementDelete {
		t.Errorf("UPDATE->DELETE must produce DELETE, got %v", stmt.Type)
	}

	// OldValues should be from the original UPDATE
	if string(stmt.OldValues["name"]) != "original" {
		t.Errorf("OldValues[name] = %q, want %q (from original UPDATE)", stmt.OldValues["name"], "original")
	}

	if len(stmt.NewValues) != 0 {
		t.Errorf("NewValues should be empty for DELETE, got %d entries", len(stmt.NewValues))
	}
}

// TestPipeline_Validation_EmptyTableName_Fails tests that validation
// fails if any entry has an empty TableName
func TestPipeline_Validation_EmptyTableName_Fails(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "", // Empty table name
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1")},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	_, err := ProcessCDCEntries(entries, config)

	if err == nil {
		t.Fatal("Expected validation error for empty TableName, got nil")
	}
}

// TestPipeline_Validation_EmptyInput_ReturnsEmpty tests that empty input
// produces empty result without error
func TestPipeline_Validation_EmptyInput_ReturnsEmpty(t *testing.T) {
	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries([]CDCEntry{}, config)

	if err != nil {
		t.Fatalf("Empty input should not error, got: %v", err)
	}
	if len(result.Statements) != 0 {
		t.Errorf("Expected 0 statements, got %d", len(result.Statements))
	}
	if result.TotalEntries != 0 {
		t.Errorf("TotalEntries = %d, want 0", result.TotalEntries)
	}
	if result.MergedCount != 0 {
		t.Errorf("MergedCount = %d, want 0", result.MergedCount)
	}
	if result.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", result.DroppedCount)
	}
}

// TestPipeline_Validation_NilInput_ReturnsEmpty tests nil input handling
func TestPipeline_Validation_NilInput_ReturnsEmpty(t *testing.T) {
	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(nil, config)

	if err != nil {
		t.Fatalf("Nil input should not error, got: %v", err)
	}
	if len(result.Statements) != 0 {
		t.Errorf("Expected 0 statements, got %d", len(result.Statements))
	}
	if result.TotalEntries != 0 {
		t.Errorf("TotalEntries = %d, want 0", result.TotalEntries)
	}
}

// TestPipeline_NilOldValues_Handled tests that nil OldValues are handled correctly
func TestPipeline_NilOldValues_Handled(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: false} // Disable strict validation
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementInsert {
		t.Errorf("Statement type = %v, want StatementInsert", stmt.Type)
	}
}

// TestPipeline_NilNewValues_Handled tests that nil NewValues are handled correctly
func TestPipeline_NilNewValues_Handled(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
			NewValues: nil,
		},
	}

	config := CDCPipelineConfig{ValidateEntries: false}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != protocol.StatementDelete {
		t.Errorf("Statement type = %v, want StatementDelete", stmt.Type)
	}
}

// TestPipeline_MixedTables tests that entries from different tables
// are processed independently
func TestPipeline_MixedTables(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1")},
		},
		{
			Table:     "products",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1")},
		},
		{
			Table:     "users",
			IntentKey: "2",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("2")},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// 3 different (table, intentKey) combinations = 3 statements
	if len(result.Statements) != 3 {
		t.Fatalf("Expected 3 statements (3 unique table:intentKey), got %d", len(result.Statements))
	}

	// Verify tables are preserved
	tablesSeen := make(map[string]int)
	for _, stmt := range result.Statements {
		tablesSeen[stmt.TableName]++
	}

	if tablesSeen["users"] != 2 {
		t.Errorf("Expected 2 users statements, got %d", tablesSeen["users"])
	}
	if tablesSeen["products"] != 1 {
		t.Errorf("Expected 1 products statement, got %d", tablesSeen["products"])
	}
}

// TestPipeline_SameIntentKey_DifferentTables tests that same IntentKey in different
// tables are NOT merged (grouping is by table+intentKey)
func TestPipeline_SameIntentKey_DifferentTables(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
		},
		{
			Table:     "products",
			IntentKey: "1", // Same intent key, different table
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1"), "name": []byte("Widget")},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Must NOT merge - different tables
	if len(result.Statements) != 2 {
		t.Fatalf("Expected 2 statements (different tables), got %d", len(result.Statements))
	}

	// Verify both tables present
	tablesSeen := make(map[string]bool)
	for _, stmt := range result.Statements {
		tablesSeen[stmt.TableName] = true
	}
	if !tablesSeen["users"] || !tablesSeen["products"] {
		t.Errorf("Both tables should be present. Seen: %v", tablesSeen)
	}
}

// TestPipeline_ComplexSequence tests a complex sequence of operations
func TestPipeline_ComplexSequence(t *testing.T) {
	entries := []CDCEntry{
		// Row 1: INSERT
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1"), "name": []byte("Alice")},
		},
		// Row 2: UPSERT (DELETE + INSERT)
		{
			Table:     "users",
			IntentKey: "2",
			OldValues: map[string][]byte{"id": []byte("2"), "name": []byte("old")},
			NewValues: nil,
		},
		{
			Table:     "users",
			IntentKey: "2",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("2"), "name": []byte("new")},
		},
		// Row 3: INSERT then DELETE (cancel out)
		{
			Table:     "users",
			IntentKey: "3",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("3"), "name": []byte("temp")},
		},
		{
			Table:     "users",
			IntentKey: "3",
			OldValues: map[string][]byte{"id": []byte("3"), "name": []byte("temp")},
			NewValues: nil,
		},
		// Row 4: UPDATE then DELETE
		{
			Table:     "users",
			IntentKey: "4",
			OldValues: map[string][]byte{"id": []byte("4"), "name": []byte("v1")},
			NewValues: map[string][]byte{"id": []byte("4"), "name": []byte("v2")},
		},
		{
			Table:     "users",
			IntentKey: "4",
			OldValues: map[string][]byte{"id": []byte("4"), "name": []byte("v2")},
			NewValues: nil,
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Row 1: INSERT -> 1 statement
	// Row 2: DELETE+INSERT (UPSERT) -> 1 UPDATE statement
	// Row 3: INSERT+DELETE -> cancelled (0 statements)
	// Row 4: UPDATE+DELETE -> 1 DELETE statement
	// Total: 3 statements
	expectedStatements := 3
	if len(result.Statements) != expectedStatements {
		t.Fatalf("Expected %d statements, got %d", expectedStatements, len(result.Statements))
	}

	if result.TotalEntries != 7 {
		t.Errorf("TotalEntries = %d, want 7", result.TotalEntries)
	}
	if result.DroppedCount != 1 {
		t.Errorf("DroppedCount = %d, want 1 (row 3 cancelled)", result.DroppedCount)
	}

	// Verify statement types
	stmtsByIntentKey := make(map[string]protocol.StatementCode)
	for _, stmt := range result.Statements {
		stmtsByIntentKey[stmt.IntentKey] = stmt.Type
	}

	if stmtsByIntentKey["1"] != protocol.StatementInsert {
		t.Errorf("Row 1 should be INSERT, got %v", stmtsByIntentKey["1"])
	}
	if stmtsByIntentKey["2"] != protocol.StatementUpdate {
		t.Errorf("Row 2 should be UPDATE (UPSERT), got %v", stmtsByIntentKey["2"])
	}
	if _, exists := stmtsByIntentKey["3"]; exists {
		t.Errorf("Row 3 should be cancelled (not present), but found %v", stmtsByIntentKey["3"])
	}
	if stmtsByIntentKey["4"] != protocol.StatementDelete {
		t.Errorf("Row 4 should be DELETE, got %v", stmtsByIntentKey["4"])
	}
}

// TestPipeline_ValidationDisabled tests that validation can be disabled
func TestPipeline_ValidationDisabled(t *testing.T) {
	entries := []CDCEntry{
		{
			Table:     "", // Would fail validation
			IntentKey: "1",
			OldValues: nil,
			NewValues: map[string][]byte{"id": []byte("1")},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: false}
	_, err := ProcessCDCEntries(entries, config)

	// Should not error even with empty table name
	if err != nil {
		t.Fatalf("With validation disabled, should not error, got: %v", err)
	}
}

// TestMergeGroup_InsertThenUpdate_BecomesInsert tests INSERT followed by UPDATE
// on the same row - should produce a single INSERT with final values
func TestMergeGroup_InsertThenUpdate_BecomesInsert(t *testing.T) {
	entries := []CDCEntry{
		// INSERT
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: make(map[string][]byte),
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			},
		},
		// UPDATE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("Alice Updated"),
			},
		},
	}

	merged, wasDropped := MergeGroup(entries)

	if wasDropped {
		t.Fatal("INSERT->UPDATE should not be dropped")
	}
	if merged == nil {
		t.Fatal("Merged entry should not be nil")
	}

	// Verify it's an INSERT (empty OldValues, has NewValues)
	if len(merged.OldValues) != 0 {
		t.Errorf("OldValues should be empty for INSERT, got %d entries", len(merged.OldValues))
	}
	if len(merged.NewValues) == 0 {
		t.Error("NewValues should not be empty")
	}

	// Verify final values from UPDATE
	if string(merged.NewValues["name"]) != "Alice Updated" {
		t.Errorf("NewValues[name] = %q, want %q (from UPDATE)", merged.NewValues["name"], "Alice Updated")
	}

	// Verify statement type is INSERT
	stmt := ConvertToStatement(*merged)
	if stmt.Type != protocol.StatementInsert {
		t.Errorf("Statement type = %v, want StatementInsert", stmt.Type)
	}
}

// TestMergeGroup_InsertUpdateUpdate_BecomesInsert tests INSERT followed by multiple UPDATEs
// Should produce a single INSERT with final values from last UPDATE
func TestMergeGroup_InsertUpdateUpdate_BecomesInsert(t *testing.T) {
	entries := []CDCEntry{
		// INSERT
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: make(map[string][]byte),
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v1"),
			},
		},
		// UPDATE 1
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v1"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v2"),
			},
		},
		// UPDATE 2
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v2"),
			},
			NewValues: map[string][]byte{
				"id":   []byte("1"),
				"name": []byte("v3"),
			},
		},
	}

	merged, wasDropped := MergeGroup(entries)

	if wasDropped {
		t.Fatal("INSERT->UPDATE->UPDATE should not be dropped")
	}
	if merged == nil {
		t.Fatal("Merged entry should not be nil")
	}

	// Verify it's an INSERT
	if len(merged.OldValues) != 0 {
		t.Errorf("OldValues should be empty for INSERT, got %d entries", len(merged.OldValues))
	}

	// Verify final values from last UPDATE
	if string(merged.NewValues["name"]) != "v3" {
		t.Errorf("NewValues[name] = %q, want %q (from last UPDATE)", merged.NewValues["name"], "v3")
	}

	// Verify statement type is INSERT
	stmt := ConvertToStatement(*merged)
	if stmt.Type != protocol.StatementInsert {
		t.Errorf("Statement type = %v, want StatementInsert", stmt.Type)
	}
}

// TestPipeline_InsertThenUpdate_ProducesInsert tests full pipeline with INSERT->UPDATE
// Verifies the result contains INSERT statement with correct values
func TestPipeline_InsertThenUpdate_ProducesInsert(t *testing.T) {
	entries := []CDCEntry{
		// INSERT
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: make(map[string][]byte),
			NewValues: map[string][]byte{
				"id":    []byte("1"),
				"name":  []byte("Alice"),
				"email": []byte("alice@example.com"),
			},
		},
		// UPDATE
		{
			Table:     "users",
			IntentKey: "1",
			OldValues: map[string][]byte{
				"id":    []byte("1"),
				"name":  []byte("Alice"),
				"email": []byte("alice@example.com"),
			},
			NewValues: map[string][]byte{
				"id":    []byte("1"),
				"name":  []byte("Alice Smith"),
				"email": []byte("alice.smith@example.com"),
			},
		},
	}

	config := CDCPipelineConfig{ValidateEntries: true}
	result, err := ProcessCDCEntries(entries, config)

	if err != nil {
		t.Fatalf("ProcessCDCEntries failed: %v", err)
	}

	// Should produce exactly 1 statement
	if len(result.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]

	// CRITICAL: Must be INSERT, not UPDATE
	if stmt.Type != protocol.StatementInsert {
		t.Errorf("Statement type = %v, want StatementInsert (row didn't exist before transaction)", stmt.Type)
	}

	// Verify empty OldValues
	if len(stmt.OldValues) != 0 {
		t.Errorf("OldValues should be empty for INSERT, got %d entries", len(stmt.OldValues))
	}

	// Verify final values from UPDATE
	if string(stmt.NewValues["name"]) != "Alice Smith" {
		t.Errorf("NewValues[name] = %q, want %q (from UPDATE)", stmt.NewValues["name"], "Alice Smith")
	}
	if string(stmt.NewValues["email"]) != "alice.smith@example.com" {
		t.Errorf("NewValues[email] = %q, want %q (from UPDATE)", stmt.NewValues["email"], "alice.smith@example.com")
	}

	if result.TotalEntries != 2 {
		t.Errorf("TotalEntries = %d, want 2", result.TotalEntries)
	}
	if result.MergedCount != 1 {
		t.Errorf("MergedCount = %d, want 1", result.MergedCount)
	}
	if result.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", result.DroppedCount)
	}
}

func BenchmarkProcessCDCEntries_1000Rows(b *testing.B) {
	entries := make([]CDCEntry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = CDCEntry{
			Table:     "test",
			IntentKey: strconv.Itoa(i),
			NewValues: map[string][]byte{"id": []byte(strconv.Itoa(i))},
		}
	}
	config := DefaultCDCPipelineConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProcessCDCEntries(entries, config)
	}
}
