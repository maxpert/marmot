package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/common"
)

// TestMergeCDCEntries_TableNameRequired is a CRITICAL test.
// It exists because commit 28cfab9 fixed handleCommit but forgot handleMutation.
// This test MUST fail if anyone removes TableName extraction from MergeCDCEntries.
func TestMergeCDCEntries_TableNameRequired(t *testing.T) {
	entries := []common.CDCEntry{
		{Table: "users", IntentKey: []byte("1"), NewValues: map[string][]byte{"id": {1}}},
	}
	result := MergeCDCEntries(entries)

	if result.TableName == "" {
		t.Fatal("CRITICAL: TableName must be extracted from CDC entries. " +
			"Without it, write_coordinator rejects CDC statements. " +
			"See MergeCDCEntries() for details.")
	}
	if result.TableName != "users" {
		t.Errorf("TableName = %q, want %q", result.TableName, "users")
	}
}

func TestMergeCDCEntries_MergesUpsertHooks(t *testing.T) {
	// SQLite fires DELETE then INSERT for UPSERT on existing row
	entries := []common.CDCEntry{
		{Table: "users", IntentKey: []byte("1"), OldValues: map[string][]byte{"name": []byte("old")}, NewValues: nil},
		{Table: "users", IntentKey: []byte("1"), OldValues: nil, NewValues: map[string][]byte{"name": []byte("new")}},
	}
	result := MergeCDCEntries(entries)

	if result.TableName != "users" {
		t.Errorf("TableName = %q, want %q", result.TableName, "users")
	}
	if string(result.IntentKey) != "1" {
		t.Errorf("IntentKey = %q, want %q", result.IntentKey, "1")
	}
	if string(result.OldValues["name"]) != "old" {
		t.Errorf("OldValues[name] = %q, want %q", result.OldValues["name"], "old")
	}
	if string(result.NewValues["name"]) != "new" {
		t.Errorf("NewValues[name] = %q, want %q", result.NewValues["name"], "new")
	}
}

func TestMergeCDCEntries_EmptyEntries(t *testing.T) {
	result := MergeCDCEntries(nil)

	if result.TableName != "" {
		t.Errorf("TableName = %q, want empty", result.TableName)
	}
	if len(result.IntentKey) != 0 {
		t.Errorf("IntentKey = %q, want empty", result.IntentKey)
	}
	if result.OldValues == nil {
		t.Error("OldValues should be initialized to empty map, got nil")
	}
	if result.NewValues == nil {
		t.Error("NewValues should be initialized to empty map, got nil")
	}
}

func TestMergeCDCEntries_SingleInsert(t *testing.T) {
	entries := []common.CDCEntry{
		{
			Table:     "products",
			IntentKey: []byte("42"),
			OldValues: nil,
			NewValues: map[string][]byte{
				"id":    []byte("42"),
				"name":  []byte("Widget"),
				"price": []byte("9.99"),
			},
		},
	}
	result := MergeCDCEntries(entries)

	if result.TableName != "products" {
		t.Errorf("TableName = %q, want %q", result.TableName, "products")
	}
	if string(result.IntentKey) != "42" {
		t.Errorf("IntentKey = %q, want %q", result.IntentKey, "42")
	}
	if len(result.OldValues) != 0 {
		t.Errorf("OldValues should be empty for INSERT, got %d entries", len(result.OldValues))
	}
	if len(result.NewValues) != 3 {
		t.Errorf("NewValues should have 3 entries, got %d", len(result.NewValues))
	}
}

func TestMergeCDCEntries_SingleDelete(t *testing.T) {
	entries := []common.CDCEntry{
		{
			Table:     "products",
			IntentKey: []byte("42"),
			OldValues: map[string][]byte{
				"id":   []byte("42"),
				"name": []byte("Widget"),
			},
			NewValues: nil,
		},
	}
	result := MergeCDCEntries(entries)

	if result.TableName != "products" {
		t.Errorf("TableName = %q, want %q", result.TableName, "products")
	}
	if len(result.NewValues) != 0 {
		t.Errorf("NewValues should be empty for DELETE, got %d entries", len(result.NewValues))
	}
	if len(result.OldValues) != 2 {
		t.Errorf("OldValues should have 2 entries, got %d", len(result.OldValues))
	}
}
