//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import "testing"

func TestGetTranspilerSchema_PrimaryKey(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	mdb, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("GetDatabase failed: %v", err)
	}

	cache, ok := mdb.GetSchemaCache().(*SchemaCache)
	if !ok || cache == nil {
		t.Fatal("schema cache is unavailable")
	}

	cache.Update("users", &TableSchema{
		PrimaryKeys:      []string{"tenant_id", "user_id"},
		AutoIncrementCol: "user_id",
	})

	info, err := dm.GetTranspilerSchema("testdb", "users")
	if err != nil {
		t.Fatalf("GetTranspilerSchema failed: %v", err)
	}

	if len(info.PrimaryKey) != 2 || info.PrimaryKey[0] != "tenant_id" || info.PrimaryKey[1] != "user_id" {
		t.Fatalf("unexpected primary key: %#v", info.PrimaryKey)
	}
	if info.AutoIncrementColumn != "user_id" {
		t.Fatalf("unexpected auto-increment column: %q", info.AutoIncrementColumn)
	}
}

func TestGetTranspilerSchema_IgnoresRowIDSentinel(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	mdb, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("GetDatabase failed: %v", err)
	}

	cache, ok := mdb.GetSchemaCache().(*SchemaCache)
	if !ok || cache == nil {
		t.Fatal("schema cache is unavailable")
	}

	cache.Update("logs", &TableSchema{
		PrimaryKeys: []string{"rowid"},
	})

	info, err := dm.GetTranspilerSchema("testdb", "logs")
	if err != nil {
		t.Fatalf("GetTranspilerSchema failed: %v", err)
	}

	if len(info.PrimaryKey) != 0 {
		t.Fatalf("expected empty primary key, got %#v", info.PrimaryKey)
	}
}

// TestGetTranspilerSchema_AutoIncrementOrdinal pins the ordinal a column-less
// "INSERT INTO t VALUES (...)" indexes its VALUES tuple by. TableSchema.Columns
// is declaration order with GENERATED columns removed, which is exactly the
// tuple SQLite expects, so the ordinal must be the index into Columns and NOT
// PRAGMA table_xinfo's cid (ColumnPositions), which counts hidden columns too.
func TestGetTranspilerSchema_AutoIncrementOrdinal(t *testing.T) {
	dm, _ := setupTestDatabaseManager(t)
	defer dm.Close()

	if err := dm.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	mdb, err := dm.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("GetDatabase failed: %v", err)
	}

	cache, ok := mdb.GetSchemaCache().(*SchemaCache)
	if !ok || cache == nil {
		t.Fatal("schema cache is unavailable")
	}

	// A table whose auto-increment column is neither first nor at its cid:
	// "gen" is a generated column, so it is absent from Columns while it still
	// occupies a cid, which is what makes ColumnPositions the wrong answer.
	cache.Update("events", &TableSchema{
		Columns:          []string{"tenant", "name", "event_id"},
		ColumnPositions:  []int{0, 2, 3},
		PrimaryKeys:      []string{"event_id"},
		AutoIncrementCol: "event_id",
	})
	cache.Update("nokey", &TableSchema{
		Columns:     []string{"a", "b"},
		PrimaryKeys: []string{"rowid"},
	})

	// Mutation: return columnOrdinal(schema.ColumnPositions...) or a constant
	// 0; this fires with 3 or 0 respectively.
	info, err := dm.GetTranspilerSchema("testdb", "events")
	if err != nil {
		t.Fatalf("GetTranspilerSchema failed: %v", err)
	}
	if info.AutoIncrementOrdinal != 2 {
		t.Fatalf("AutoIncrementOrdinal = %d, want 2", info.AutoIncrementOrdinal)
	}

	// Mutation: drop the `name == ""` guard in columnOrdinal and let the loop
	// return 0 for an absent column; a table with no auto-increment column
	// reports ordinal 0 and the rule would inject into its first column.
	info, err = dm.GetTranspilerSchema("testdb", "nokey")
	if err != nil {
		t.Fatalf("GetTranspilerSchema failed: %v", err)
	}
	if info.AutoIncrementColumn != "" {
		t.Fatalf("AutoIncrementColumn = %q, want empty", info.AutoIncrementColumn)
	}
	if info.AutoIncrementOrdinal != -1 {
		t.Fatalf("AutoIncrementOrdinal = %d, want -1", info.AutoIncrementOrdinal)
	}
}
