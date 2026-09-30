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

// TestGetTranspilerSchema_AutoIncrementWidth pins the fields the transpiler's
// id injection will read: the declared width and UNSIGNED flag of the
// AUTO-INCREMENT column specifically, not of whichever narrow column happens to
// come first.
//
// The column order here is deliberate. "q" is narrow (16-bit, signed) and sits
// BEFORE the auto-increment column, so a selection loop that took the first
// marked column instead of the named one would report 16/false where the answer
// is 32/true. With the columns the other way round that mistake would be
// invisible.
func TestGetTranspilerSchema_AutoIncrementWidth(t *testing.T) {
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

	// "CREATE TABLE marked (q SMALLINT, id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"
	// as the schema cache holds it after parsing the width markers.
	cache.Update("marked", &TableSchema{
		Columns:          []string{"q", "id"},
		PrimaryKeys:      []string{"id"},
		AutoIncrementCol: "id",
		FullColumns: []ColumnSchema{
			{Name: "q", Type: "INTEGER", DeclaredWidth: 16},
			{Name: "id", Type: "INTEGER", DeclaredWidth: 32, Unsigned: true, ExplicitAutoInc: true, IsPK: true, PKOrder: 1},
		},
	})
	// A BIGINT auto-increment column carries no marker and keeps the 64-bit path.
	cache.Update("wide", &TableSchema{
		Columns:          []string{"id", "v"},
		PrimaryKeys:      []string{"id"},
		AutoIncrementCol: "id",
		FullColumns: []ColumnSchema{
			{Name: "id", Type: "INTEGER", IsPK: true, PKOrder: 1},
			{Name: "v", Type: "TEXT"},
		},
	})

	// Mutation: have the selection loop in GetTranspilerSchema take the first
	// column with a non-zero DeclaredWidth instead of matching
	// AutoIncrementColumn by name. Both assertions below fire, reporting q's
	// 16/false in place of id's 32/true.
	info, err := dm.GetTranspilerSchema("testdb", "marked")
	if err != nil {
		t.Fatalf("GetTranspilerSchema failed: %v", err)
	}
	if info.AutoIncrementWidth != 32 {
		t.Errorf("AutoIncrementWidth = %d, want 32 (id's width, not q's)", info.AutoIncrementWidth)
	}
	if !info.AutoIncrementUnsigned {
		t.Error("AutoIncrementUnsigned = false, want true (id is UNSIGNED, q is not)")
	}
	// The ordinal must still point at the auto-increment column, which is
	// second here. Mutation: return the index of the marked column found by
	// the loop above instead of columnOrdinal's result.
	if info.AutoIncrementOrdinal != 1 {
		t.Errorf("AutoIncrementOrdinal = %d, want 1 (id is the second column)", info.AutoIncrementOrdinal)
	}
	if info.AutoIncrementColumn != "id" {
		t.Errorf("AutoIncrementColumn = %q, want id", info.AutoIncrementColumn)
	}

	// Unmarked means the 64-bit path, not a default width.
	// Mutation: default AutoIncrementWidth to 64 when no marker is present.
	wide, err := dm.GetTranspilerSchema("testdb", "wide")
	if err != nil {
		t.Fatalf("GetTranspilerSchema failed: %v", err)
	}
	if wide.AutoIncrementWidth != 0 {
		t.Errorf("AutoIncrementWidth = %d for an unmarked BIGINT column, want 0", wide.AutoIncrementWidth)
	}
	if wide.AutoIncrementUnsigned {
		t.Error("AutoIncrementUnsigned = true for an unmarked column, want false")
	}
}
