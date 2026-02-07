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
