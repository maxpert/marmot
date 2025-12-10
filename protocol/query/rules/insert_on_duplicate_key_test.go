package rules

import (
	"testing"
)

func TestInsertOnDuplicateKeyRule_Name(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	if rule.Name() != "InsertOnDuplicateKey" {
		t.Errorf("expected name 'InsertOnDuplicateKey', got '%s'", rule.Name())
	}
}

func TestInsertOnDuplicateKeyRule_Priority(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	if rule.Priority() != 6 {
		t.Errorf("expected priority 6, got %d", rule.Priority())
	}
}

func TestInsertOnDuplicateKeyRule_SimpleUpsert(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE count = count + VALUES(count)"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	expected := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON CONFLICT (id) DO UPDATE SET count = `count` + excluded.`count`"
	if result != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result)
	}
}

func TestInsertOnDuplicateKeyRule_MultiRowInsert(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1), (2, 'Bob', 2) ON DUPLICATE KEY UPDATE count = VALUES(count)"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	expected := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1), (2, 'Bob', 2) ON CONFLICT (id) DO UPDATE SET count = excluded.`count`"
	if result != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result)
	}
}

func TestInsertOnDuplicateKeyRule_MultipleUpdateColumns(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE name = VALUES(name), count = count + VALUES(count)"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	expected := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON CONFLICT (id) DO UPDATE SET name = excluded.`name`, count = `count` + excluded.`count`"
	if result != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result)
	}
}

func TestInsertOnDuplicateKeyRule_ComplexExpression(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO stats (id, visits, revenue) VALUES (1, 10, 100) ON DUPLICATE KEY UPDATE visits = visits + VALUES(visits), revenue = revenue + VALUES(revenue) * 2"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	expected := "INSERT INTO stats (id, visits, revenue) VALUES (1, 10, 100) ON CONFLICT (id) DO UPDATE SET visits = visits + excluded.visits, revenue = revenue + excluded.revenue * 2"
	if result != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result)
	}
}

func TestInsertOnDuplicateKeyRule_NoOnDuplicate(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name) VALUES (1, 'Alice')"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatal("rule should not have been applied")
	}

	if result != sql {
		t.Errorf("SQL should be unchanged, got: %s", result)
	}
}

func TestInsertOnDuplicateKeyRule_NotInsertStatement(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "SELECT * FROM users"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatal("rule should not have been applied")
	}

	if result != sql {
		t.Error("SQL should be unchanged")
	}
}

func TestInsertOnDuplicateKeyRule_ApplyAST(t *testing.T) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name) VALUES (1, 'Alice')"

	stmt, err := parseSQL(sql)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	result, applied, err := rule.ApplyAST(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatal("ApplyAST should not apply transformations")
	}
	if result != stmt {
		t.Errorf("statement should be unchanged")
	}
}

func BenchmarkInsertOnDuplicateKeyRule_SimpleUpsert(b *testing.B) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE count = count + VALUES(count)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}

func BenchmarkInsertOnDuplicateKeyRule_MultiRow(b *testing.B) {
	rule := &InsertOnDuplicateKeyRule{}
	sql := "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 3) ON DUPLICATE KEY UPDATE count = VALUES(count), name = VALUES(name)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}
