package rules

import (
	"strings"
	"testing"
)

func TestDeleteWithJoinRule_Name(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	if rule.Name() != "DeleteWithJoin" {
		t.Errorf("expected name 'DeleteWithJoin', got '%s'", rule.Name())
	}
}

func TestDeleteWithJoinRule_Priority(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	if rule.Priority() != 12 {
		t.Errorf("expected priority 12, got %d", rule.Priority())
	}
}

func TestDeleteWithJoinRule_SimpleInnerJoin(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u JOIN banned_users b ON u.id = b.user_id"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	if !strings.Contains(result, "DELETE FROM users WHERE rowid IN") {
		t.Errorf("expected DELETE FROM users WHERE rowid IN, got: %s", result)
	}
	if !strings.Contains(result, "SELECT u.rowid FROM") {
		t.Errorf("expected SELECT u.rowid FROM in subquery, got: %s", result)
	}
	if !strings.Contains(strings.ToLower(result), "join") {
		t.Errorf("expected JOIN to be preserved, got: %s", result)
	}
}

func TestDeleteWithJoinRule_LeftJoin(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE p FROM wp_posts p LEFT JOIN wp_postmeta m ON p.ID = m.post_id WHERE p.post_type = 'revision'"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	if !strings.Contains(result, "DELETE FROM wp_posts WHERE rowid IN") {
		t.Errorf("expected DELETE FROM wp_posts WHERE rowid IN, got: %s", result)
	}
	if !strings.Contains(result, "SELECT p.rowid FROM") {
		t.Errorf("expected SELECT p.rowid FROM in subquery, got: %s", result)
	}
	if !strings.Contains(strings.ToLower(result), "left join") {
		t.Errorf("expected LEFT JOIN to be preserved, got: %s", result)
	}
	if !strings.Contains(result, "WHERE p.post_type = 'revision'") {
		t.Errorf("expected WHERE clause to be preserved, got: %s", result)
	}
}

func TestDeleteWithJoinRule_MultipleJoins(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id WHERE p.category = 'test'"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	if !strings.Contains(result, "DELETE FROM users WHERE rowid IN") {
		t.Errorf("expected DELETE FROM users WHERE rowid IN, got: %s", result)
	}
	if !strings.Contains(result, "SELECT u.rowid FROM") {
		t.Errorf("expected SELECT u.rowid FROM in subquery, got: %s", result)
	}
	if strings.Count(strings.ToLower(result), "join") < 2 {
		t.Errorf("expected 2 JOINs to be preserved, got: %s", result)
	}
}

func TestDeleteWithJoinRule_ComplexWhereClause(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u JOIN sessions s ON u.id = s.user_id WHERE s.expired = 1 AND u.status = 'inactive'"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	if !strings.Contains(result, "DELETE FROM users WHERE rowid IN") {
		t.Errorf("expected DELETE FROM users WHERE rowid IN, got: %s", result)
	}
	if !strings.Contains(result, "WHERE s.expired = 1") {
		t.Errorf("expected WHERE clause to be preserved, got: %s", result)
	}
}

func TestDeleteWithJoinRule_NoJoin(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE FROM users WHERE id = 1"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatal("rule should not have been applied to DELETE without JOIN")
	}
	if result != sql {
		t.Errorf("SQL should be unchanged, got: %s", result)
	}
}

func TestDeleteWithJoinRule_SimpleDeleteWithAlias(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE FROM users u WHERE u.id = 1"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatal("rule should not have been applied to DELETE without JOIN")
	}
	if result != sql {
		t.Errorf("SQL should be unchanged, got: %s", result)
	}
}

func TestDeleteWithJoinRule_MultiTableDelete(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u, o FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'inactive'"

	_, applied, err := rule.ApplyPattern(sql)
	if err == nil {
		t.Fatal("expected error for multi-table DELETE")
	}
	if applied {
		t.Fatal("rule should not have been applied for multi-table DELETE")
	}
	if !strings.Contains(err.Error(), "multi-table DELETE not supported") {
		t.Errorf("expected multi-table DELETE error message, got: %v", err)
	}
}

func TestDeleteWithJoinRule_NotDeleteStatement(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "SELECT * FROM users u JOIN orders o ON u.id = o.user_id"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatal("rule should not have been applied to non-DELETE statement")
	}
	if result != sql {
		t.Errorf("SQL should be unchanged, got: %s", result)
	}
}

func TestDeleteWithJoinRule_InvalidSQL(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE INVALID SYNTAX"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error for invalid SQL: %v", err)
	}
	if applied {
		t.Fatal("rule should not have been applied to invalid SQL")
	}
	if result != sql {
		t.Errorf("SQL should be unchanged for invalid syntax, got: %s", result)
	}
}

func TestDeleteWithJoinRule_NoAlias(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE users FROM users JOIN banned_users ON users.id = banned_users.user_id"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	if !strings.Contains(result, "DELETE FROM users WHERE rowid IN") {
		t.Errorf("expected DELETE FROM users WHERE rowid IN, got: %s", result)
	}
	if !strings.Contains(result, "SELECT users.rowid FROM") {
		t.Errorf("expected SELECT users.rowid FROM in subquery, got: %s", result)
	}
}

func TestDeleteWithJoinRule_RightJoin(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u RIGHT JOIN sessions s ON u.id = s.user_id WHERE s.active = 0"

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatal("rule should have been applied")
	}

	if !strings.Contains(result, "DELETE FROM users WHERE rowid IN") {
		t.Errorf("expected DELETE FROM users WHERE rowid IN, got: %s", result)
	}
	if !strings.Contains(strings.ToLower(result), "right join") {
		t.Errorf("expected RIGHT JOIN to be preserved, got: %s", result)
	}
}

func TestDeleteWithJoinRule_ApplyAST(t *testing.T) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u JOIN orders o ON u.id = o.user_id"

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

func BenchmarkDeleteWithJoinRule_SimpleJoin(b *testing.B) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u JOIN banned_users b ON u.id = b.user_id"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}

func BenchmarkDeleteWithJoinRule_ComplexJoin(b *testing.B) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE u FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id WHERE p.category = 'test' AND u.status = 'inactive' ORDER BY u.created_at LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}

func BenchmarkDeleteWithJoinRule_NoJoin(b *testing.B) {
	rule := &DeleteWithJoinRule{}
	sql := "DELETE FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}
