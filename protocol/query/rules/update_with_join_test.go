package rules

import (
	"strings"
	"testing"
)

func TestUpdateWithJoinRule_SimpleConstantValue(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE wp_posts p INNER JOIN wp_term_relationships tr ON p.ID = tr.object_id SET p.comment_status = 'closed' WHERE tr.term_taxonomy_id = 5`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "UPDATE wp_posts SET comment_status = 'closed'") {
		t.Errorf("Expected UPDATE wp_posts SET comment_status = 'closed', got: %s", result)
	}

	if !strings.Contains(result, "WHERE rowid IN (") {
		t.Errorf("Expected WHERE rowid IN clause, got: %s", result)
	}

	if !strings.Contains(result, "SELECT p.rowid FROM") {
		t.Errorf("Expected SELECT p.rowid FROM in subquery, got: %s", result)
	}
}

func TestUpdateWithJoinRule_ValueFromJoinedTable(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE users u INNER JOIN orders o ON u.id = o.user_id SET u.last_order = o.created_at WHERE o.status = 'completed'`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "UPDATE users SET last_order = (") {
		t.Errorf("Expected correlated subquery for SET clause, got: %s", result)
	}

	if !strings.Contains(result, "WHERE rowid IN (") {
		t.Errorf("Expected WHERE rowid IN clause, got: %s", result)
	}

	if !strings.Contains(result, "LIMIT 1") {
		t.Errorf("Expected LIMIT 1 in correlated subquery, got: %s", result)
	}
}

func TestUpdateWithJoinRule_MultipleSetColumns(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE products p INNER JOIN categories c ON p.category_id = c.id SET p.category_name = c.name, p.updated = 1 WHERE c.active = true`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "category_name = (") {
		t.Errorf("Expected correlated subquery for category_name, got: %s", result)
	}

	if !strings.Contains(result, "updated = 1") {
		t.Errorf("Expected simple assignment for updated, got: %s", result)
	}
}

func TestUpdateWithJoinRule_LeftJoin(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE customers c LEFT JOIN orders o ON c.id = o.customer_id SET c.has_orders = 0 WHERE o.id IS NULL`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "UPDATE customers") {
		t.Errorf("Expected UPDATE customers, got: %s", result)
	}

	if !strings.Contains(result, "left join") {
		t.Errorf("Expected LEFT JOIN preserved in subquery, got: %s", result)
	}
}

func TestUpdateWithJoinRule_RightJoin(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE users u RIGHT JOIN profiles p ON u.id = p.user_id SET u.profile_complete = true WHERE p.bio IS NOT NULL`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "right join") {
		t.Errorf("Expected RIGHT JOIN preserved in subquery, got: %s", result)
	}
}

func TestUpdateWithJoinRule_ComplexWhere(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE inventory i INNER JOIN warehouses w ON i.warehouse_id = w.id SET i.status = 'unavailable' WHERE w.region = 'EU' AND i.quantity < 10 AND (i.reserved = 0 OR i.expired = 1)`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "w.region = 'EU'") {
		t.Errorf("Expected WHERE clause preserved, got: %s", result)
	}

	if !strings.Contains(result, "i.quantity < 10") {
		t.Errorf("Expected complex WHERE conditions preserved, got: %s", result)
	}
}

func TestUpdateWithJoinRule_WithOrderByAndLimit(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE posts p INNER JOIN users u ON p.author_id = u.id SET p.featured = 1 WHERE u.premium = 1 ORDER BY p.created_at DESC LIMIT 10`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(strings.ToLower(result), "order by") {
		t.Errorf("Expected ORDER BY preserved in WHERE IN subquery, got: %s", result)
	}

	if !strings.Contains(strings.ToLower(result), "limit 10") {
		t.Errorf("Expected LIMIT preserved in WHERE IN subquery, got: %s", result)
	}
}

func TestUpdateWithJoinRule_NoJoin(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE users SET status = 'active' WHERE created_at > '2024-01-01'`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if applied {
		t.Fatal("Expected rule NOT to apply for simple UPDATE")
	}

	if result != sql {
		t.Errorf("Expected SQL unchanged, got: %s", result)
	}
}

func TestUpdateWithJoinRule_NonUpdateStatement(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if applied {
		t.Fatal("Expected rule NOT to apply for SELECT")
	}

	if result != sql {
		t.Errorf("Expected SQL unchanged, got: %s", result)
	}
}

func TestUpdateWithJoinRule_InvalidSQL(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE users u INNER JOIN WHERE invalid`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern should not error on invalid SQL: %v", err)
	}

	if applied {
		t.Fatal("Expected rule NOT to apply for invalid SQL")
	}

	if result != sql {
		t.Errorf("Expected SQL unchanged, got: %s", result)
	}
}

func TestUpdateWithJoinRule_Priority(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	if rule.Priority() != 13 {
		t.Errorf("Expected priority 13, got %d", rule.Priority())
	}
}

func TestUpdateWithJoinRule_Name(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	if rule.Name() != "UpdateWithJoin" {
		t.Errorf("Expected name 'UpdateWithJoin', got %s", rule.Name())
	}
}

func TestUpdateWithJoinRule_ApplyAST(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	_, applied, err := rule.ApplyAST(nil)
	if err != nil {
		t.Fatalf("ApplyAST failed: %v", err)
	}

	if applied {
		t.Fatal("Expected ApplyAST to not apply (pattern-based rule)")
	}
}

func TestUpdateWithJoinRule_ThreeWayJoin(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE users u INNER JOIN orders o ON u.id = o.user_id INNER JOIN products p ON o.product_id = p.id SET u.last_product = p.name WHERE p.category = 'electronics'`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "UPDATE users") {
		t.Errorf("Expected UPDATE users, got: %s", result)
	}

	if !strings.Contains(result, "last_product = (") {
		t.Errorf("Expected correlated subquery for last_product, got: %s", result)
	}
}

func TestUpdateWithJoinRule_MixedSetClause(t *testing.T) {
	rule := &UpdateWithJoinRule{}

	sql := `UPDATE employees e INNER JOIN departments d ON e.dept_id = d.id SET e.dept_name = d.name, e.updated_at = NOW(), e.version = e.version + 1 WHERE d.active = 1`

	result, applied, err := rule.ApplyPattern(sql)
	if err != nil {
		t.Fatalf("ApplyPattern failed: %v", err)
	}

	if !applied {
		t.Fatal("Expected rule to apply")
	}

	if !strings.Contains(result, "dept_name = (") {
		t.Errorf("Expected correlated subquery for dept_name, got: %s", result)
	}

	if !strings.Contains(strings.ToLower(result), "updated_at = now()") {
		t.Errorf("Expected simple assignment for updated_at, got: %s", result)
	}

	if !strings.Contains(result, "version = e.version + 1") {
		t.Errorf("Expected simple assignment for version (self-reference), got: %s", result)
	}
}

func BenchmarkUpdateWithJoinRule_SimpleConstant(b *testing.B) {
	rule := &UpdateWithJoinRule{}
	sql := `UPDATE wp_posts p INNER JOIN wp_term_relationships tr ON p.ID = tr.object_id SET p.comment_status = 'closed' WHERE tr.term_taxonomy_id = 5`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}

func BenchmarkUpdateWithJoinRule_ComplexJoin(b *testing.B) {
	rule := &UpdateWithJoinRule{}
	sql := `UPDATE users u INNER JOIN orders o ON u.id = o.user_id SET u.last_order = o.created_at WHERE o.status = 'completed'`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}

func BenchmarkUpdateWithJoinRule_NoJoin(b *testing.B) {
	rule := &UpdateWithJoinRule{}
	sql := `UPDATE users SET status = 'active' WHERE created_at > '2024-01-01'`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rule.ApplyPattern(sql)
	}
}

func equalizeSQL(a, b string) bool {
	normalize := func(s string) string {
		s = strings.ReplaceAll(s, "\n", " ")
		s = strings.ReplaceAll(s, "\t", " ")
		for strings.Contains(s, "  ") {
			s = strings.ReplaceAll(s, "  ", " ")
		}
		return strings.TrimSpace(strings.ToLower(s))
	}
	return normalize(a) == normalize(b)
}
