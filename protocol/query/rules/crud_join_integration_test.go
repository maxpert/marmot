package rules

import (
	"strings"
	"testing"
)

// ruleInterface is a local interface to avoid circular imports with query.Rule
type ruleInterface interface {
	Name() string
	Priority() int
	ApplyPattern(sql string) (string, bool, error)
}

func TestCRUDQueryCompatibility(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantContains   []string
		wantNotContain []string
	}{
		{
			name: "Product meta upsert with ON DUPLICATE KEY",
			sql: "INSERT INTO wp_wc_product_meta_lookup (product_id, sku, stock_quantity) " +
				"VALUES (123, 'PROD-001', 50) " +
				"ON DUPLICATE KEY UPDATE stock_quantity = stock_quantity + VALUES(stock_quantity)",
			wantContains: []string{
				"ON CONFLICT",
				"DO UPDATE SET",
				"stock_quantity",
			},
			wantNotContain: []string{
				"ON DUPLICATE KEY",
				"VALUES(stock_quantity)",
			},
		},
		{
			name: "Revision cleanup with LEFT JOIN",
			sql: "DELETE a FROM wp_posts a " +
				"LEFT JOIN wp_postmeta b ON (a.ID = b.post_id) " +
				"WHERE a.post_type = 'revision'",
			wantContains: []string{
				"DELETE FROM wp_posts",
				"WHERE rowid IN",
				"SELECT a.rowid",
				"left join",
			},
			wantNotContain: []string{
				"DELETE a FROM",
			},
		},
		{
			name: "Close comments for category posts with INNER JOIN",
			sql: "UPDATE wp_posts p " +
				"INNER JOIN wp_term_relationships tr ON p.ID = tr.object_id " +
				"SET p.comment_status = 'closed' " +
				"WHERE tr.term_taxonomy_id = 5",
			wantContains: []string{
				"UPDATE wp_posts",
				"comment_status = 'closed'",
				"WHERE rowid IN",
			},
			wantNotContain: []string{
				"UPDATE wp_posts p INNER JOIN",
			},
		},
		{
			name: "Transient REPLACE into INSERT OR REPLACE",
			sql: "REPLACE INTO wp_options (option_name, option_value, autoload) " +
				"VALUES ('_transient_my_cache', 'data', 'no')",
			wantContains: []string{
				"INSERT OR REPLACE",
				"INTO wp_options",
			},
			wantNotContain: []string{},
		},
		{
			name: "Stats update with INNER JOIN",
			sql: "UPDATE wp_users u " +
				"INNER JOIN wp_wc_customer_lookup c ON u.ID = c.user_id " +
				"SET u.display_name = c.username " +
				"WHERE c.customer_id > 0",
			wantContains: []string{
				"UPDATE wp_users",
				"WHERE rowid IN",
				"display_name",
			},
			wantNotContain: []string{},
		},
		{
			name: "Bulk product import upsert",
			sql: "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) " +
				"VALUES (100, '_price', '29.99'), (101, '_price', '39.99') " +
				"ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)",
			wantContains: []string{
				"ON CONFLICT",
				"excluded",
			},
			wantNotContain: []string{
				"ON DUPLICATE KEY",
			},
		},
		{
			name: "Delete orphaned comments with JOIN",
			sql: "DELETE c FROM wp_comments c " +
				"LEFT JOIN wp_posts p ON c.comment_post_ID = p.ID " +
				"WHERE p.ID IS NULL",
			wantContains: []string{
				"DELETE FROM wp_comments",
				"WHERE rowid IN",
				"SELECT c.rowid",
				"left join",
			},
			wantNotContain: []string{
				"DELETE c FROM",
			},
		},
		{
			name: "Update user flags with complex JOIN and multiple conditions",
			sql: "UPDATE wp_usermeta um " +
				"INNER JOIN wp_users u ON um.user_id = u.ID " +
				"INNER JOIN wp_posts p ON u.ID = p.post_author " +
				"SET um.meta_value = 'verified' " +
				"WHERE p.post_type = 'post' AND p.post_status = 'publish'",
			wantContains: []string{
				"UPDATE wp_usermeta",
				"WHERE rowid IN",
				"join",
			},
			wantNotContain: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSQL(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rules := []ruleInterface{
				&ReplaceIntoRule{},
				&InsertOnDuplicateKeyRule{},
				&DeleteWithJoinRule{},
				&UpdateWithJoinRule{},
			}

			sql := tt.sql
			for _, rule := range rules {
				newSQL, applied, err := rule.ApplyPattern(sql)
				if err != nil {
					t.Fatalf("rule %s failed: %v", rule.Name(), err)
				}
				if applied {
					sql = newSQL
				}
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(sql, want) {
					t.Errorf("expected output to contain %q, got:\n%s", want, sql)
				}
			}

			for _, notWant := range tt.wantNotContain {
				if strings.Contains(sql, notWant) {
					t.Errorf("expected output NOT to contain %q, got:\n%s", notWant, sql)
				}
			}

			t.Logf("Original: %s", tt.sql)
			t.Logf("Transformed: %s", sql)
		})
	}
}

func TestInsertOnDuplicateKeyIntegration(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		validate func(t *testing.T, result string)
	}{
		{
			name: "Simple product meta upsert",
			sql: "INSERT INTO wp_postmeta (post_id, meta_key, meta_value) " +
				"VALUES (42, '_regular_price', '99.99') " +
				"ON DUPLICATE KEY UPDATE meta_value = VALUES(meta_value)",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "ON CONFLICT") {
					t.Errorf("expected ON CONFLICT, got: %s", result)
				}
				if !strings.Contains(result, "excluded") {
					t.Errorf("expected 'excluded' keyword, got: %s", result)
				}
			},
		},
		{
			name: "Product meta with arithmetic",
			sql: "INSERT INTO wp_wc_product_meta_lookup (product_id, stock_quantity) " +
				"VALUES (5, 10) " +
				"ON DUPLICATE KEY UPDATE stock_quantity = stock_quantity + VALUES(stock_quantity)",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "ON CONFLICT") {
					t.Errorf("expected ON CONFLICT, got: %s", result)
				}
				if !strings.Contains(strings.ToLower(result), "excluded") {
					t.Errorf("expected 'excluded' keyword (case insensitive), got: %s", result)
				}
			},
		},
		{
			name: "Multi-row options upsert",
			sql: "INSERT INTO wp_options (option_name, option_value) " +
				"VALUES ('siteurl', 'http://example.com'), ('home', 'http://example.com') " +
				"ON DUPLICATE KEY UPDATE option_value = VALUES(option_value)",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "ON CONFLICT") {
					t.Errorf("expected ON CONFLICT, got: %s", result)
				}
			},
		},
	}

	rule := &InsertOnDuplicateKeyRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, applied, err := rule.ApplyPattern(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !applied {
				t.Fatal("rule should have been applied")
			}

			tt.validate(t, result)
			t.Logf("Transformed to: %s", result)
		})
	}
}

func TestDeleteWithJoinIntegration(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		validate func(t *testing.T, result string)
	}{
		{
			name: "Delete spam comments with INNER JOIN",
			sql: "DELETE c FROM wp_comments c " +
				"INNER JOIN wp_posts p ON c.comment_post_ID = p.ID " +
				"WHERE c.comment_approved = 'spam'",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "DELETE FROM wp_comments") {
					t.Errorf("expected DELETE FROM wp_comments, got: %s", result)
				}
				if !strings.Contains(result, "WHERE rowid IN") {
					t.Errorf("expected WHERE rowid IN, got: %s", result)
				}
				if !strings.Contains(result, "SELECT c.rowid") {
					t.Errorf("expected SELECT c.rowid in subquery, got: %s", result)
				}
			},
		},
		{
			name: "Delete orphaned rows with LEFT JOIN and IS NULL",
			sql: "DELETE r FROM wp_posts r " +
				"LEFT JOIN wp_posts p ON r.post_parent = p.ID " +
				"WHERE r.post_type = 'revision' AND p.ID IS NULL",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "DELETE FROM wp_posts") {
					t.Errorf("expected DELETE FROM wp_posts, got: %s", result)
				}
				if !strings.Contains(result, "WHERE rowid IN") {
					t.Errorf("expected WHERE rowid IN, got: %s", result)
				}
				if !strings.Contains(strings.ToLower(result), "left join") {
					t.Errorf("expected LEFT JOIN, got: %s", result)
				}
			},
		},
		{
			name: "Delete orphaned metadata rows",
			sql: "DELETE um FROM wp_usermeta um " +
				"LEFT JOIN wp_users u ON um.user_id = u.ID " +
				"WHERE u.ID IS NULL",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "DELETE FROM wp_usermeta") {
					t.Errorf("expected DELETE FROM wp_usermeta, got: %s", result)
				}
				if !strings.Contains(result, "um.ID IS NULL") {
					t.Logf("note: WHERE clause is: %s", result)
				}
			},
		},
	}

	rule := &DeleteWithJoinRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, applied, err := rule.ApplyPattern(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !applied {
				t.Fatal("rule should have been applied")
			}

			tt.validate(t, result)
			t.Logf("Transformed to: %s", result)
		})
	}
}

func TestUpdateWithJoinIntegration(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		validate func(t *testing.T, result string)
	}{
		{
			name: "Update row status based on joined table condition",
			sql: "UPDATE wp_posts p " +
				"INNER JOIN wp_users u ON p.post_author = u.ID " +
				"SET p.post_status = 'draft' " +
				"WHERE u.user_status = 0",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "UPDATE wp_posts") {
					t.Errorf("expected UPDATE wp_posts, got: %s", result)
				}
				if !strings.Contains(result, "WHERE rowid IN") {
					t.Errorf("expected WHERE rowid IN, got: %s", result)
				}
				if !strings.Contains(result, "post_status") {
					t.Errorf("expected post_status in SET clause, got: %s", result)
				}
			},
		},
		{
			name: "Update metadata based on joined table type",
			sql: "UPDATE wp_postmeta pm " +
				"INNER JOIN wp_posts p ON pm.post_id = p.ID " +
				"SET pm.meta_value = 'updated' " +
				"WHERE p.post_type = 'product' AND pm.meta_key = '_featured'",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "UPDATE wp_postmeta") {
					t.Errorf("expected UPDATE wp_postmeta, got: %s", result)
				}
				if !strings.Contains(result, "WHERE rowid IN") {
					t.Errorf("expected WHERE rowid IN, got: %s", result)
				}
				if !strings.Contains(result, "meta_value") {
					t.Errorf("expected meta_value in SET clause, got: %s", result)
				}
			},
		},
		{
			name: "Update metadata from relationship join",
			sql: "UPDATE wp_usermeta um " +
				"INNER JOIN wp_users u ON um.user_id = u.ID " +
				"SET um.meta_value = u.user_registered " +
				"WHERE um.meta_key = '_registered_date'",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "UPDATE wp_usermeta") {
					t.Errorf("expected UPDATE wp_usermeta, got: %s", result)
				}
				if !strings.Contains(result, "WHERE rowid IN") {
					t.Errorf("expected WHERE rowid IN, got: %s", result)
				}
			},
		},
	}

	rule := &UpdateWithJoinRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, applied, err := rule.ApplyPattern(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !applied {
				t.Fatal("rule should have been applied")
			}

			tt.validate(t, result)
			t.Logf("Transformed to: %s", result)
		})
	}
}

func TestReplaceIntoIntegration(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		validate func(t *testing.T, result string)
	}{
		{
			name: "Transient REPLACE",
			sql: "REPLACE INTO wp_options (option_id, option_name, option_value) " +
				"VALUES (1, '_transient_test', 'value')",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "INSERT OR REPLACE") {
					t.Errorf("expected INSERT OR REPLACE, got: %s", result)
				}
			},
		},
		{
			name: "Order stats REPLACE",
			sql: "REPLACE INTO wp_wc_order_stats (order_id, total_sales, num_items_sold) " +
				"VALUES (999, 250.50, 5)",
			validate: func(t *testing.T, result string) {
				if !strings.Contains(result, "INSERT OR REPLACE") {
					t.Errorf("expected INSERT OR REPLACE, got: %s", result)
				}
			},
		},
	}

	rule := &ReplaceIntoRule{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, applied, err := rule.ApplyPattern(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !applied {
				t.Fatal("rule should have been applied")
			}

			tt.validate(t, result)
			t.Logf("Transformed to: %s", result)
		})
	}
}

func TestComplexCRUDScenarios(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		expectApplied bool
		checkFunc     func(t *testing.T, result string, applied bool)
	}{
		{
			name: "Non-matching SELECT statement",
			sql:  "SELECT * FROM wp_posts WHERE post_status = 'publish'",
			checkFunc: func(t *testing.T, result string, applied bool) {
				if applied {
					t.Error("SELECT statement should not trigger any rule")
				}
			},
		},
		{
			name: "Simple INSERT without ON DUPLICATE KEY",
			sql:  "INSERT INTO wp_posts (post_title, post_content) VALUES ('Test', 'Content')",
			checkFunc: func(t *testing.T, result string, applied bool) {
				if applied {
					t.Error("INSERT without ON DUPLICATE KEY should not trigger InsertOnDuplicateKeyRule")
				}
			},
		},
		{
			name: "DELETE without JOIN",
			sql:  "DELETE FROM wp_comments WHERE comment_approved = 'spam'",
			checkFunc: func(t *testing.T, result string, applied bool) {
				if applied {
					t.Error("DELETE without JOIN should not trigger DeleteWithJoinRule")
				}
			},
		},
		{
			name: "UPDATE without JOIN",
			sql:  "UPDATE wp_posts SET post_status = 'draft' WHERE ID = 1",
			checkFunc: func(t *testing.T, result string, applied bool) {
				if applied {
					t.Error("UPDATE without JOIN should not trigger UpdateWithJoinRule")
				}
			},
		},
	}

	rules := []ruleInterface{
		&ReplaceIntoRule{},
		&InsertOnDuplicateKeyRule{},
		&DeleteWithJoinRule{},
		&UpdateWithJoinRule{},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := tt.sql
			anyApplied := false

			for _, rule := range rules {
				newSQL, applied, err := rule.ApplyPattern(sql)
				if err != nil {
					t.Fatalf("rule %s failed: %v", rule.Name(), err)
				}
				if applied {
					sql = newSQL
					anyApplied = true
				}
			}

			tt.checkFunc(t, sql, anyApplied)
		})
	}
}

func TestCRUDRuleOrdering(t *testing.T) {
	rule1 := &InsertOnDuplicateKeyRule{} // Priority 6
	rule2 := &ReplaceIntoRule{}          // Priority 11
	rule3 := &DeleteWithJoinRule{}       // Priority 12
	rule4 := &UpdateWithJoinRule{}       // Priority 13

	rules := []struct {
		rule ruleInterface
		name string
	}{
		{rule1, "InsertOnDuplicateKeyRule"},
		{rule2, "ReplaceIntoRule"},
		{rule3, "DeleteWithJoinRule"},
		{rule4, "UpdateWithJoinRule"},
	}

	for i := 0; i < len(rules)-1; i++ {
		for j := i + 1; j < len(rules); j++ {
			if rules[i].rule.Priority() > rules[j].rule.Priority() {
				t.Errorf("rule %d (%s, priority %d) should run before rule %d (%s, priority %d)",
					i, rules[i].name, rules[i].rule.Priority(),
					j, rules[j].name, rules[j].rule.Priority())
			}
		}
	}

	t.Logf("Rule priorities - InsertOnDuplicateKey: %d, ReplaceInto: %d, DeleteWithJoin: %d, UpdateWithJoin: %d",
		rule1.Priority(), rule2.Priority(), rule3.Priority(), rule4.Priority())
}
