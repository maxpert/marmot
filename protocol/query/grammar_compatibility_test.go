package query

import (
	"testing"
)

// Grammar Compatibility Test Suite
//
// This test suite validates MySQL grammar support through Vitess parser and Marmot's CDC requirements.
//
// Failures can be categorized as:
// 1. VITESS PARSER LIMITATION - The Vitess MySQL parser doesn't support this syntax
// 2. MARMOT CDC REQUIREMENT - Valid SQL but CDC constraints reject it (e.g., missing WHERE, no column list)
//
// Tests marked with shouldParse=false are known limitations that should be documented.

// TestMySQLSelectModifiers tests MySQL-specific SELECT modifiers
func TestMySQLSelectModifiers(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
		limitation  string // Document why it fails
	}{
		// DISTINCT / ALL - SUPPORTED
		{
			name:        "SELECT DISTINCT",
			sql:         "SELECT DISTINCT name FROM users",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT ALL",
			sql:         "SELECT ALL name FROM users",
			wantType:    StatementSelect,
			shouldParse: true,
		},

		// MySQL-specific modifiers - SUPPORTED (removed during transpilation)
		{
			name:        "SELECT HIGH_PRIORITY",
			sql:         "SELECT HIGH_PRIORITY * FROM users",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT SQL_SMALL_RESULT",
			sql:         "SELECT SQL_SMALL_RESULT * FROM users",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT SQL_BIG_RESULT",
			sql:         "SELECT SQL_BIG_RESULT * FROM large_table",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT SQL_BUFFER_RESULT",
			sql:         "SELECT SQL_BUFFER_RESULT * FROM users",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT SQL_NO_CACHE",
			sql:         "SELECT SQL_NO_CACHE * FROM users WHERE id = 1",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT SQL_CALC_FOUND_ROWS",
			sql:         "SELECT SQL_CALC_FOUND_ROWS * FROM users LIMIT 10",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "SELECT with multiple modifiers",
			sql:         "SELECT HIGH_PRIORITY SQL_NO_CACHE DISTINCT name FROM users",
			wantType:    StatementSelect,
			shouldParse: true,
		},

		// STRAIGHT_JOIN - NOW SUPPORTED
		{
			name:        "SELECT STRAIGHT_JOIN",
			sql:         "SELECT STRAIGHT_JOIN u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			wantType:    StatementSelect,
			shouldParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLJoinTypes tests all JOIN type variations - ALL SUPPORTED
func TestMySQLJoinTypes(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
	}{
		// Standard JOIN types - ALL SUPPORTED
		{"INNER JOIN", "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id", true},
		{"LEFT JOIN", "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id", true},
		{"LEFT OUTER JOIN", "SELECT * FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id", true},
		{"RIGHT JOIN", "SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id", true},
		{"RIGHT OUTER JOIN", "SELECT * FROM users u RIGHT OUTER JOIN orders o ON u.id = o.user_id", true},
		{"CROSS JOIN", "SELECT * FROM users CROSS JOIN products", true},
		{"NATURAL JOIN", "SELECT * FROM users NATURAL JOIN user_profiles", true},
		{"NATURAL LEFT JOIN", "SELECT * FROM users NATURAL LEFT JOIN orders", true},

		// JOIN with USING clause - SUPPORTED
		{"JOIN with USING", "SELECT * FROM users u JOIN orders o USING(user_id)", true},
		{"LEFT JOIN with USING", "SELECT * FROM users u LEFT JOIN orders o USING(user_id)", true},
		{"JOIN with multiple USING columns", "SELECT * FROM t1 JOIN t2 USING(col1, col2)", true},

		// Multiple JOINs - SUPPORTED
		{"multiple JOINs", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id", true},
		{"mixed JOIN types", "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id INNER JOIN products p ON o.product_id = p.id", true},

		// Self JOIN - SUPPORTED
		{"self JOIN", "SELECT e1.name AS employee, e2.name AS manager FROM employees e1 LEFT JOIN employees e2 ON e1.manager_id = e2.id", true},

		// JOIN with subquery - SUPPORTED
		{"JOIN with derived table", "SELECT u.name, s.total FROM users u LEFT JOIN (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id) s ON u.id = s.user_id", true},

		// Complex JOIN conditions - SUPPORTED
		{"JOIN with multiple conditions", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.status = 'active' AND o.total > 100", true},
		{"JOIN with OR condition", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id OR u.email = o.email", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail, but succeeded")
				}
			}
		})
	}
}

// TestMySQLSetOperations tests UNION, INTERSECT, EXCEPT
func TestMySQLSetOperations(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
		limitation  string
	}{
		// UNION variations - MOSTLY SUPPORTED
		{
			name:        "simple UNION",
			sql:         "SELECT id FROM users UNION SELECT id FROM admins",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "UNION ALL",
			sql:         "SELECT id FROM t1 UNION ALL SELECT id FROM t2",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "UNION DISTINCT",
			sql:         "SELECT id FROM t1 UNION DISTINCT SELECT id FROM t2",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "multiple UNIONs",
			sql:         "SELECT id FROM t1 UNION SELECT id FROM t2 UNION SELECT id FROM t3",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "UNION with parenthesized ORDER BY",
			sql:         "(SELECT id, name FROM users) UNION (SELECT id, name FROM admins) ORDER BY name",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "UNION with parenthesized LIMIT",
			sql:         "(SELECT id FROM users LIMIT 5) UNION (SELECT id FROM admins LIMIT 5)",
			wantType:    StatementSelect,
			shouldParse: true,
		},
		{
			name:        "UNION with global ORDER BY and LIMIT",
			sql:         "SELECT id FROM users UNION SELECT id FROM admins ORDER BY id LIMIT 10",
			wantType:    StatementSelect,
			shouldParse: true,
		},

		// INTERSECT (MySQL 8.0.31+) - VITESS LIMITATION
		{
			name:        "simple INTERSECT",
			sql:         "SELECT id FROM users INTERSECT SELECT user_id FROM orders",
			wantType:    StatementSelect,
			shouldParse: false,
			limitation:  "VITESS: INTERSECT not supported",
		},
		{
			name:        "INTERSECT ALL",
			sql:         "SELECT id FROM t1 INTERSECT ALL SELECT id FROM t2",
			wantType:    StatementSelect,
			shouldParse: false,
			limitation:  "VITESS: INTERSECT not supported",
		},

		// EXCEPT (MySQL 8.0.31+) - VITESS LIMITATION
		{
			name:        "simple EXCEPT",
			sql:         "SELECT id FROM users EXCEPT SELECT user_id FROM banned_users",
			wantType:    StatementSelect,
			shouldParse: false,
			limitation:  "VITESS: EXCEPT not supported",
		},
		{
			name:        "EXCEPT ALL",
			sql:         "SELECT id FROM t1 EXCEPT ALL SELECT id FROM t2",
			wantType:    StatementSelect,
			shouldParse: false,
			limitation:  "VITESS: EXCEPT not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLInsertVariations tests all INSERT statement variations
func TestMySQLInsertVariations(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
		limitation  string
	}{
		// Basic INSERT - SUPPORTED
		{
			name:        "INSERT with columns",
			sql:         "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
			wantType:    StatementInsert,
			shouldParse: true,
		},
		{
			name:        "INSERT without columns",
			sql:         "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
			wantType:    StatementInsert,
			shouldParse: true, // CDC validation moved to runtime hooks
		},
		{
			name:        "INSERT multiple rows",
			sql:         "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
			wantType:    StatementInsert,
			shouldParse: true,
		},

		// INSERT IGNORE - SUPPORTED
		{
			name:        "INSERT IGNORE",
			sql:         "INSERT IGNORE INTO users (id, name) VALUES (1, 'Alice')",
			wantType:    StatementInsert,
			shouldParse: true,
		},

		// INSERT ... SELECT - now supported (CDC via runtime hooks)
		{
			name:        "INSERT SELECT",
			sql:         "INSERT INTO archive SELECT * FROM users WHERE status = 'inactive'",
			wantType:    StatementInsert,
			shouldParse: true,
		},
		{
			name:        "INSERT SELECT with columns",
			sql:         "INSERT INTO summary (user_id, total) SELECT user_id, SUM(amount) FROM orders GROUP BY user_id",
			wantType:    StatementInsert,
			shouldParse: true,
		},

		// INSERT ... ON DUPLICATE KEY UPDATE - Now supported via transpiler rule
		{
			name:        "INSERT ON DUPLICATE KEY UPDATE",
			sql:         "INSERT INTO users (id, name, count) VALUES (1, 'Alice', 1) ON DUPLICATE KEY UPDATE count = count + 1",
			wantType:    StatementInsert,
			shouldParse: true,
		},

		// INSERT with modifiers - VITESS LIMITATION
		{
			name:        "INSERT LOW_PRIORITY",
			sql:         "INSERT LOW_PRIORITY INTO users (id, name) VALUES (1, 'Alice')",
			wantType:    StatementInsert,
			shouldParse: false,
			limitation:  "VITESS: INSERT LOW_PRIORITY not supported",
		},
		{
			name:        "INSERT DELAYED",
			sql:         "INSERT DELAYED INTO logs (message) VALUES ('log entry')",
			wantType:    StatementInsert,
			shouldParse: false,
			limitation:  "VITESS: INSERT DELAYED not supported (deprecated in MySQL 5.6+)",
		},

		// INSERT with SET syntax - NOW SUPPORTED
		{
			name:        "INSERT SET",
			sql:         "INSERT INTO users SET id = 1, name = 'Alice', email = 'alice@example.com'",
			wantType:    StatementInsert,
			shouldParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLReplaceStatement tests REPLACE INTO variations
func TestMySQLReplaceStatement(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
		limitation  string
	}{
		{
			name:        "simple REPLACE",
			sql:         "REPLACE INTO users (id, name) VALUES (1, 'Alice')",
			wantType:    StatementReplace,
			shouldParse: true,
		},
		{
			name:        "REPLACE without columns",
			sql:         "REPLACE INTO users VALUES (1, 'A'), (2, 'B'), (3, 'C')",
			wantType:    StatementReplace,
			shouldParse: true, // CDC validation moved to runtime hooks
		},
		{
			name:        "REPLACE SELECT",
			sql:         "REPLACE INTO archive SELECT * FROM users WHERE status = 'inactive'",
			wantType:    StatementReplace,
			shouldParse: true, // CDC via runtime hooks
		},
		{
			name:        "REPLACE with SET syntax",
			sql:         "REPLACE INTO users SET id = 1, name = 'Alice'",
			wantType:    StatementReplace,
			shouldParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLUpdateVariations tests UPDATE statement variations
func TestMySQLUpdateVariations(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
		limitation  string
	}{
		// Basic UPDATE - SUPPORTED
		{
			name:        "simple UPDATE",
			sql:         "UPDATE users SET name = 'Bob' WHERE id = 1",
			wantType:    StatementUpdate,
			shouldParse: true,
		},
		{
			name:        "UPDATE multiple columns",
			sql:         "UPDATE users SET name = 'Bob', email = 'bob@example.com', updated_at = NOW() WHERE id = 1",
			wantType:    StatementUpdate,
			shouldParse: true,
		},
		{
			name:        "UPDATE with expression",
			sql:         "UPDATE products SET stock = stock - 1 WHERE id = 100",
			wantType:    StatementUpdate,
			shouldParse: true,
		},

		// UPDATE with subquery - CDC LIMITATION (complex WHERE)
		{
			name:        "UPDATE with subquery in WHERE",
			sql:         "UPDATE users SET status = 'premium' WHERE id IN (SELECT user_id FROM orders WHERE total > 1000)",
			wantType:    StatementUpdate,
			shouldParse: true, // CDC validation moved to runtime hooks
		},

		// UPDATE with JOIN - Supported via UpdateWithJoinRule
		{
			name:        "UPDATE with INNER JOIN",
			sql:         "UPDATE users u INNER JOIN orders o ON u.id = o.user_id SET u.last_order = o.created_at WHERE o.status = 'completed'",
			wantType:    StatementUpdate,
			shouldParse: true,
		},
		{
			name:        "UPDATE multi-table",
			sql:         "UPDATE users, orders SET users.order_count = users.order_count + 1 WHERE users.id = orders.user_id",
			wantType:    StatementUpdate,
			shouldParse: true,
		},

		// UPDATE without WHERE - NOW SUPPORTED
		{
			name:        "UPDATE without WHERE",
			sql:         "UPDATE users SET status = 'inactive' ORDER BY last_login",
			wantType:    StatementUpdate,
			shouldParse: true,
		},

		// UPDATE with LIMIT - NOW SUPPORTED
		{
			name:        "UPDATE with LIMIT",
			sql:         "UPDATE users SET status = 'processed' WHERE status = 'pending' LIMIT 100",
			wantType:    StatementUpdate,
			shouldParse: true,
		},

		// UPDATE with modifiers - VITESS LIMITATION
		{
			name:        "UPDATE LOW_PRIORITY",
			sql:         "UPDATE LOW_PRIORITY users SET name = 'Test' WHERE id = 1",
			wantType:    StatementUpdate,
			shouldParse: false,
			limitation:  "VITESS: UPDATE LOW_PRIORITY not supported",
		},
		{
			name:        "UPDATE IGNORE",
			sql:         "UPDATE IGNORE users SET email = 'duplicate@example.com' WHERE id = 1",
			wantType:    StatementUpdate,
			shouldParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLDeleteVariations tests DELETE statement variations
func TestMySQLDeleteVariations(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
		limitation  string
	}{
		// Basic DELETE - SUPPORTED
		{
			name:        "simple DELETE",
			sql:         "DELETE FROM users WHERE id = 1",
			wantType:    StatementDelete,
			shouldParse: true,
		},

		// DELETE without WHERE - now supported (CDC via runtime hooks)
		{
			name:        "DELETE all rows",
			sql:         "DELETE FROM temp_table",
			wantType:    StatementDelete,
			shouldParse: true, // CDC validation moved to runtime hooks
		},

		// DELETE with subquery - now supported (CDC via runtime hooks)
		{
			name:        "DELETE with IN subquery",
			sql:         "DELETE FROM users WHERE id IN (SELECT user_id FROM banned_users)",
			wantType:    StatementDelete,
			shouldParse: true, // CDC validation moved to runtime hooks
		},

		// DELETE with USING - NOT SUPPORTED (multi-table)
		{
			name:        "DELETE with USING",
			sql:         "DELETE FROM users USING users, banned_users WHERE users.id = banned_users.user_id",
			wantType:    StatementDelete,
			shouldParse: false,
			limitation:  "MARMOT: DELETE from multiple tables not supported",
		},

		// DELETE with JOIN - TRANSPILED
		{
			name:        "DELETE with JOIN",
			sql:         "DELETE u FROM users u JOIN banned_users b ON u.id = b.user_id",
			wantType:    StatementDelete,
			shouldParse: true,
		},

		// DELETE multi-table - NOW SUPPORTED (transpiled to separate DELETE statements)
		{
			name:        "DELETE multi-table",
			sql:         "DELETE users, orders FROM users JOIN orders ON users.id = orders.user_id WHERE users.status = 'spam'",
			wantType:    StatementDelete,
			shouldParse: true,
		},

		// DELETE with ORDER BY+LIMIT - NOW SUPPORTED
		{
			name:        "DELETE with ORDER BY and LIMIT",
			sql:         "DELETE FROM logs WHERE level = 'debug' ORDER BY created_at LIMIT 10000",
			wantType:    StatementDelete,
			shouldParse: true,
		},

		// DELETE with modifiers - VITESS LIMITATION
		{
			name:        "DELETE LOW_PRIORITY",
			sql:         "DELETE LOW_PRIORITY FROM users WHERE status = 'deleted'",
			wantType:    StatementDelete,
			shouldParse: false,
			limitation:  "VITESS: DELETE LOW_PRIORITY not supported",
		},
		{
			name:        "DELETE IGNORE",
			sql:         "DELETE IGNORE FROM users WHERE id = 1",
			wantType:    StatementDelete,
			shouldParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLWindowFunctions tests window function variations - MOSTLY SUPPORTED
func TestMySQLWindowFunctions(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
		limitation  string
	}{
		// Basic window functions - SUPPORTED
		{"ROW_NUMBER", "SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM users", true, ""},
		{"RANK", "SELECT id, RANK() OVER (ORDER BY score DESC) as rnk FROM scores", true, ""},
		{"DENSE_RANK", "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores", true, ""},
		{"NTILE", "SELECT id, NTILE(4) OVER (ORDER BY salary) as quartile FROM employees", true, ""},

		// Navigation functions - SUPPORTED
		{"LAG", "SELECT date, value, LAG(value) OVER (ORDER BY date) as prev_value FROM data", true, ""},
		{"LAG with offset and default", "SELECT date, value, LAG(value, 2, 0) OVER (ORDER BY date) as prev_value FROM data", true, ""},
		{"LEAD", "SELECT date, value, LEAD(value) OVER (ORDER BY date) as next_value FROM data", true, ""},
		{"FIRST_VALUE", "SELECT id, FIRST_VALUE(name) OVER (ORDER BY created_at) as first_name FROM users", true, ""},
		{"LAST_VALUE", "SELECT id, LAST_VALUE(name) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_name FROM users", true, ""},
		{"NTH_VALUE", "SELECT id, NTH_VALUE(name, 3) OVER (ORDER BY created_at) as third_name FROM users", true, ""},

		// Aggregate window functions - SUPPORTED
		{"SUM OVER", "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_total FROM orders", true, ""},
		{"AVG OVER", "SELECT id, amount, AVG(amount) OVER (PARTITION BY category) as category_avg FROM products", true, ""},
		{"COUNT OVER", "SELECT id, COUNT(*) OVER (PARTITION BY user_id) as user_order_count FROM orders", true, ""},

		// PARTITION BY - SUPPORTED
		{"PARTITION BY single column", "SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rnk FROM products", true, ""},
		{"PARTITION BY multiple columns", "SELECT id, ROW_NUMBER() OVER (PARTITION BY year, month ORDER BY day) as day_of_month FROM calendar", true, ""},

		// Frame specifications - MOSTLY SUPPORTED
		{"ROWS BETWEEN", "SELECT id, SUM(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_sum FROM data", true, ""},
		{"ROWS UNBOUNDED PRECEDING", "SELECT id, SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) as cumulative FROM data", true, ""},
		{"ROWS BETWEEN UNBOUNDED", "SELECT id, SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total FROM data", true, ""},
		{
			name:        "RANGE BETWEEN with INTERVAL",
			sql:         "SELECT id, SUM(amount) OVER (ORDER BY date RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) as week_sum FROM data",
			shouldParse: true,
		},

		// Named windows - PARTIALLY SUPPORTED
		{"WINDOW clause", "SELECT id, ROW_NUMBER() OVER w as rn, SUM(amount) OVER w as running_total FROM orders WINDOW w AS (ORDER BY created_at)", true, ""},
		{
			name:        "multiple named windows",
			sql:         "SELECT id, ROW_NUMBER() OVER w1 as rn, SUM(amount) OVER w2 as cat_total FROM orders WINDOW w1 AS (ORDER BY id), w2 AS (PARTITION BY category)",
			shouldParse: false,
			limitation:  "VITESS: Multiple WINDOW definitions not supported",
		},

		// Multiple window functions - SUPPORTED
		{"multiple window functions same OVER", "SELECT id, ROW_NUMBER() OVER (ORDER BY date), RANK() OVER (ORDER BY date), DENSE_RANK() OVER (ORDER BY date) FROM events", true, ""},
		{"multiple window functions different OVERs", "SELECT id, ROW_NUMBER() OVER (ORDER BY id), SUM(amount) OVER (PARTITION BY category), AVG(price) OVER (ORDER BY date ROWS 3 PRECEDING) FROM products", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLCTEVariations tests Common Table Expression variations - ALL SUPPORTED
func TestMySQLCTEVariations(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
	}{
		// Simple CTEs
		{"simple CTE", "WITH active_users AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM active_users", true},
		{"CTE with column list", "WITH user_stats(user_id, total_orders) AS (SELECT user_id, COUNT(*) FROM orders GROUP BY user_id) SELECT * FROM user_stats", true},

		// Multiple CTEs
		{"multiple CTEs", "WITH a AS (SELECT 1 as x), b AS (SELECT 2 as y), c AS (SELECT 3 as z) SELECT * FROM a, b, c", true},
		{"CTEs referencing each other", "WITH first_cte AS (SELECT id FROM users), second_cte AS (SELECT id FROM first_cte WHERE id > 10) SELECT * FROM second_cte", true},

		// Recursive CTEs
		{"recursive CTE for numbers", "WITH RECURSIVE nums AS (SELECT 1 as n UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT * FROM nums", true},
		{"recursive CTE for tree", "WITH RECURSIVE tree AS (SELECT id, name, parent_id, 0 as depth FROM categories WHERE parent_id IS NULL UNION ALL SELECT c.id, c.name, c.parent_id, t.depth + 1 FROM categories c JOIN tree t ON c.parent_id = t.id) SELECT * FROM tree", true},
		{"recursive CTE with LIMIT", "WITH RECURSIVE nums AS (SELECT 1 as n UNION ALL SELECT n + 1 FROM nums WHERE n < 100) SELECT * FROM nums LIMIT 10", true},

		// CTEs with complex queries
		{"CTE with aggregation", "WITH monthly_sales AS (SELECT DATE_FORMAT(sale_date, '%Y-%m') as month, SUM(amount) as total FROM sales GROUP BY month) SELECT * FROM monthly_sales WHERE total > 1000", true},
		{"CTE with window function", "WITH ranked AS (SELECT id, name, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rnk FROM products) SELECT * FROM ranked WHERE rnk <= 3", true},
		{"CTE with JOIN", "WITH user_orders AS (SELECT u.id, u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id) SELECT * FROM user_orders WHERE order_count > 5", true},
		{"CTE with UNION", "WITH cte AS (SELECT id FROM users WHERE status = 'active') SELECT * FROM cte UNION SELECT * FROM cte WHERE id > 100", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail, but succeeded")
				}
			}
		})
	}
}

// TestMySQLLockingClauses tests FOR UPDATE, FOR SHARE, etc.
func TestMySQLLockingClauses(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
		limitation  string
	}{
		// FOR UPDATE - PARTIALLY SUPPORTED
		{"FOR UPDATE", "SELECT * FROM users WHERE id = 1 FOR UPDATE", true, ""},
		{
			name:        "FOR UPDATE OF table",
			sql:         "SELECT * FROM users u JOIN orders o ON u.id = o.user_id FOR UPDATE OF u",
			shouldParse: false,
			limitation:  "VITESS: FOR UPDATE OF not supported",
		},
		{
			name:        "FOR UPDATE NOWAIT",
			sql:         "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT",
			shouldParse: true,
		},
		{
			name:        "FOR UPDATE SKIP LOCKED",
			sql:         "SELECT * FROM queue WHERE processed = 0 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 10",
			shouldParse: false,
			limitation:  "VITESS: FOR UPDATE SKIP LOCKED not supported",
		},

		// FOR SHARE - NOW SUPPORTED
		{
			name:        "FOR SHARE",
			sql:         "SELECT * FROM users WHERE id = 1 FOR SHARE",
			shouldParse: true,
		},
		{"LOCK IN SHARE MODE", "SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLSubqueries tests various subquery patterns
func TestMySQLSubqueries(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
		limitation  string
	}{
		// Scalar subqueries - SUPPORTED
		{"scalar subquery in SELECT", "SELECT id, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count FROM users", true, ""},
		{"scalar subquery in WHERE", "SELECT * FROM users WHERE id = (SELECT MAX(user_id) FROM orders)", true, ""},

		// IN subqueries - SUPPORTED
		{"IN subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)", true, ""},
		{"NOT IN subquery", "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)", true, ""},

		// EXISTS subqueries - SUPPORTED
		{"EXISTS", "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)", true, ""},
		{"NOT EXISTS", "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)", true, ""},

		// ANY/SOME/ALL subqueries - NOW SUPPORTED
		{
			name:        "ANY subquery",
			sql:         "SELECT * FROM products WHERE price > ANY (SELECT price FROM products WHERE category = 'electronics')",
			shouldParse: true,
		},
		{
			name:        "SOME subquery",
			sql:         "SELECT * FROM products WHERE price > SOME (SELECT price FROM products WHERE category = 'electronics')",
			shouldParse: true,
		},
		{
			name:        "ALL subquery",
			sql:         "SELECT * FROM products WHERE price > ALL (SELECT price FROM products WHERE category = 'books')",
			shouldParse: true,
		},

		// Derived tables (FROM subqueries) - SUPPORTED
		{"derived table", "SELECT * FROM (SELECT id, name FROM users WHERE status = 'active') AS active_users", true, ""},
		{"derived table with aggregation", "SELECT u.name, s.total FROM users u JOIN (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id) s ON u.id = s.user_id", true, ""},
		{"nested derived tables", "SELECT * FROM (SELECT * FROM (SELECT id FROM users) inner_t) outer_t", true, ""},

		// Correlated subqueries - SUPPORTED
		{"correlated subquery in WHERE", "SELECT * FROM users u WHERE (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) > 5", true, ""},
		{"correlated subquery in SELECT", "SELECT u.name, (SELECT MAX(o.total) FROM orders o WHERE o.user_id = u.id) as max_order FROM users u", true, ""},

		// Subquery in HAVING - SUPPORTED
		{"subquery in HAVING", "SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id HAVING COUNT(*) > (SELECT AVG(order_count) FROM (SELECT COUNT(*) as order_count FROM orders GROUP BY user_id) t)", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLGroupByHaving tests GROUP BY and HAVING variations
func TestMySQLGroupByHaving(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
		limitation  string
	}{
		// Basic GROUP BY - SUPPORTED
		{"GROUP BY single column", "SELECT category, COUNT(*) FROM products GROUP BY category", true, ""},
		{"GROUP BY multiple columns", "SELECT year, month, SUM(sales) FROM revenue GROUP BY year, month", true, ""},
		{"GROUP BY with alias", "SELECT YEAR(created_at) as yr, COUNT(*) FROM orders GROUP BY yr", true, ""},
		{"GROUP BY column position", "SELECT category, COUNT(*) FROM products GROUP BY 1", true, ""},
		{"GROUP BY expression", "SELECT YEAR(created_at), MONTH(created_at), COUNT(*) FROM orders GROUP BY YEAR(created_at), MONTH(created_at)", true, ""},

		// HAVING clause - SUPPORTED
		{"GROUP BY with HAVING", "SELECT category, COUNT(*) as cnt FROM products GROUP BY category HAVING cnt > 5", true, ""},
		{"HAVING with aggregate function", "SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id HAVING SUM(amount) > 1000", true, ""},
		{"HAVING with multiple conditions", "SELECT category, AVG(price) as avg_price, COUNT(*) as cnt FROM products GROUP BY category HAVING AVG(price) > 50 AND COUNT(*) >= 10", true, ""},

		// WITH ROLLUP - NOW SUPPORTED
		{
			name:        "GROUP BY WITH ROLLUP",
			sql:         "SELECT year, quarter, SUM(sales) FROM revenue GROUP BY year, quarter WITH ROLLUP",
			shouldParse: true,
		},

		// Multiple aggregate functions - SUPPORTED
		{"multiple aggregates", "SELECT category, COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM products GROUP BY category", true, ""},
		{"GROUP BY with ORDER BY", "SELECT category, COUNT(*) as cnt FROM products GROUP BY category ORDER BY cnt DESC", true, ""},
		{"GROUP BY with LIMIT", "SELECT category, COUNT(*) as cnt FROM products GROUP BY category ORDER BY cnt DESC LIMIT 10", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}

// TestMySQLLimitOffset tests LIMIT and OFFSET variations - ALL SUPPORTED
func TestMySQLLimitOffset(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name               string
		sql                string
		shouldParse        bool
		wantTranspiledSQL  string
		checkTranspilation bool
	}{
		// Standard LIMIT
		{"LIMIT alone", "SELECT * FROM users LIMIT 10", true, "", false},
		{"LIMIT with OFFSET", "SELECT * FROM users LIMIT 10 OFFSET 5", true, "", false},

		// MySQL-style LIMIT offset, count (transpiled)
		{
			name:               "MySQL LIMIT offset, count",
			sql:                "SELECT * FROM users LIMIT 5, 10",
			shouldParse:        true,
			wantTranspiledSQL:  "SELECT * FROM users LIMIT 10 OFFSET 5",
			checkTranspilation: true,
		},
		{
			name:               "MySQL LIMIT 0, count",
			sql:                "SELECT * FROM users LIMIT 0, 20",
			shouldParse:        true,
			wantTranspiledSQL:  "SELECT * FROM users LIMIT 20 OFFSET 0",
			checkTranspilation: true,
		},

		// Edge cases
		{"LIMIT 0", "SELECT * FROM users LIMIT 0", true, "", false},
		{"large LIMIT", "SELECT * FROM users LIMIT 999999999", true, "", false},
		{"LIMIT with ORDER BY", "SELECT * FROM users ORDER BY id DESC LIMIT 10", true, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}

				if tt.checkTranspilation && ctx.TranspiledSQL != tt.wantTranspiledSQL {
					t.Errorf("TranspiledSQL = %q, want %q", ctx.TranspiledSQL, tt.wantTranspiledSQL)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail, but succeeded")
				}
			}
		})
	}
}

// TestMySQLDDLStatements tests DDL statement variations - ALL SUPPORTED
func TestMySQLDDLStatements(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		wantType    StatementType
		shouldParse bool
	}{
		// CREATE TABLE
		{"CREATE TABLE basic", "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", StatementDDL, true},
		{"CREATE TABLE IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)", StatementDDL, true},
		{"CREATE TABLE with multiple columns", "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total DECIMAL(10,2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", StatementDDL, true},
		{"CREATE TABLE with FOREIGN KEY", "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))", StatementDDL, true},
		{"CREATE TABLE with INDEX", "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255), INDEX idx_email (email))", StatementDDL, true},

		// ALTER TABLE
		{"ALTER TABLE ADD COLUMN", "ALTER TABLE users ADD COLUMN age INT", StatementDDL, true},
		{"ALTER TABLE DROP COLUMN", "ALTER TABLE users DROP COLUMN age", StatementDDL, true},
		{"ALTER TABLE MODIFY COLUMN", "ALTER TABLE users MODIFY COLUMN name VARCHAR(200)", StatementDDL, true},
		{"ALTER TABLE ADD INDEX", "ALTER TABLE users ADD INDEX idx_name (name)", StatementDDL, true},
		{"ALTER TABLE RENAME", "ALTER TABLE users RENAME TO customers", StatementDDL, true},

		// DROP TABLE
		{"DROP TABLE", "DROP TABLE users", StatementDDL, true},
		{"DROP TABLE IF EXISTS", "DROP TABLE IF EXISTS users", StatementDDL, true},
		{"DROP multiple tables", "DROP TABLE users, orders, products", StatementDDL, true},

		// CREATE INDEX
		{"CREATE INDEX", "CREATE INDEX idx_name ON users (name)", StatementDDL, true},
		{"CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX idx_email ON users (email)", StatementDDL, true},

		// DROP INDEX
		{"DROP INDEX", "DROP INDEX idx_name ON users", StatementDDL, true},

		// CREATE/DROP DATABASE
		{"CREATE DATABASE", "CREATE DATABASE mydb", StatementCreateDatabase, true},
		{"CREATE DATABASE IF NOT EXISTS", "CREATE DATABASE IF NOT EXISTS mydb", StatementCreateDatabase, true},
		{"DROP DATABASE", "DROP DATABASE mydb", StatementDropDatabase, true},
		{"DROP DATABASE IF EXISTS", "DROP DATABASE IF EXISTS mydb", StatementDropDatabase, true},

		// TRUNCATE
		{"TRUNCATE TABLE", "TRUNCATE TABLE logs", StatementDDL, true},

		// RENAME TABLE
		{"RENAME TABLE", "RENAME TABLE users TO customers", StatementDDL, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
					return
				}
				if ctx.StatementType != tt.wantType {
					t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail, but succeeded")
				}
			}
		})
	}
}

// TestMySQLIndexHints tests USE/FORCE/IGNORE INDEX hints
func TestMySQLIndexHints(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name        string
		sql         string
		shouldParse bool
		limitation  string
	}{
		// USE INDEX - SUPPORTED
		{"USE INDEX", "SELECT * FROM users USE INDEX (idx_name) WHERE name = 'Alice'", true, ""},
		{"USE INDEX multiple", "SELECT * FROM users USE INDEX (idx_name, idx_email) WHERE name = 'Alice'", true, ""},

		// USE INDEX FOR - NOW SUPPORTED
		{
			name:        "USE INDEX FOR JOIN",
			sql:         "SELECT * FROM users USE INDEX FOR JOIN (idx_id) WHERE id > 100",
			shouldParse: true,
		},

		// FORCE INDEX - SUPPORTED
		{"FORCE INDEX", "SELECT * FROM users FORCE INDEX (idx_name) WHERE name LIKE 'A%'", true, ""},
		{
			name:        "FORCE INDEX FOR ORDER BY",
			sql:         "SELECT * FROM users FORCE INDEX FOR ORDER BY (idx_created) ORDER BY created_at",
			shouldParse: true,
		},

		// IGNORE INDEX - SUPPORTED
		{"IGNORE INDEX", "SELECT * FROM users IGNORE INDEX (idx_name) WHERE name = 'Alice'", true, ""},
		{
			name:        "IGNORE INDEX FOR GROUP BY",
			sql:         "SELECT category, COUNT(*) FROM products IGNORE INDEX FOR GROUP BY (idx_cat) GROUP BY category",
			shouldParse: true,
		},

		// Combined - SUPPORTED
		{"USE and IGNORE INDEX", "SELECT * FROM users USE INDEX (idx_name) IGNORE INDEX (idx_email) WHERE name = 'Alice'", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)

			if tt.shouldParse {
				if err != nil {
					t.Errorf("expected parsing to succeed, got error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected parsing to fail (limitation: %s), but succeeded", tt.limitation)
				}
			}
		})
	}
}
