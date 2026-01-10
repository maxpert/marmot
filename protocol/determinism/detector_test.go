package determinism

import (
	"strings"
	"testing"
)

// TestCheckSQL_DeterministicStatements tests that deterministic statements are correctly identified
func TestCheckSQL_DeterministicStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		// INSERT with literals
		{"INSERT with literals", "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"},
		{"INSERT with multiple rows", "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"},
		{"INSERT with NULL", "INSERT INTO users (id, name, email) VALUES (1, 'Alice', NULL)"},
		{"INSERT with numbers", "INSERT INTO products (id, price, qty) VALUES (1, 19.99, 100)"},

		// INSERT with deterministic functions
		{"INSERT with upper()", "INSERT INTO users (id, name) VALUES (1, upper('alice'))"},
		{"INSERT with lower()", "INSERT INTO users (id, name) VALUES (1, lower('ALICE'))"},
		{"INSERT with coalesce()", "INSERT INTO users (id, name) VALUES (1, coalesce('primary', 'default'))"},
		{"INSERT with ifnull()", "INSERT INTO users (id, name) VALUES (1, ifnull('value', 'default'))"},
		{"INSERT with abs()", "INSERT INTO numbers (id, value) VALUES (1, abs(-42))"},
		{"INSERT with length()", "INSERT INTO stats (id, len) VALUES (1, length('hello'))"},
		{"INSERT with substr()", "INSERT INTO users (id, initial) VALUES (1, substr('Alice', 1, 1))"},
		// Note: replace() function not tested - rqlite/sql parser treats REPLACE as keyword
		{"INSERT with trim()", "INSERT INTO users (id, name) VALUES (1, trim('  Alice  '))"},
		{"INSERT with round()", "INSERT INTO prices (id, value) VALUES (1, round(19.999, 2))"},
		{"INSERT with hex()", "INSERT INTO data (id, hex_val) VALUES (1, hex('hello'))"},
		{"INSERT with nested deterministic", "INSERT INTO users (id, name) VALUES (1, upper(trim(lower('  ALICE  '))))"},

		// datetime/date/time with fixed values (NOT 'now')
		{"datetime with fixed string", "INSERT INTO events (id, created) VALUES (1, datetime('2024-01-01'))"},
		{"datetime with fixed and modifier", "INSERT INTO events (id, created) VALUES (1, datetime('2024-01-01', '+1 day'))"},
		{"date with fixed string", "INSERT INTO events (id, day) VALUES (1, date('2024-01-01'))"},
		{"time with fixed string", "INSERT INTO events (id, t) VALUES (1, time('12:30:00'))"},
		{"strftime with fixed", "INSERT INTO events (id, yr) VALUES (1, strftime('%Y', '2024-01-01'))"},

		// UPDATE with literals in SET
		{"UPDATE SET literal", "UPDATE users SET name = 'Bob' WHERE id = 1"},
		{"UPDATE SET multiple literals", "UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1"},
		{"UPDATE SET NULL", "UPDATE users SET email = NULL WHERE id = 1"},
		{"UPDATE SET with deterministic function", "UPDATE users SET name = upper('alice') WHERE id = 1"},

		// DELETE (deterministic as long as WHERE doesn't use non-det functions)
		{"DELETE with literal WHERE", "DELETE FROM users WHERE id = 1"},
		{"DELETE with string literal WHERE", "DELETE FROM users WHERE status = 'inactive'"},
		{"DELETE with IN literal list", "DELETE FROM users WHERE id IN (1, 2, 3)"},
		{"DELETE with deterministic function in WHERE", "DELETE FROM users WHERE name = upper('alice')"},
		{"DELETE LIMIT with ORDER BY", "DELETE FROM users ORDER BY id LIMIT 10"},

		// Arithmetic with literals
		{"arithmetic with literals", "INSERT INTO data (id, val) VALUES (1, 10 + 20 * 3)"},

		// CASE with literals
		{"CASE with literal conditions", "INSERT INTO data (id, category) VALUES (1, CASE WHEN 1 = 1 THEN 'A' ELSE 'B' END)"},

		// JSON functions
		{"json_extract", "INSERT INTO data (id, val) VALUES (1, json_extract('{\"a\":1}', '$.a'))"},
		{"json_object", "INSERT INTO data (id, val) VALUES (1, json_object('a', 1, 'b', 2))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if !result.IsDeterministic {
				t.Errorf("expected deterministic, got non-deterministic: reason=%q, funcs=%v\nSQL: %s",
					result.Reason, result.NonDetFunctions, tt.sql)
			}
		})
	}
}

// TestCheckSQL_NonDeterministicStatements tests that non-deterministic statements are correctly identified
func TestCheckSQL_NonDeterministicStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sql        string
		wantReason string // substring expected in reason
	}{
		// random() and randomblob()
		{"INSERT with random()", "INSERT INTO users (id, token) VALUES (1, random())", "random"},
		{"INSERT with randomblob()", "INSERT INTO data (id, blob) VALUES (1, randomblob(16))", "randomblob"},

		// datetime/date/time with 'now'
		{"datetime('now')", "INSERT INTO events (id, created) VALUES (1, datetime('now'))", "datetime"},
		{"datetime('now', 'localtime')", "INSERT INTO events (id, created) VALUES (1, datetime('now', 'localtime'))", "datetime"},
		{"date('now')", "INSERT INTO events (id, day) VALUES (1, date('now'))", "date"},
		{"time('now')", "INSERT INTO events (id, t) VALUES (1, time('now'))", "time"},
		{"strftime with 'now'", "INSERT INTO events (id, yr) VALUES (1, strftime('%Y', 'now'))", "strftime"},
		{"julianday('now')", "INSERT INTO events (id, jd) VALUES (1, julianday('now'))", "julianday"},

		// datetime() with no args (equivalent to 'now')
		{"datetime() no args", "INSERT INTO events (id, created) VALUES (1, datetime())", "datetime"},

		// Nested with non-deterministic inner function
		{"abs(random())", "INSERT INTO data (id, val) VALUES (1, abs(random()))", "random"},
		{"upper with random", "INSERT INTO data (id, val) VALUES (1, upper(hex(randomblob(4))))", "randomblob"},
		{"coalesce with datetime('now')", "INSERT INTO data (id, val) VALUES (1, coalesce(NULL, datetime('now')))", "datetime"},

		// UPDATE SET with column reference (non-deterministic for statement-based replication)
		{"UPDATE SET self-reference", "UPDATE products SET stock = stock - 1 WHERE id = 100", "column reference"},
		{"UPDATE SET column reference", "UPDATE users SET backup_email = email WHERE id = 1", "column reference"},
		{"UPDATE SET arithmetic on column", "UPDATE counters SET value = value + 1 WHERE id = 1", "column reference"},
		{"UPDATE SET function on column", "UPDATE users SET name = upper(name) WHERE id = 1", "column reference"},
		{"UPDATE SET coalesce with column", "UPDATE users SET name = coalesce(nickname, name, 'Anonymous') WHERE id = 1", "column reference"},

		// UPDATE/DELETE with non-deterministic in WHERE
		{"UPDATE with random() in SET", "UPDATE users SET token = random() WHERE id = 1", "random"},
		{"DELETE with random() in WHERE", "DELETE FROM users WHERE random() < 100", "random"},

		// Subqueries
		{"INSERT with scalar subquery", "INSERT INTO summary (id, total) SELECT 1, sum(amount) FROM orders", "subquery"},
		{"UPDATE with subquery in SET", "UPDATE users SET total = (SELECT sum(amount) FROM orders WHERE user_id = 1) WHERE id = 1", "subquery"},
		{"DELETE with subquery in WHERE", "DELETE FROM users WHERE id IN (SELECT user_id FROM banned_users)", "subquery"},

		// Unknown functions (fail-safe)
		{"unknown function", "INSERT INTO data (id, val) VALUES (1, custom_func('arg'))", "unknown function"},
		{"UDF-like function", "INSERT INTO data (id, val) VALUES (1, my_udf(123))", "unknown function"},

		// CASE with non-deterministic
		{"CASE with random()", "INSERT INTO data (id, status) VALUES (1, CASE WHEN random() > 0 THEN 'a' ELSE 'b' END)", "random"},

		// changes() and last_insert_rowid()
		{"changes()", "INSERT INTO log (id, changed) VALUES (1, changes())", "changes"},
		{"last_insert_rowid()", "INSERT INTO log (id, last_id) VALUES (1, last_insert_rowid())", "last_insert_rowid"},

		// CURRENT_DATE/TIME/TIMESTAMP (parsed as identifiers, not functions)
		{"CURRENT_DATE", "INSERT INTO t (d) VALUES (CURRENT_DATE)", "non-deterministic identifier"},
		{"CURRENT_TIME", "INSERT INTO t (d) VALUES (CURRENT_TIME)", "non-deterministic identifier"},
		{"CURRENT_TIMESTAMP", "INSERT INTO t (d) VALUES (CURRENT_TIMESTAMP)", "non-deterministic identifier"},

		// strftime with no time arg (defaults to 'now')
		{"strftime no time arg", "INSERT INTO t (ts) VALUES (strftime('%s'))", "strftime"},

		// INSERT ... SELECT (contains SELECT = subquery)
		{"INSERT SELECT", "INSERT INTO archive SELECT * FROM users", "subquery"},
		{"INSERT columns SELECT", "INSERT INTO t (a, b) SELECT x, y FROM t2", "subquery"},

		// rowid/oid/_rowid_ in UPDATE SET
		{"rowid in SET", "UPDATE t SET x = rowid WHERE id = 1", "column reference"},
		{"_rowid_ in SET", "UPDATE t SET x = _rowid_ WHERE id = 1", "column reference"},
		{"oid in SET", "UPDATE t SET x = oid WHERE id = 1", "column reference"},

		// DELETE with LIMIT but no ORDER BY (rows affected are undefined)
		{"DELETE LIMIT no ORDER BY", "DELETE FROM t LIMIT 1", "LIMIT but no ORDER BY"},
		{"DELETE WHERE LIMIT no ORDER BY", "DELETE FROM t WHERE x = 1 LIMIT 5", "LIMIT but no ORDER BY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic, got deterministic\nSQL: %s", tt.sql)
				return
			}
			if tt.wantReason != "" && result.Reason == "" {
				t.Errorf("expected reason containing %q, got empty", tt.wantReason)
			}
		})
	}
}

// TestIsDeterministicFunction tests the function whitelist
func TestIsDeterministicFunction(t *testing.T) {
	t.Parallel()

	deterministic := []string{
		"abs", "round", "min", "max", "coalesce", "ifnull", "nullif",
		"length", "lower", "upper", "trim", "ltrim", "rtrim",
		"substr", "replace", "instr", "hex", "quote", "printf",
		"json_extract", "json_object", "json_array",
		"count", "sum", "avg", "total",
	}

	for _, fn := range deterministic {
		t.Run(fn, func(t *testing.T) {
			if !IsDeterministicFunction(fn) {
				t.Errorf("expected %q to be deterministic", fn)
			}
			// Test case insensitivity
			if !IsDeterministicFunction(strings.ToUpper(fn)) {
				t.Errorf("expected %q (uppercase) to be deterministic", strings.ToUpper(fn))
			}
		})
	}

	nonDeterministic := []string{
		"random", "randomblob", "changes", "last_insert_rowid", "total_changes",
		"datetime", "date", "time", "julianday", "strftime", // These need 'now' check
	}

	for _, fn := range nonDeterministic {
		t.Run(fn+"_nondet", func(t *testing.T) {
			if IsDeterministicFunction(fn) {
				t.Errorf("expected %q to be non-deterministic", fn)
			}
		})
	}
}

// TestCheckSQL_EdgeCases tests edge cases
func TestCheckSQL_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		wantDet bool
	}{
		// Empty string values
		{"INSERT with empty string", "INSERT INTO data (id, val) VALUES (1, '')", true},
		{"UPDATE SET empty string", "UPDATE data SET val = '' WHERE id = 1", true},

		// Zero values
		{"INSERT with zero", "INSERT INTO data (id, val) VALUES (1, 0)", true},
		{"INSERT with negative", "INSERT INTO data (id, val) VALUES (1, -100)", true},

		// Column in WHERE only (deterministic - WHERE doesn't affect result values)
		{"UPDATE with column in WHERE", "UPDATE users SET status = 'verified' WHERE email LIKE '%@company.com'", true},
		{"DELETE with column comparison", "DELETE FROM users WHERE email = backup_email", true},

		// Multiple statements (parser handles first one)
		{"simple INSERT", "INSERT INTO t (id) VALUES (1)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic != tt.wantDet {
				t.Errorf("IsDeterministic = %v, want %v, reason: %s\nSQL: %s",
					result.IsDeterministic, tt.wantDet, result.Reason, tt.sql)
			}
		})
	}
}

// TestIsDeterministic tests the convenience function
func TestIsDeterministic(t *testing.T) {
	t.Parallel()

	if !IsDeterministic("INSERT INTO t (id, name) VALUES (1, 'test')") {
		t.Error("expected deterministic for simple INSERT")
	}

	if IsDeterministic("INSERT INTO t (id, ts) VALUES (1, datetime('now'))") {
		t.Error("expected non-deterministic for datetime('now')")
	}

	if IsDeterministic("UPDATE t SET val = val + 1 WHERE id = 1") {
		t.Error("expected non-deterministic for column self-reference")
	}
}

// TestCheckSQL_NonDeterministicFunctions tests all known non-deterministic functions
func TestCheckSQL_NonDeterministicFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sql        string
		wantReason string
	}{
		// random() variants
		{"random() in INSERT", "INSERT INTO t (id, v) VALUES (1, random())", "random"},
		{"random() in UPDATE SET", "UPDATE t SET v = random() WHERE id = 1", "random"},
		{"random() in WHERE", "DELETE FROM t WHERE v = random()", "random"},
		{"random() case insensitive", "INSERT INTO t (id, v) VALUES (1, RANDOM())", "random"},

		// randomblob()
		{"randomblob(16)", "INSERT INTO t (id, blob) VALUES (1, randomblob(16))", "randomblob"},
		{"randomblob(32)", "INSERT INTO t (id, blob) VALUES (1, randomblob(32))", "randomblob"},
		{"randomblob case insensitive", "INSERT INTO t (id, blob) VALUES (1, RANDOMBLOB(8))", "randomblob"},

		// changes()
		{"changes()", "INSERT INTO log (id, n) VALUES (1, changes())", "changes"},
		{"changes() in UPDATE", "UPDATE t SET v = changes() WHERE id = 1", "changes"},
		{"CHANGES() uppercase", "INSERT INTO log (n) VALUES (CHANGES())", "changes"},

		// last_insert_rowid()
		{"last_insert_rowid()", "INSERT INTO t (id, ref) VALUES (1, last_insert_rowid())", "last_insert_rowid"},
		{"LAST_INSERT_ROWID uppercase", "INSERT INTO t (ref) VALUES (LAST_INSERT_ROWID())", "last_insert_rowid"},

		// total_changes()
		{"total_changes()", "INSERT INTO stats (id, total) VALUES (1, total_changes())", "total_changes"},
		{"TOTAL_CHANGES uppercase", "INSERT INTO stats (total) VALUES (TOTAL_CHANGES())", "total_changes"},
		{"total_changes() in UPDATE", "UPDATE stats SET total = total_changes() WHERE id = 1", "total_changes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic, got deterministic\nSQL: %s", tt.sql)
				return
			}
			if !strings.Contains(strings.ToLower(result.Reason), strings.ToLower(tt.wantReason)) {
				t.Errorf("reason %q should contain %q", result.Reason, tt.wantReason)
			}
		})
	}
}

// TestCheckSQL_TimeFunctionsWithNow tests time functions with 'now' argument variations
func TestCheckSQL_TimeFunctionsWithNow(t *testing.T) {
	t.Parallel()

	nonDetTests := []struct {
		name string
		sql  string
	}{
		// datetime() variants
		{"datetime('now')", "INSERT INTO t (ts) VALUES (datetime('now'))"},
		{"datetime('NOW') uppercase", "INSERT INTO t (ts) VALUES (datetime('NOW'))"},
		{"datetime('now', 'localtime')", "INSERT INTO t (ts) VALUES (datetime('now', 'localtime'))"},
		{"datetime('now', '+1 day')", "INSERT INTO t (ts) VALUES (datetime('now', '+1 day'))"},
		{"datetime() no args", "INSERT INTO t (ts) VALUES (datetime())"},
		{"DATETIME('now')", "INSERT INTO t (ts) VALUES (DATETIME('now'))"},

		// date() variants
		{"date('now')", "INSERT INTO t (d) VALUES (date('now'))"},
		{"date('NOW')", "INSERT INTO t (d) VALUES (date('NOW'))"},
		{"date('now', 'start of month')", "INSERT INTO t (d) VALUES (date('now', 'start of month'))"},
		{"date() no args", "INSERT INTO t (d) VALUES (date())"},
		{"DATE('now')", "INSERT INTO t (d) VALUES (DATE('now'))"},

		// time() variants
		{"time('now')", "INSERT INTO t (tm) VALUES (time('now'))"},
		{"time('NOW')", "INSERT INTO t (tm) VALUES (time('NOW'))"},
		{"time('now', '+2 hours')", "INSERT INTO t (tm) VALUES (time('now', '+2 hours'))"},
		{"time() no args", "INSERT INTO t (tm) VALUES (time())"},
		{"TIME('now')", "INSERT INTO t (tm) VALUES (TIME('now'))"},

		// julianday() variants
		{"julianday('now')", "INSERT INTO t (jd) VALUES (julianday('now'))"},
		{"julianday('NOW')", "INSERT INTO t (jd) VALUES (julianday('NOW'))"},
		{"julianday() no args", "INSERT INTO t (jd) VALUES (julianday())"},
		{"JULIANDAY('now')", "INSERT INTO t (jd) VALUES (JULIANDAY('now'))"},

		// strftime() variants
		{"strftime('%Y', 'now')", "INSERT INTO t (yr) VALUES (strftime('%Y', 'now'))"},
		{"strftime('%s', 'now')", "INSERT INTO t (ts) VALUES (strftime('%s', 'now'))"},
		{"strftime('%Y-%m-%d', 'now')", "INSERT INTO t (d) VALUES (strftime('%Y-%m-%d', 'now'))"},
		{"strftime('%s') single arg", "INSERT INTO t (ts) VALUES (strftime('%s'))"},
		{"strftime('%Y') single arg", "INSERT INTO t (yr) VALUES (strftime('%Y'))"},
		{"STRFTIME('%s', 'now')", "INSERT INTO t (ts) VALUES (STRFTIME('%s', 'now'))"},

		// unixepoch() variants
		{"unixepoch('now')", "INSERT INTO t (ts) VALUES (unixepoch('now'))"},
		{"unixepoch() no args", "INSERT INTO t (ts) VALUES (unixepoch())"},
		{"UNIXEPOCH('now')", "INSERT INTO t (ts) VALUES (UNIXEPOCH('now'))"},

		// timediff() variants
		{"timediff('now', '2024-01-01')", "INSERT INTO t (diff) VALUES (timediff('now', '2024-01-01'))"},
		{"timediff() no args", "INSERT INTO t (diff) VALUES (timediff())"},
	}

	for _, tt := range nonDetTests {
		t.Run(tt.name+"_nondet", func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic\nSQL: %s", tt.sql)
			}
		})
	}

	// Deterministic time function calls (fixed timestamps)
	detTests := []struct {
		name string
		sql  string
	}{
		{"datetime('2024-01-01')", "INSERT INTO t (ts) VALUES (datetime('2024-01-01'))"},
		{"datetime('2024-01-01', '+1 day')", "INSERT INTO t (ts) VALUES (datetime('2024-01-01', '+1 day'))"},
		{"date('2024-01-01')", "INSERT INTO t (d) VALUES (date('2024-01-01'))"},
		{"date('2024-01-01', 'start of month')", "INSERT INTO t (d) VALUES (date('2024-01-01', 'start of month'))"},
		{"time('12:30:00')", "INSERT INTO t (tm) VALUES (time('12:30:00'))"},
		{"time('12:30:00', '+1 hour')", "INSERT INTO t (tm) VALUES (time('12:30:00', '+1 hour'))"},
		{"julianday('2024-01-01')", "INSERT INTO t (jd) VALUES (julianday('2024-01-01'))"},
		{"strftime('%Y', '2024-01-01')", "INSERT INTO t (yr) VALUES (strftime('%Y', '2024-01-01'))"},
		{"strftime('%s', '2024-01-01 12:00:00')", "INSERT INTO t (ts) VALUES (strftime('%s', '2024-01-01 12:00:00'))"},
		{"unixepoch('2024-01-01')", "INSERT INTO t (ts) VALUES (unixepoch('2024-01-01'))"},
		{"timediff('2024-01-01', '2023-01-01')", "INSERT INTO t (diff) VALUES (timediff('2024-01-01', '2023-01-01'))"},
	}

	for _, tt := range detTests {
		t.Run(tt.name+"_det", func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if !result.IsDeterministic {
				t.Errorf("expected deterministic, reason=%s\nSQL: %s", result.Reason, tt.sql)
			}
		})
	}
}

// TestCheckSQL_CurrentTimeIdentifiers tests CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP
func TestCheckSQL_CurrentTimeIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		// Basic usage
		{"CURRENT_DATE", "INSERT INTO t (d) VALUES (CURRENT_DATE)"},
		{"CURRENT_TIME", "INSERT INTO t (tm) VALUES (CURRENT_TIME)"},
		{"CURRENT_TIMESTAMP", "INSERT INTO t (ts) VALUES (CURRENT_TIMESTAMP)"},

		// Mixed case
		{"current_date lowercase", "INSERT INTO t (d) VALUES (current_date)"},
		{"current_time lowercase", "INSERT INTO t (tm) VALUES (current_time)"},
		{"current_timestamp lowercase", "INSERT INTO t (ts) VALUES (current_timestamp)"},
		{"Current_Date mixedcase", "INSERT INTO t (d) VALUES (Current_Date)"},

		// In expressions
		{"CURRENT_DATE in expression", "INSERT INTO t (d, label) VALUES (CURRENT_DATE, 'today')"},
		{"CURRENT_TIMESTAMP in UPDATE", "UPDATE t SET ts = CURRENT_TIMESTAMP WHERE id = 1"},
		{"CURRENT_TIME in multiple columns", "INSERT INTO t (tm1, tm2) VALUES (CURRENT_TIME, CURRENT_TIME)"},

		// In WHERE clause
		{"CURRENT_DATE in WHERE", "DELETE FROM t WHERE d < CURRENT_DATE"},
		{"CURRENT_TIMESTAMP in WHERE", "UPDATE t SET status = 'old' WHERE ts < CURRENT_TIMESTAMP"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic\nSQL: %s", tt.sql)
			}
			if result.Reason == "" {
				t.Error("expected non-empty reason")
			}
		})
	}
}

// TestCheckSQL_ColumnReferencesInUpdateSet tests column references in UPDATE SET clauses
func TestCheckSQL_ColumnReferencesInUpdateSet(t *testing.T) {
	t.Parallel()

	nonDetTests := []struct {
		name string
		sql  string
	}{
		// Self-reference arithmetic
		{"x = x + 1", "UPDATE t SET x = x + 1 WHERE id = 1"},
		{"x = x - 1", "UPDATE t SET x = x - 1 WHERE id = 1"},
		{"x = x * 2", "UPDATE t SET x = x * 2 WHERE id = 1"},
		{"x = x / 2", "UPDATE t SET x = x / 2 WHERE id = 1"},
		{"x = x || 'suffix'", "UPDATE t SET x = x || 'suffix' WHERE id = 1"},

		// Cross-column reference
		{"x = y", "UPDATE t SET x = y WHERE id = 1"},
		{"x = y + z", "UPDATE t SET x = y + z WHERE id = 1"},
		{"email_backup = email", "UPDATE users SET email_backup = email WHERE id = 1"},

		// Column in function
		{"x = upper(name)", "UPDATE t SET x = upper(name) WHERE id = 1"},
		{"x = lower(name)", "UPDATE t SET x = lower(name) WHERE id = 1"},
		{"x = length(data)", "UPDATE t SET x = length(data) WHERE id = 1"},
		{"x = coalesce(a, b, c)", "UPDATE t SET x = coalesce(a, b, c) WHERE id = 1"},
		{"x = abs(value)", "UPDATE t SET x = abs(value) WHERE id = 1"},
		{"x = substr(name, 1, 3)", "UPDATE t SET x = substr(name, 1, 3) WHERE id = 1"},

		// rowid variants
		{"x = rowid", "UPDATE t SET x = rowid WHERE id = 1"},
		{"x = _rowid_", "UPDATE t SET x = _rowid_ WHERE id = 1"},
		{"x = oid", "UPDATE t SET x = oid WHERE id = 1"},
		{"x = ROWID", "UPDATE t SET x = ROWID WHERE id = 1"},

		// Qualified column reference
		{"x = t.y", "UPDATE t SET x = t.y WHERE id = 1"},

		// Multiple assignments with column ref
		{"a = b, c = d", "UPDATE t SET a = b, c = d WHERE id = 1"},
		{"counter = counter + 1, updated = 1", "UPDATE t SET counter = counter + 1, updated = 1 WHERE id = 1"},

		// Nested column references
		{"x = upper(lower(name))", "UPDATE t SET x = upper(lower(name)) WHERE id = 1"},
		{"x = coalesce(nullif(a, ''), b)", "UPDATE t SET x = coalesce(nullif(a, ''), b) WHERE id = 1"},
	}

	for _, tt := range nonDetTests {
		t.Run(tt.name+"_nondet", func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic (column reference)\nSQL: %s", tt.sql)
			}
			if !strings.Contains(result.Reason, "column reference") {
				t.Errorf("expected reason about column reference, got: %s", result.Reason)
			}
		})
	}

	// Deterministic UPDATE SET (literals only)
	detTests := []struct {
		name string
		sql  string
	}{
		{"SET literal string", "UPDATE t SET x = 'value' WHERE id = 1"},
		{"SET literal number", "UPDATE t SET x = 42 WHERE id = 1"},
		{"SET literal with expression", "UPDATE t SET x = 10 + 20 WHERE id = 1"},
		{"SET NULL", "UPDATE t SET x = NULL WHERE id = 1"},
		{"SET deterministic function", "UPDATE t SET x = upper('test') WHERE id = 1"},
		{"SET multiple literals", "UPDATE t SET x = 'a', y = 'b', z = 1 WHERE id = 1"},
		{"SET with coalesce on literals", "UPDATE t SET x = coalesce('a', 'b') WHERE id = 1"},
	}

	for _, tt := range detTests {
		t.Run(tt.name+"_det", func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if !result.IsDeterministic {
				t.Errorf("expected deterministic, reason=%s\nSQL: %s", result.Reason, tt.sql)
			}
		})
	}
}

// TestCheckSQL_Subqueries tests various subquery patterns
func TestCheckSQL_Subqueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		// INSERT ... SELECT
		{"INSERT SELECT all", "INSERT INTO archive SELECT * FROM users"},
		{"INSERT SELECT specific columns", "INSERT INTO t (a, b) SELECT x, y FROM src"},
		{"INSERT SELECT with WHERE", "INSERT INTO archive SELECT * FROM users WHERE status = 'inactive'"},
		{"INSERT SELECT with ORDER BY", "INSERT INTO t (a) SELECT x FROM src ORDER BY x"},
		{"INSERT SELECT with LIMIT", "INSERT INTO t (a) SELECT x FROM src LIMIT 10"},
		{"INSERT SELECT with JOIN", "INSERT INTO t (a, b) SELECT u.x, o.y FROM users u JOIN orders o ON u.id = o.user_id"},

		// UPDATE with scalar subquery in SET
		{"UPDATE SET scalar subquery", "UPDATE t SET x = (SELECT max(y) FROM src) WHERE id = 1"},
		{"UPDATE SET scalar subquery with WHERE", "UPDATE users SET total = (SELECT sum(amount) FROM orders WHERE user_id = users.id) WHERE status = 'active'"},
		{"UPDATE SET correlated subquery", "UPDATE t SET x = (SELECT y FROM src WHERE src.id = t.ref_id) WHERE id = 1"},

		// DELETE with subquery in WHERE
		{"DELETE IN subquery", "DELETE FROM users WHERE id IN (SELECT user_id FROM banned)"},
		{"DELETE EXISTS subquery", "DELETE FROM users WHERE EXISTS (SELECT 1 FROM banned WHERE banned.user_id = users.id)"},
		{"DELETE NOT IN subquery", "DELETE FROM users WHERE id NOT IN (SELECT user_id FROM active_users)"},

		// UPDATE with subquery in WHERE
		{"UPDATE WHERE IN subquery", "UPDATE users SET status = 'banned' WHERE id IN (SELECT user_id FROM violations)"},
		{"UPDATE WHERE EXISTS", "UPDATE users SET active = 0 WHERE EXISTS (SELECT 1 FROM deleted WHERE deleted.user_id = users.id)"},

		// Nested subqueries
		{"nested subquery", "INSERT INTO t (a) SELECT x FROM (SELECT y AS x FROM src)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic (subquery)\nSQL: %s", tt.sql)
			}
			if !strings.Contains(result.Reason, "subquery") {
				t.Errorf("expected reason about subquery, got: %s", result.Reason)
			}
		})
	}
}

// TestCheckSQL_DeleteLimitNoOrderBy tests DELETE with LIMIT but no ORDER BY
func TestCheckSQL_DeleteLimitNoOrderBy(t *testing.T) {
	t.Parallel()

	nonDetTests := []struct {
		name string
		sql  string
	}{
		{"DELETE LIMIT 1", "DELETE FROM t LIMIT 1"},
		{"DELETE LIMIT 10", "DELETE FROM t LIMIT 10"},
		{"DELETE WHERE LIMIT", "DELETE FROM t WHERE status = 'old' LIMIT 5"},
		{"DELETE WHERE AND LIMIT", "DELETE FROM t WHERE a = 1 AND b = 2 LIMIT 100"},
	}

	for _, tt := range nonDetTests {
		t.Run(tt.name+"_nondet", func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic\nSQL: %s", tt.sql)
			}
			if !strings.Contains(result.Reason, "LIMIT") {
				t.Errorf("expected reason about LIMIT, got: %s", result.Reason)
			}
		})
	}

	// Deterministic DELETE with LIMIT and ORDER BY
	detTests := []struct {
		name string
		sql  string
	}{
		{"DELETE ORDER BY LIMIT", "DELETE FROM t ORDER BY id LIMIT 10"},
		{"DELETE WHERE ORDER BY LIMIT", "DELETE FROM t WHERE status = 'old' ORDER BY created_at LIMIT 5"},
		{"DELETE ORDER BY DESC LIMIT", "DELETE FROM t ORDER BY id DESC LIMIT 1"},
		{"DELETE multiple ORDER BY LIMIT", "DELETE FROM t ORDER BY a, b LIMIT 10"},
		{"DELETE without LIMIT", "DELETE FROM t WHERE id = 1"},
		{"DELETE all", "DELETE FROM t"},
	}

	for _, tt := range detTests {
		t.Run(tt.name+"_det", func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if !result.IsDeterministic {
				t.Errorf("expected deterministic, reason=%s\nSQL: %s", result.Reason, tt.sql)
			}
		})
	}
}

// TestCheckSQL_UnknownFunctions tests that unknown functions are flagged as non-deterministic
func TestCheckSQL_UnknownFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sql      string
		funcName string
	}{
		{"custom_func", "INSERT INTO t (v) VALUES (custom_func())", "custom_func"},
		{"my_udf", "INSERT INTO t (v) VALUES (my_udf(1, 2))", "my_udf"},
		{"app_specific", "INSERT INTO t (v) VALUES (app_specific('arg'))", "app_specific"},
		{"calculate", "UPDATE t SET v = calculate(x, y) WHERE id = 1", "calculate"},
		{"process", "INSERT INTO t (v) VALUES (process('data'))", "process"},
		{"unknown123", "INSERT INTO t (v) VALUES (unknown123())", "unknown123"},

		// SQLite extension functions that aren't in whitelist
		{"compress", "INSERT INTO t (v) VALUES (compress('data'))", "compress"},
		{"decompress", "INSERT INTO t (v) VALUES (decompress(blob))", "decompress"},

		// Functions with similar names to known ones
		{"random2", "INSERT INTO t (v) VALUES (random2())", "random2"},
		{"my_random", "INSERT INTO t (v) VALUES (my_random())", "my_random"},
		{"abs2", "INSERT INTO t (v) VALUES (abs2(-1))", "abs2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic (unknown function)\nSQL: %s", tt.sql)
			}
			if !strings.Contains(result.Reason, "unknown function") {
				t.Errorf("expected reason about unknown function, got: %s", result.Reason)
			}
		})
	}
}

// TestCheckSQL_NestedNonDeterministic tests nested function calls with non-deterministic inner functions
func TestCheckSQL_NestedNonDeterministic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sql        string
		wantReason string
	}{
		// random() nested
		{"abs(random())", "INSERT INTO t (v) VALUES (abs(random()))", "random"},
		{"round(random())", "INSERT INTO t (v) VALUES (round(random()))", "random"},
		{"hex(randomblob(4))", "INSERT INTO t (v) VALUES (hex(randomblob(4)))", "randomblob"},
		{"upper(hex(randomblob(8)))", "INSERT INTO t (v) VALUES (upper(hex(randomblob(8))))", "randomblob"},
		{"length(hex(random()))", "INSERT INTO t (v) VALUES (length(hex(random())))", "random"},
		{"substr(hex(random()), 1, 4)", "INSERT INTO t (v) VALUES (substr(hex(random()), 1, 4))", "random"},

		// datetime('now') nested
		{"upper(datetime('now'))", "INSERT INTO t (v) VALUES (upper(datetime('now')))", "datetime"},
		{"length(date('now'))", "INSERT INTO t (v) VALUES (length(date('now')))", "date"},
		{"substr(time('now'), 1, 2)", "INSERT INTO t (v) VALUES (substr(time('now'), 1, 2))", "time"},
		{"coalesce(NULL, datetime('now'))", "INSERT INTO t (v) VALUES (coalesce(NULL, datetime('now')))", "datetime"},
		{"ifnull(NULL, date('now'))", "INSERT INTO t (v) VALUES (ifnull(NULL, date('now')))", "date"},

		// Multiple levels of nesting
		{"upper(lower(hex(random())))", "INSERT INTO t (v) VALUES (upper(lower(hex(random()))))", "random"},
		{"substr(upper(datetime('now')), 1, 10)", "INSERT INTO t (v) VALUES (substr(upper(datetime('now')), 1, 10))", "datetime"},
		{"coalesce(ifnull(NULL, random()), 0)", "INSERT INTO t (v) VALUES (coalesce(ifnull(NULL, random()), 0))", "random"},

		// Non-deterministic in CASE
		{"CASE with random()", "INSERT INTO t (v) VALUES (CASE WHEN random() > 0 THEN 1 ELSE 0 END)", "random"},
		{"CASE result random()", "INSERT INTO t (v) VALUES (CASE WHEN 1=1 THEN random() ELSE 0 END)", "random"},

		// In coalesce/ifnull
		{"coalesce with random", "INSERT INTO t (v) VALUES (coalesce(random(), 0))", "random"},
		{"ifnull with datetime", "INSERT INTO t (v) VALUES (ifnull(NULL, datetime('now')))", "datetime"},

		// iif with non-deterministic
		{"iif with random", "INSERT INTO t (v) VALUES (iif(random() > 0, 'a', 'b'))", "random"},

		// CURRENT_* nested
		{"upper(CURRENT_DATE)", "INSERT INTO t (v) VALUES (upper(CURRENT_DATE))", ""},
		{"coalesce(NULL, CURRENT_TIMESTAMP)", "INSERT INTO t (v) VALUES (coalesce(NULL, CURRENT_TIMESTAMP))", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic\nSQL: %s", tt.sql)
			}
		})
	}
}

// TestCheckSQL_MixedDeterministicNonDeterministic tests statements with both det and non-det functions
func TestCheckSQL_MixedDeterministicNonDeterministic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		// Multiple columns, one non-deterministic
		{"one column non-det", "INSERT INTO t (a, b, c) VALUES (1, 'test', random())"},
		{"first column non-det", "INSERT INTO t (a, b, c) VALUES (random(), 'test', 123)"},
		{"middle column non-det", "INSERT INTO t (a, b, c) VALUES (1, datetime('now'), 123)"},

		// Deterministic and non-deterministic in same expression
		{"det + non-det arithmetic", "INSERT INTO t (v) VALUES (10 + random())"},
		{"det || non-det concat", "INSERT INTO t (v) VALUES ('prefix_' || hex(random()))"},

		// Multiple non-deterministic in one statement
		{"multiple random()", "INSERT INTO t (a, b) VALUES (random(), random())"},
		{"random and datetime", "INSERT INTO t (a, b) VALUES (random(), datetime('now'))"},
		{"three non-det", "INSERT INTO t (a, b, c) VALUES (random(), datetime('now'), last_insert_rowid())"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if result.IsDeterministic {
				t.Errorf("expected non-deterministic\nSQL: %s", tt.sql)
			}
		})
	}
}

// TestCheckSQL_ParseErrors tests handling of invalid SQL
func TestCheckSQL_ParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		{"incomplete INSERT", "INSERT INTO"},
		{"invalid syntax", "INSERT INTO t VALUES (("},
		{"empty string", ""},
		{"garbage", "asdfghjkl"},
		{"missing VALUES", "INSERT INTO t (a)"},
		{"unclosed string", "INSERT INTO t (a) VALUES ('unclosed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			// Parse errors should be non-deterministic (fail-safe)
			if result.IsDeterministic {
				t.Errorf("parse error should result in non-deterministic\nSQL: %s", tt.sql)
			}
			if !strings.Contains(result.Reason, "parse error") {
				t.Errorf("expected parse error reason, got: %s", result.Reason)
			}
		})
	}
}

// TestCheckSQL_DeterministicFunctionVariants tests various deterministic function patterns
func TestCheckSQL_DeterministicFunctionVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		// Math functions
		{"abs(-42)", "INSERT INTO t (v) VALUES (abs(-42))"},
		{"round(3.14159, 2)", "INSERT INTO t (v) VALUES (round(3.14159, 2))"},
		{"min(1, 2, 3)", "INSERT INTO t (v) VALUES (min(1, 2, 3))"},
		{"max(1, 2, 3)", "INSERT INTO t (v) VALUES (max(1, 2, 3))"},
		{"sign(-5)", "INSERT INTO t (v) VALUES (sign(-5))"},

		// String functions
		{"upper('hello')", "INSERT INTO t (v) VALUES (upper('hello'))"},
		{"lower('HELLO')", "INSERT INTO t (v) VALUES (lower('HELLO'))"},
		{"trim('  hello  ')", "INSERT INTO t (v) VALUES (trim('  hello  '))"},
		{"ltrim('  hello')", "INSERT INTO t (v) VALUES (ltrim('  hello'))"},
		{"rtrim('hello  ')", "INSERT INTO t (v) VALUES (rtrim('hello  '))"},
		{"length('hello')", "INSERT INTO t (v) VALUES (length('hello'))"},
		{"substr('hello', 2, 3)", "INSERT INTO t (v) VALUES (substr('hello', 2, 3))"},
		{"substring('hello', 2, 3)", "INSERT INTO t (v) VALUES (substring('hello', 2, 3))"},
		// Note: replace() not tested - rqlite/sql parser treats REPLACE as keyword
		{"instr('hello', 'l')", "INSERT INTO t (v) VALUES (instr('hello', 'l'))"},
		{"hex('hello')", "INSERT INTO t (v) VALUES (hex('hello'))"},
		{"quote('hello')", "INSERT INTO t (v) VALUES (quote('hello'))"},
		{"printf('%d', 42)", "INSERT INTO t (v) VALUES (printf('%d', 42))"},
		{"format('%s', 'test')", "INSERT INTO t (v) VALUES (format('%s', 'test'))"},

		// Null handling
		{"coalesce(NULL, 'default')", "INSERT INTO t (v) VALUES (coalesce(NULL, 'default'))"},
		{"ifnull(NULL, 'default')", "INSERT INTO t (v) VALUES (ifnull(NULL, 'default'))"},
		{"nullif('a', 'b')", "INSERT INTO t (v) VALUES (nullif('a', 'b'))"},
		{"iif(1=1, 'yes', 'no')", "INSERT INTO t (v) VALUES (iif(1=1, 'yes', 'no'))"},

		// JSON functions
		{"json_extract('{\"a\":1}', '$.a')", "INSERT INTO t (v) VALUES (json_extract('{\"a\":1}', '$.a'))"},
		{"json_object('a', 1)", "INSERT INTO t (v) VALUES (json_object('a', 1))"},
		{"json_array(1, 2, 3)", "INSERT INTO t (v) VALUES (json_array(1, 2, 3))"},
		{"json_valid('{}')", "INSERT INTO t (v) VALUES (json_valid('{}'))"},

		// Type functions
		{"typeof(42)", "INSERT INTO t (v) VALUES (typeof(42))"},
		{"zeroblob(10)", "INSERT INTO t (v) VALUES (zeroblob(10))"},

		// Nested deterministic
		{"upper(trim(lower('  HELLO  ')))", "INSERT INTO t (v) VALUES (upper(trim(lower('  HELLO  '))))"},
		{"coalesce(nullif('', ''), 'default')", "INSERT INTO t (v) VALUES (coalesce(nullif('', ''), 'default'))"},
		{"abs(min(-1, -2, -3))", "INSERT INTO t (v) VALUES (abs(min(-1, -2, -3)))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if !result.IsDeterministic {
				t.Errorf("expected deterministic, reason=%s\nSQL: %s", result.Reason, tt.sql)
			}
		})
	}
}

// TestCheckSQL_WhereClauseDoesNotAffectDeterminism tests that WHERE clause columns don't affect determinism
func TestCheckSQL_WhereClauseDoesNotAffectDeterminism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		// UPDATE with column in WHERE only
		{"UPDATE column in WHERE", "UPDATE t SET x = 'value' WHERE y = 1"},
		{"UPDATE column comparison in WHERE", "UPDATE t SET x = 'value' WHERE a = b"},
		{"UPDATE column LIKE in WHERE", "UPDATE t SET status = 'new' WHERE name LIKE 'test%'"},
		{"UPDATE column function in WHERE", "UPDATE t SET status = 'lower' WHERE upper(name) = 'TEST'"},

		// DELETE with column in WHERE
		{"DELETE column in WHERE", "DELETE FROM t WHERE id = 1"},
		{"DELETE column comparison", "DELETE FROM t WHERE a = b"},
		{"DELETE column BETWEEN", "DELETE FROM t WHERE x BETWEEN y AND z"},
		{"DELETE column IN list", "DELETE FROM t WHERE status IN ('a', 'b')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQL(tt.sql)
			if !result.IsDeterministic {
				t.Errorf("expected deterministic (column in WHERE should be OK), reason=%s\nSQL: %s",
					result.Reason, tt.sql)
			}
		})
	}
}

// TestResult_Fields tests the Result struct fields
func TestResult_Fields(t *testing.T) {
	t.Parallel()

	t.Run("deterministic result", func(t *testing.T) {
		result := CheckSQL("INSERT INTO t (id) VALUES (1)")
		if !result.IsDeterministic {
			t.Error("expected IsDeterministic=true")
		}
		if result.Reason != "" {
			t.Errorf("expected empty reason, got: %s", result.Reason)
		}
		if len(result.NonDetFunctions) != 0 {
			t.Errorf("expected empty NonDetFunctions, got: %v", result.NonDetFunctions)
		}
	})

	t.Run("non-deterministic with single function", func(t *testing.T) {
		result := CheckSQL("INSERT INTO t (id) VALUES (random())")
		if result.IsDeterministic {
			t.Error("expected IsDeterministic=false")
		}
		if result.Reason == "" {
			t.Error("expected non-empty reason")
		}
		if len(result.NonDetFunctions) == 0 {
			t.Error("expected NonDetFunctions to contain the function")
		}
	})

	t.Run("non-deterministic with multiple functions", func(t *testing.T) {
		result := CheckSQL("INSERT INTO t (a, b) VALUES (random(), datetime('now'))")
		if result.IsDeterministic {
			t.Error("expected IsDeterministic=false")
		}
		if len(result.NonDetFunctions) < 2 {
			t.Errorf("expected at least 2 NonDetFunctions, got: %v", result.NonDetFunctions)
		}
	})
}

// BenchmarkCheckSQL benchmarks the determinism checker
func BenchmarkCheckSQL(b *testing.B) {
	sqls := []string{
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
		"INSERT INTO users (id, created) VALUES (1, datetime('now'))",
		"UPDATE users SET name = upper('test') WHERE id = 1",
		"UPDATE users SET counter = counter + 1 WHERE id = 1",
		"DELETE FROM users WHERE id IN (1, 2, 3)",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			CheckSQL(sql)
		}
	}
}

// BenchmarkCheckSQL_Complex benchmarks complex SQL statements
func BenchmarkCheckSQL_Complex(b *testing.B) {
	sqls := []string{
		"INSERT INTO t (a, b, c, d, e) VALUES (upper(lower(trim('  test  '))), coalesce(NULL, 'default'), abs(round(-3.14, 2)), hex('data'), json_object('key', 'value'))",
		"UPDATE t SET a = upper('x'), b = lower('Y'), c = 'literal', d = 42 WHERE id = 1 AND status = 'active' AND created < '2024-01-01'",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range sqls {
			CheckSQL(sql)
		}
	}
}
