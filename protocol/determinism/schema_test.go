package determinism

import (
	"strings"
	"testing"

	"github.com/rqlite/sql"
)

func TestSchema_AddTable(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	err := schema.AddTable(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		status TEXT DEFAULT 'active'
	)`)
	if err != nil {
		t.Fatalf("AddTable failed: %v", err)
	}

	table, ok := schema.Tables["users"]
	if !ok {
		t.Fatal("users table not found")
	}

	// Check created_at has non-det DEFAULT
	createdAt := table.Columns["created_at"]
	if !createdAt.HasDefault {
		t.Error("created_at should have default")
	}
	if createdAt.DefaultIsDeterministic {
		t.Error("created_at DEFAULT CURRENT_TIMESTAMP should be non-deterministic")
	}

	// Check status has deterministic DEFAULT
	status := table.Columns["status"]
	if !status.HasDefault {
		t.Error("status should have default")
	}
	if !status.DefaultIsDeterministic {
		t.Error("status DEFAULT 'active' should be deterministic")
	}
}

func TestSchema_AddTrigger(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE audit_log (id INTEGER, msg TEXT)`)
	schema.AddTable(`CREATE TABLE audit_backup (id INTEGER, msg TEXT)`)

	// Add trigger with non-deterministic DML body
	err := schema.AddTrigger(`CREATE TRIGGER log_changes AFTER INSERT ON audit_log
		BEGIN
			INSERT INTO audit_backup (id, msg) VALUES (random(), 'backup');
		END`)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	table := schema.Tables["audit_log"]
	if !table.HasTrigger {
		t.Error("audit_log should have trigger marked")
	}
}

func TestCheckSQLWithSchema_NonDetDefault(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		name TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	tests := []struct {
		name    string
		sql     string
		wantDet bool
	}{
		{
			name:    "INSERT missing non-det DEFAULT column",
			sql:     "INSERT INTO events (id, name) VALUES (1, 'test')",
			wantDet: false,
		},
		{
			name:    "INSERT all columns specified",
			sql:     "INSERT INTO events (id, name, created_at) VALUES (1, 'test', '2024-01-01')",
			wantDet: true,
		},
		{
			name:    "INSERT missing only det columns",
			sql:     "INSERT INTO events (id, created_at) VALUES (1, '2024-01-01')",
			wantDet: true, // name has no DEFAULT, will be NULL
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQLWithSchema(tt.sql, schema)
			if result.IsDeterministic != tt.wantDet {
				t.Errorf("got IsDeterministic=%v, want %v, reason=%s",
					result.IsDeterministic, tt.wantDet, result.Reason)
			}
		})
	}
}

func TestCheckSQLWithSchema_Trigger(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE tracked (id INTEGER, data TEXT)`)
	schema.AddTable(`CREATE TABLE tracked_log (ts TEXT)`)
	schema.AddTrigger(`CREATE TRIGGER track_insert AFTER INSERT ON tracked
		BEGIN
			INSERT INTO tracked_log (ts) VALUES (datetime('now'));
		END`)

	tests := []struct {
		name    string
		sql     string
		wantDet bool
	}{
		{
			name:    "INSERT to table with non-det trigger",
			sql:     "INSERT INTO tracked (id, data) VALUES (1, 'test')",
			wantDet: false,
		},
		{
			name:    "UPDATE table with non-det trigger",
			sql:     "UPDATE tracked SET data = 'new' WHERE id = 1",
			wantDet: false,
		},
		{
			name:    "DELETE from table with non-det trigger",
			sql:     "DELETE FROM tracked WHERE id = 1",
			wantDet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQLWithSchema(tt.sql, schema)
			if result.IsDeterministic != tt.wantDet {
				t.Errorf("got IsDeterministic=%v, want %v, reason=%s",
					result.IsDeterministic, tt.wantDet, result.Reason)
			}
		})
	}
}

func TestCheckSQLWithSchema_DetTrigger(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE simple (id INTEGER, data TEXT)`)
	schema.AddTable(`CREATE TABLE simple_log (id INTEGER, data TEXT)`)
	// Deterministic trigger - DML with literals only
	schema.AddTrigger(`CREATE TRIGGER simple_trigger AFTER INSERT ON simple
		BEGIN
			INSERT INTO simple_log (id, data) VALUES (1, 'logged');
		END`)

	result := CheckSQLWithSchema("INSERT INTO simple (id, data) VALUES (1, 'test')", schema)
	if !result.IsDeterministic {
		t.Errorf("deterministic trigger should not flag table, reason=%s", result.Reason)
	}
}

func TestCheckSQLWithSchema_UnknownTable(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	// Empty schema - unknown tables should pass through

	result := CheckSQLWithSchema("INSERT INTO unknown (id) VALUES (1)", schema)
	if !result.IsDeterministic {
		t.Errorf("unknown table should be deterministic, reason=%s", result.Reason)
	}
}

func TestCheckSQLWithSchema_NilSchema(t *testing.T) {
	t.Parallel()

	// Nil schema should just do basic check
	result := CheckSQLWithSchema("INSERT INTO t (id) VALUES (1)", nil)
	if !result.IsDeterministic {
		t.Error("nil schema should pass through basic check")
	}

	result = CheckSQLWithSchema("INSERT INTO t (id) VALUES (random())", nil)
	if result.IsDeterministic {
		t.Error("random() should still be detected with nil schema")
	}
}

// TestSchema_AllCurrentTimeDefaults tests all current time DEFAULT variants
func TestSchema_AllCurrentTimeDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSQL  string
		colName    string
		wantNonDet bool
	}{
		{"CURRENT_TIMESTAMP", `CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP)`, "c", true},
		{"CURRENT_DATE", `CREATE TABLE t (c DATE DEFAULT CURRENT_DATE)`, "c", true},
		{"CURRENT_TIME", `CREATE TABLE t (c TIME DEFAULT CURRENT_TIME)`, "c", true},
		{"literal string", `CREATE TABLE t (c TEXT DEFAULT 'hello')`, "c", false},
		{"literal number", `CREATE TABLE t (c INTEGER DEFAULT 42)`, "c", false},
		{"no default", `CREATE TABLE t (c TEXT)`, "c", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema()
			err := schema.AddTable(tt.createSQL)
			if err != nil {
				t.Fatalf("AddTable failed: %v", err)
			}

			col := schema.Tables["t"].Columns[tt.colName]
			if tt.wantNonDet {
				if !col.HasDefault || col.DefaultIsDeterministic {
					t.Errorf("expected non-deterministic DEFAULT, got HasDefault=%v, IsDet=%v",
						col.HasDefault, col.DefaultIsDeterministic)
				}
			} else if col.HasDefault && !col.DefaultIsDeterministic {
				t.Errorf("expected deterministic DEFAULT, got non-deterministic")
			}
		})
	}
}

// TestSchema_TriggerVariants tests different trigger body contents
func TestSchema_TriggerVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		triggerSQL     string
		wantHasTrigger bool
	}{
		{
			name: "random() in INSERT",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id) VALUES (random()); END`,
			wantHasTrigger: true,
		},
		{
			name: "datetime('now') in INSERT",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id) VALUES (datetime('now')); END`,
			wantHasTrigger: true,
		},
		{
			name: "CURRENT_TIMESTAMP in INSERT",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id) VALUES (CURRENT_TIMESTAMP); END`,
			wantHasTrigger: true,
		},
		{
			name: "literal only INSERT",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id) VALUES (1); END`,
			wantHasTrigger: false,
		},
		{
			name: "UPDATE with column ref",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN UPDATE dst SET cnt = cnt + 1 WHERE id = 1; END`,
			wantHasTrigger: true, // column ref in SET is non-det
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema()
			schema.AddTable(`CREATE TABLE src (id INTEGER)`)
			schema.AddTable(`CREATE TABLE dst (id INTEGER, cnt INTEGER)`)
			err := schema.AddTrigger(tt.triggerSQL)
			if err != nil {
				t.Fatalf("AddTrigger failed: %v", err)
			}

			table := schema.Tables["src"]
			if table.HasTrigger != tt.wantHasTrigger {
				t.Errorf("HasTrigger=%v, want %v", table.HasTrigger, tt.wantHasTrigger)
			}
		})
	}
}

// TestCheckSQLWithSchema_CombinedDefaultAndTrigger tests table with both non-det DEFAULT and trigger
func TestCheckSQLWithSchema_CombinedDefaultAndTrigger(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE combined (
		id INTEGER PRIMARY KEY,
		created DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	schema.AddTable(`CREATE TABLE combined_log (msg TEXT)`)
	schema.AddTrigger(`CREATE TRIGGER combined_trigger AFTER INSERT ON combined
		BEGIN INSERT INTO combined_log (msg) VALUES (random()); END`)

	// Should fail for both reasons (trigger takes precedence in current impl)
	result := CheckSQLWithSchema("INSERT INTO combined (id) VALUES (1)", schema)
	if result.IsDeterministic {
		t.Errorf("should be non-deterministic, got deterministic")
	}
}

// TestCheckSQLWithSchema_UpdateDeleteIgnoreDefault tests that UPDATE/DELETE don't care about DEFAULT
func TestCheckSQLWithSchema_UpdateDeleteIgnoreDefault(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		created DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	// UPDATE doesn't use DEFAULT values
	result := CheckSQLWithSchema("UPDATE t SET id = 2 WHERE id = 1", schema)
	if !result.IsDeterministic {
		t.Errorf("UPDATE should ignore DEFAULT, reason=%s", result.Reason)
	}

	// DELETE doesn't use DEFAULT values
	result = CheckSQLWithSchema("DELETE FROM t WHERE id = 1", schema)
	if !result.IsDeterministic {
		t.Errorf("DELETE should ignore DEFAULT, reason=%s", result.Reason)
	}
}

// TestSchema_DefaultExpressionVariants tests various DEFAULT expression types
func TestSchema_DefaultExpressionVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createSQL  string
		colName    string
		wantNonDet bool
	}{
		// Non-deterministic DEFAULTs
		{"CURRENT_TIMESTAMP", `CREATE TABLE t (c DATETIME DEFAULT CURRENT_TIMESTAMP)`, "c", true},
		{"CURRENT_DATE", `CREATE TABLE t (c DATE DEFAULT CURRENT_DATE)`, "c", true},
		{"CURRENT_TIME", `CREATE TABLE t (c TIME DEFAULT CURRENT_TIME)`, "c", true},
		{"datetime('now')", `CREATE TABLE t (c DATETIME DEFAULT (datetime('now')))`, "c", true},
		{"date('now')", `CREATE TABLE t (c DATE DEFAULT (date('now')))`, "c", true},
		{"time('now')", `CREATE TABLE t (c TIME DEFAULT (time('now')))`, "c", true},
		{"random()", `CREATE TABLE t (c INTEGER DEFAULT (random()))`, "c", true},
		{"strftime with now", `CREATE TABLE t (c TEXT DEFAULT (strftime('%s', 'now')))`, "c", true},
		{"julianday('now')", `CREATE TABLE t (c REAL DEFAULT (julianday('now')))`, "c", true},

		// Deterministic DEFAULTs
		{"literal string", `CREATE TABLE t (c TEXT DEFAULT 'hello')`, "c", false},
		{"literal number", `CREATE TABLE t (c INTEGER DEFAULT 42)`, "c", false},
		{"literal float", `CREATE TABLE t (c REAL DEFAULT 3.14)`, "c", false},
		{"literal negative", `CREATE TABLE t (c INTEGER DEFAULT -1)`, "c", false},
		{"NULL default", `CREATE TABLE t (c TEXT DEFAULT NULL)`, "c", false},
		{"no default", `CREATE TABLE t (c TEXT)`, "c", false},
		{"expression with literals", `CREATE TABLE t (c INTEGER DEFAULT (10 + 20))`, "c", false},
		{"upper() on literal", `CREATE TABLE t (c TEXT DEFAULT (upper('test')))`, "c", false},
		{"coalesce on literals", `CREATE TABLE t (c TEXT DEFAULT (coalesce(NULL, 'default')))`, "c", false},
		{"abs on literal", `CREATE TABLE t (c INTEGER DEFAULT (abs(-42)))`, "c", false},
		{"datetime fixed", `CREATE TABLE t (c DATETIME DEFAULT (datetime('2024-01-01')))`, "c", false},
		{"strftime fixed", `CREATE TABLE t (c TEXT DEFAULT (strftime('%Y', '2024-01-01')))`, "c", false},

		// Unknown function in DEFAULT (fail-safe to non-det)
		{"unknown function", `CREATE TABLE t (c TEXT DEFAULT (custom_func()))`, "c", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema()
			err := schema.AddTable(tt.createSQL)
			if err != nil {
				t.Fatalf("AddTable failed: %v", err)
			}

			col := schema.Tables["t"].Columns[tt.colName]
			if tt.wantNonDet {
				if !col.HasDefault || col.DefaultIsDeterministic {
					t.Errorf("expected non-deterministic DEFAULT, got HasDefault=%v, IsDet=%v",
						col.HasDefault, col.DefaultIsDeterministic)
				}
			} else if col.HasDefault && !col.DefaultIsDeterministic {
				t.Errorf("expected deterministic DEFAULT, got non-deterministic")
			}
		})
	}
}

// TestSchema_TriggerBodyVariants tests various trigger body patterns
func TestSchema_TriggerBodyVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		triggerSQL     string
		wantHasTrigger bool
	}{
		// Non-deterministic triggers
		{
			name: "INSERT with random()",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id) VALUES (random()); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with datetime('now')",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with date('now')",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (d) VALUES (date('now')); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with time('now')",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (tm) VALUES (time('now')); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with CURRENT_TIMESTAMP",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (ts) VALUES (CURRENT_TIMESTAMP); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with CURRENT_DATE",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (d) VALUES (CURRENT_DATE); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with CURRENT_TIME",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (tm) VALUES (CURRENT_TIME); END`,
			wantHasTrigger: true,
		},
		{
			name: "UPDATE with column reference",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN UPDATE dst SET cnt = cnt + 1 WHERE id = 1; END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with last_insert_rowid()",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (ref) VALUES (last_insert_rowid()); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with changes()",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (n) VALUES (changes()); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT with randomblob()",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (data) VALUES (randomblob(16)); END`,
			wantHasTrigger: true,
		},
		{
			name: "nested non-det in INSERT",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (h) VALUES (hex(random())); END`,
			wantHasTrigger: true,
		},
		{
			name: "INSERT SELECT (subquery)",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst SELECT * FROM other; END`,
			wantHasTrigger: true,
		},

		// Deterministic triggers
		{
			name: "INSERT with literals only",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id, data) VALUES (1, 'logged'); END`,
			wantHasTrigger: false,
		},
		{
			name: "INSERT with deterministic function",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (id, data) VALUES (1, upper('test')); END`,
			wantHasTrigger: false,
		},
		{
			name: "UPDATE with literal SET",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN UPDATE dst SET data = 'updated' WHERE id = 1; END`,
			wantHasTrigger: false,
		},
		{
			name: "DELETE with WHERE",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN DELETE FROM dst WHERE id = 1; END`,
			wantHasTrigger: false,
		},
		{
			name: "INSERT with fixed datetime",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('2024-01-01')); END`,
			wantHasTrigger: false,
		},
		{
			name: "INSERT with multiple literals",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (a, b, c) VALUES (1, 'two', 3.0); END`,
			wantHasTrigger: false,
		},
		{
			name: "INSERT with nested deterministic",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (s) VALUES (upper(lower('TEST'))); END`,
			wantHasTrigger: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema()
			schema.AddTable(`CREATE TABLE src (id INTEGER)`)
			schema.AddTable(`CREATE TABLE dst (id INTEGER, data TEXT, ts TEXT, d TEXT, tm TEXT, cnt INTEGER, ref INTEGER, n INTEGER, h TEXT, a INTEGER, b TEXT, c REAL, s TEXT)`)
			err := schema.AddTrigger(tt.triggerSQL)
			if err != nil {
				t.Fatalf("AddTrigger failed: %v", err)
			}

			table := schema.Tables["src"]
			if table.HasTrigger != tt.wantHasTrigger {
				t.Errorf("HasTrigger=%v, want %v", table.HasTrigger, tt.wantHasTrigger)
			}
		})
	}
}

// TestSchema_MultipleTriggerTypes tests different trigger event types
func TestSchema_MultipleTriggerTypes(t *testing.T) {
	t.Parallel()

	triggerTypes := []struct {
		name       string
		triggerSQL string
	}{
		{
			name: "BEFORE INSERT",
			triggerSQL: `CREATE TRIGGER t BEFORE INSERT ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
		{
			name: "AFTER INSERT",
			triggerSQL: `CREATE TRIGGER t AFTER INSERT ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
		{
			name: "BEFORE UPDATE",
			triggerSQL: `CREATE TRIGGER t BEFORE UPDATE ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
		{
			name: "AFTER UPDATE",
			triggerSQL: `CREATE TRIGGER t AFTER UPDATE ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
		{
			name: "BEFORE DELETE",
			triggerSQL: `CREATE TRIGGER t BEFORE DELETE ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
		{
			name: "AFTER DELETE",
			triggerSQL: `CREATE TRIGGER t AFTER DELETE ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
		{
			name: "INSTEAD OF INSERT (view trigger)",
			triggerSQL: `CREATE TRIGGER t INSTEAD OF INSERT ON src
				BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`,
		},
	}

	for _, tt := range triggerTypes {
		t.Run(tt.name, func(t *testing.T) {
			schema := NewSchema()
			schema.AddTable(`CREATE TABLE src (id INTEGER)`)
			schema.AddTable(`CREATE TABLE dst (ts TEXT)`)
			err := schema.AddTrigger(tt.triggerSQL)
			if err != nil {
				t.Fatalf("AddTrigger failed: %v", err)
			}

			table := schema.Tables["src"]
			if !table.HasTrigger {
				t.Errorf("expected HasTrigger=true for %s", tt.name)
			}
		})
	}
}

// TestSchema_DropTable tests the DropTable functionality
func TestSchema_DropTable(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	schema.AddTable(`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)`)

	if _, ok := schema.Tables["users"]; !ok {
		t.Fatal("users table should exist")
	}
	if _, ok := schema.Tables["orders"]; !ok {
		t.Fatal("orders table should exist")
	}

	schema.DropTable("users")

	if _, ok := schema.Tables["users"]; ok {
		t.Error("users table should have been dropped")
	}
	if _, ok := schema.Tables["orders"]; !ok {
		t.Error("orders table should still exist")
	}

	// Drop non-existent table should not panic
	schema.DropTable("nonexistent")
}

// TestSchema_CaseInsensitivity tests that table/column names are case insensitive
func TestSchema_CaseInsensitivity(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE Users (ID INTEGER PRIMARY KEY, Name TEXT DEFAULT 'default')`)

	// Should be stored lowercase
	if _, ok := schema.Tables["users"]; !ok {
		t.Error("table should be stored as lowercase")
	}
	if _, ok := schema.Tables["Users"]; ok {
		t.Error("table should not be stored with original case")
	}

	table := schema.Tables["users"]
	if _, ok := table.Columns["id"]; !ok {
		t.Error("column ID should be stored as lowercase 'id'")
	}
	if _, ok := table.Columns["name"]; !ok {
		t.Error("column Name should be stored as lowercase 'name'")
	}
}

// TestCheckSQLWithSchema_InsertColumnsVariants tests INSERT with various column specifications
func TestCheckSQLWithSchema_InsertColumnsVariants(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		name TEXT,
		created DATETIME DEFAULT CURRENT_TIMESTAMP,
		status TEXT DEFAULT 'active',
		data BLOB
	)`)

	tests := []struct {
		name    string
		sql     string
		wantDet bool
	}{
		// Non-deterministic (missing column with non-det DEFAULT)
		{
			name:    "missing non-det DEFAULT column",
			sql:     "INSERT INTO t (id, name, status, data) VALUES (1, 'test', 'active', NULL)",
			wantDet: false,
		},
		{
			name:    "only id specified",
			sql:     "INSERT INTO t (id) VALUES (1)",
			wantDet: false,
		},
		{
			name:    "id and name only",
			sql:     "INSERT INTO t (id, name) VALUES (1, 'test')",
			wantDet: false,
		},

		// Deterministic
		{
			name:    "all columns specified including created",
			sql:     "INSERT INTO t (id, name, created, status, data) VALUES (1, 'test', '2024-01-01', 'new', NULL)",
			wantDet: true,
		},
		{
			name:    "skip det DEFAULT column",
			sql:     "INSERT INTO t (id, name, created, data) VALUES (1, 'test', '2024-01-01', NULL)",
			wantDet: true,
		},
		{
			name:    "id and created only",
			sql:     "INSERT INTO t (id, created) VALUES (1, '2024-01-01')",
			wantDet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQLWithSchema(tt.sql, schema)
			if result.IsDeterministic != tt.wantDet {
				t.Errorf("got IsDeterministic=%v, want %v, reason=%s",
					result.IsDeterministic, tt.wantDet, result.Reason)
			}
		})
	}
}

// TestCheckSQLWithSchema_TriggerOnDifferentOperations tests triggers for INSERT/UPDATE/DELETE
func TestCheckSQLWithSchema_TriggerOnDifferentOperations(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE data (id INTEGER, value TEXT)`)
	schema.AddTable(`CREATE TABLE audit (ts TEXT)`)
	schema.AddTrigger(`CREATE TRIGGER audit_insert AFTER INSERT ON data
		BEGIN INSERT INTO audit (ts) VALUES (datetime('now')); END`)
	schema.AddTrigger(`CREATE TRIGGER audit_update AFTER UPDATE ON data
		BEGIN INSERT INTO audit (ts) VALUES (datetime('now')); END`)
	schema.AddTrigger(`CREATE TRIGGER audit_delete AFTER DELETE ON data
		BEGIN INSERT INTO audit (ts) VALUES (datetime('now')); END`)

	tests := []struct {
		name    string
		sql     string
		wantDet bool
	}{
		{"INSERT triggers non-det", "INSERT INTO data (id, value) VALUES (1, 'test')", false},
		{"UPDATE triggers non-det", "UPDATE data SET value = 'new' WHERE id = 1", false},
		{"DELETE triggers non-det", "DELETE FROM data WHERE id = 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQLWithSchema(tt.sql, schema)
			if result.IsDeterministic != tt.wantDet {
				t.Errorf("got IsDeterministic=%v, want %v, reason=%s",
					result.IsDeterministic, tt.wantDet, result.Reason)
			}
		})
	}
}

// TestCheckSQLWithSchema_CombinedChecks tests that basic checks still work with schema
func TestCheckSQLWithSchema_CombinedChecks(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE simple (id INTEGER, data TEXT)`)

	tests := []struct {
		name    string
		sql     string
		wantDet bool
		reason  string
	}{
		// Basic non-determinism should still be detected
		{"random() still detected", "INSERT INTO simple (id, data) VALUES (random(), 'test')", false, "random"},
		{"datetime('now') still detected", "INSERT INTO simple (id, data) VALUES (1, datetime('now'))", false, "datetime"},
		{"CURRENT_TIMESTAMP still detected", "INSERT INTO simple (id, data) VALUES (1, CURRENT_TIMESTAMP)", false, "identifier"},
		{"column ref still detected", "UPDATE simple SET data = id WHERE id = 1", false, "column reference"},
		{"subquery still detected", "INSERT INTO simple SELECT * FROM other", false, "subquery"},

		// Deterministic should pass
		{"literal INSERT passes", "INSERT INTO simple (id, data) VALUES (1, 'test')", true, ""},
		{"literal UPDATE passes", "UPDATE simple SET data = 'new' WHERE id = 1", true, ""},
		{"literal DELETE passes", "DELETE FROM simple WHERE id = 1", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CheckSQLWithSchema(tt.sql, schema)
			if result.IsDeterministic != tt.wantDet {
				t.Errorf("got IsDeterministic=%v, want %v, reason=%s",
					result.IsDeterministic, tt.wantDet, result.Reason)
			}
		})
	}
}

// TestCheckStatementWithSchema tests the parsed statement version
func TestCheckStatementWithSchema(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		name TEXT,
		created DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	t.Run("parsed statement with non-det DEFAULT", func(t *testing.T) {
		parser := sql.NewParser(strings.NewReader("INSERT INTO events (id, name) VALUES (1, 'test')"))
		stmt, err := parser.ParseStatement()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		result := CheckStatementWithSchema(stmt, schema)
		if result.IsDeterministic {
			t.Error("expected non-deterministic due to missing created column")
		}
	})

	t.Run("parsed statement with all columns", func(t *testing.T) {
		parser := sql.NewParser(strings.NewReader("INSERT INTO events (id, name, created) VALUES (1, 'test', '2024-01-01')"))
		stmt, err := parser.ParseStatement()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		result := CheckStatementWithSchema(stmt, schema)
		if !result.IsDeterministic {
			t.Errorf("expected deterministic, reason=%s", result.Reason)
		}
	})

	t.Run("nil schema passes through", func(t *testing.T) {
		parser := sql.NewParser(strings.NewReader("INSERT INTO events (id, name) VALUES (1, 'test')"))
		stmt, err := parser.ParseStatement()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		result := CheckStatementWithSchema(stmt, nil)
		if !result.IsDeterministic {
			t.Error("nil schema should pass through basic check")
		}
	})
}

// TestSchema_AddTablePreservesTrigger tests that adding table preserves existing trigger info
func TestSchema_AddTablePreservesTrigger(t *testing.T) {
	t.Parallel()

	schema := NewSchema()

	// Add trigger first (table doesn't exist yet)
	schema.AddTrigger(`CREATE TRIGGER t AFTER INSERT ON src
		BEGIN INSERT INTO dst (ts) VALUES (datetime('now')); END`)

	// Now add the table
	schema.AddTable(`CREATE TABLE src (id INTEGER, name TEXT)`)

	// Trigger flag should be preserved
	table := schema.Tables["src"]
	if !table.HasTrigger {
		t.Error("HasTrigger should be preserved after AddTable")
	}

	// Column info should also be available
	if _, ok := table.Columns["id"]; !ok {
		t.Error("column info should be available")
	}
}

// TestSchema_MultipleColumns tests table with many column types
func TestSchema_MultipleColumns(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	err := schema.AddTable(`CREATE TABLE complex (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		deleted_at DATETIME,
		status TEXT DEFAULT 'active',
		priority INTEGER DEFAULT 0,
		score REAL DEFAULT 0.0,
		data BLOB,
		token TEXT DEFAULT (hex(randomblob(16))),
		hash TEXT DEFAULT (upper('default'))
	)`)
	if err != nil {
		t.Fatalf("AddTable failed: %v", err)
	}

	table := schema.Tables["complex"]

	// Check non-det defaults
	nonDetCols := []string{"created_at", "updated_at", "token"}
	for _, col := range nonDetCols {
		c := table.Columns[col]
		if c == nil {
			t.Errorf("column %s not found", col)
			continue
		}
		if !c.HasDefault || c.DefaultIsDeterministic {
			t.Errorf("column %s should have non-det DEFAULT", col)
		}
	}

	// Check det defaults
	detCols := []string{"status", "priority", "score", "hash"}
	for _, col := range detCols {
		c := table.Columns[col]
		if c == nil {
			t.Errorf("column %s not found", col)
			continue
		}
		if c.HasDefault && !c.DefaultIsDeterministic {
			t.Errorf("column %s should have deterministic DEFAULT", col)
		}
	}

	// Check no default columns
	noDefaultCols := []string{"name", "email", "deleted_at", "data"}
	for _, col := range noDefaultCols {
		c := table.Columns[col]
		if c == nil {
			t.Errorf("column %s not found", col)
			continue
		}
		if c.HasDefault {
			t.Errorf("column %s should not have DEFAULT", col)
		}
	}
}

// TestSchema_NonTableStatements tests handling of non-CREATE TABLE statements
func TestSchema_NonTableStatements(t *testing.T) {
	t.Parallel()

	schema := NewSchema()

	// These should not cause errors but also not add tables
	nonTableStmts := []string{
		"INSERT INTO t (id) VALUES (1)",
		"UPDATE t SET x = 1",
		"DELETE FROM t",
		"SELECT * FROM t",
	}

	for _, stmt := range nonTableStmts {
		err := schema.AddTable(stmt)
		if err != nil {
			t.Errorf("AddTable(%q) should not error, got: %v", stmt, err)
		}
		if len(schema.Tables) != 0 {
			t.Errorf("no tables should be added for %q", stmt)
		}
	}
}

// TestSchema_NonTriggerStatements tests handling of non-CREATE TRIGGER statements
func TestSchema_NonTriggerStatements(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE t (id INTEGER)`)

	// These should not cause errors
	nonTriggerStmts := []string{
		"INSERT INTO t (id) VALUES (1)",
		"UPDATE t SET id = 1",
		"DELETE FROM t",
		"SELECT * FROM t",
		"CREATE TABLE other (id INTEGER)",
	}

	for _, stmt := range nonTriggerStmts {
		err := schema.AddTrigger(stmt)
		if err != nil {
			t.Errorf("AddTrigger(%q) should not error, got: %v", stmt, err)
		}
	}

	// Table should not have trigger marked
	if schema.Tables["t"].HasTrigger {
		t.Error("HasTrigger should be false")
	}
}

// TestCheckSQLWithSchema_MultipleNonDetReasons tests statements with multiple non-det sources
func TestCheckSQLWithSchema_MultipleNonDetReasons(t *testing.T) {
	t.Parallel()

	schema := NewSchema()
	schema.AddTable(`CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		ts DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	schema.AddTable(`CREATE TABLE log (ts TEXT)`)
	schema.AddTrigger(`CREATE TRIGGER t_trigger AFTER INSERT ON t
		BEGIN INSERT INTO log (ts) VALUES (datetime('now')); END`)

	// This INSERT:
	// 1. Missing ts column (non-det DEFAULT)
	// 2. Has non-det trigger
	// 3. Uses random() in value
	result := CheckSQLWithSchema("INSERT INTO t (id) VALUES (1)", schema)
	if result.IsDeterministic {
		t.Error("expected non-deterministic")
	}
	// Should report trigger (first schema check)
	if !strings.Contains(result.Reason, "trigger") {
		t.Errorf("expected trigger reason, got: %s", result.Reason)
	}
}

// BenchmarkCheckSQLWithSchema benchmarks schema-aware checking
func BenchmarkCheckSQLWithSchema(b *testing.B) {
	schema := NewSchema()
	schema.AddTable(`CREATE TABLE events (
		id INTEGER PRIMARY KEY,
		name TEXT,
		created DATETIME DEFAULT CURRENT_TIMESTAMP,
		status TEXT DEFAULT 'active'
	)`)

	sqls := []string{
		"INSERT INTO events (id, name) VALUES (1, 'test')",
		"INSERT INTO events (id, name, created, status) VALUES (1, 'test', '2024-01-01', 'new')",
		"UPDATE events SET name = 'updated' WHERE id = 1",
		"DELETE FROM events WHERE id = 1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sqlStr := range sqls {
			CheckSQLWithSchema(sqlStr, schema)
		}
	}
}

// BenchmarkSchema_AddTable benchmarks table parsing
func BenchmarkSchema_AddTable(b *testing.B) {
	createSQL := `CREATE TABLE complex (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		status TEXT DEFAULT 'active',
		priority INTEGER DEFAULT 0,
		score REAL DEFAULT 0.0,
		data BLOB
	)`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema := NewSchema()
		schema.AddTable(createSQL)
	}
}
