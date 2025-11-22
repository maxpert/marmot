package protocol

import (
	"testing"
)

// TestParseStatement_VitessEdgeCases tests edge cases that Vitess handles better than regex
func TestParseStatement_VitessEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantType  StatementType
		wantDB    string
		wantTable string
	}{
		{
			name:      "Qualified table name - SELECT",
			sql:       "SELECT * FROM mydb.users",
			wantType:  StatementSelect,
			wantDB:    "",
			wantTable: "",
		},
		{
			name:      "INSERT with qualified name",
			sql:       "INSERT INTO testdb.users VALUES (1, 'alice')",
			wantType:  StatementInsert,
			wantDB:    "testdb",
			wantTable: "users",
		},
		{
			name:      "UPDATE with qualified name",
			sql:       "UPDATE analytics.metrics SET count = 100",
			wantType:  StatementUpdate,
			wantDB:    "analytics",
			wantTable: "metrics",
		},
		{
			name:      "DELETE with qualified name",
			sql:       "DELETE FROM logs.old_entries WHERE date < '2020-01-01'",
			wantType:  StatementDelete,
			wantDB:    "logs",
			wantTable: "old_entries",
		},
		{
			name:     "Multi-line comment",
			sql:      "/* Comment line 1\n   Comment line 2 */\nSELECT * FROM users",
			wantType: StatementSelect,
		},
		{
			name:      "Backtick identifiers - table",
			sql:       "INSERT INTO `my-table` VALUES (1)",
			wantType:  StatementInsert,
			wantTable: "my-table",
		},
		{
			name:      "Backtick identifiers - database and table",
			sql:       "INSERT INTO `my-db`.`my-table` VALUES (1)",
			wantType:  StatementInsert,
			wantDB:    "my-db",
			wantTable: "my-table",
		},
		{
			name:      "CREATE DATABASE IF NOT EXISTS",
			sql:       "CREATE DATABASE IF NOT EXISTS analytics",
			wantType:  StatementCreateDatabase,
			wantDB:    "analytics",
		},
		{
			name:     "USE with backticks",
			sql:      "USE `my-database`",
			wantType: StatementUseDatabase,
			wantDB:   "my-database",
		},
		{
			name:     "Complex UPDATE with JOIN",
			sql:      "UPDATE users u JOIN orders o ON u.id = o.user_id SET u.total = o.amount",
			wantType: StatementUnsupported, // UPDATE...JOIN is MySQL-only
			// Note: Table extraction from complex JOINs is best-effort
			// wantTable: "users", // Vitess may parse this differently
		},
		{
			name:     "DBeaver-style comment - SHOW DATABASES",
			sql:      "/* ApplicationName=DBeaver 25.2.5 - SQLEditor <Script.sql> */ SHOW DATABASES",
			wantType: StatementShowDatabases,
		},
		{
			name:     "DBeaver-style comment - Main connection",
			sql:      "/* ApplicationName=DBeaver 25.2.5 - Main */ SET autocommit=1",
			wantType: StatementSet, // SET skips SQLite validation
		},
		{
			name:     "DBeaver-style comment - Metadata",
			sql:      "/* ApplicationName=DBeaver 25.2.5 - Metadata */ SET autocommit=1",
			wantType: StatementSet, // SET skips SQLite validation
		},
		{
			name:      "String with semicolon",
			sql:       "INSERT INTO logs VALUES (1, 'Error: failed; retry')",
			wantType:  StatementInsert,
			wantTable: "logs",
		},
		{
			name:      "String with comment-like content",
			sql:       "INSERT INTO comments VALUES (1, '/* not a comment */')",
			wantType:  StatementInsert,
			wantTable: "comments",
		},
		{
			name:     "MySQL-style comment",
			sql:      "# This is a comment\nSELECT * FROM users",
			wantType: StatementUnsupported, // # comments are MySQL-only, not SQLite
		},
		{
			name:     "Double-dash comment",
			sql:      "-- This is a comment\nSELECT * FROM users",
			wantType: StatementSelect,
		},
		{
			name:      "CREATE TABLE with backtick identifiers",
			sql:       "CREATE TABLE `user-data` (id INT, `user-name` VARCHAR(255))",
			wantType:  StatementDDL,
			wantTable: "user-data",
		},
		{
			name:      "ALTER TABLE with qualified name",
			sql:       "ALTER TABLE mydb.users ADD COLUMN email VARCHAR(255)",
			wantType:  StatementDDL,
			wantDB:    "mydb",
			wantTable: "users",
		},
		{
			name:      "DROP TABLE with qualified name",
			sql:       "DROP TABLE IF EXISTS testdb.old_table",
			wantType:  StatementDDL,
			wantDB:    "testdb",
			wantTable: "old_table",
		},
		{
			name:     "SELECT with subquery",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantType: StatementSelect,
		},
		{
			name:     "SELECT with CTE",
			sql:      "WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active_users",
			wantType: StatementSelect,
		},
		{
			name:     "SELECT with window function",
			sql:      "SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) as row_num FROM users",
			wantType: StatementSelect,
		},
		{
			name:      "INSERT with ON DUPLICATE KEY UPDATE",
			sql:       "INSERT INTO users VALUES (1, 'alice') ON DUPLICATE KEY UPDATE name = 'alice'",
			wantType:  StatementUnsupported, // MySQL-only, use INSERT OR REPLACE in SQLite
			wantTable: "",
		},
		{
			name:      "REPLACE with qualified name",
			sql:       "REPLACE INTO analytics.metrics VALUES (1, 100)",
			wantType:  StatementReplace,
			wantDB:    "analytics",
			wantTable: "metrics",
		},
		{
			name:     "CREATE SCHEMA (synonym for DATABASE)",
			sql:      "CREATE SCHEMA myschema",
			wantType: StatementCreateDatabase,
			wantDB:   "myschema",
		},
		{
			name:     "DROP SCHEMA (synonym for DATABASE)",
			sql:      "DROP SCHEMA oldschema",
			wantType: StatementDropDatabase,
			wantDB:   "oldschema",
		},
		{
			name:     "SHOW SCHEMAS (synonym for DATABASES)",
			sql:      "SHOW SCHEMAS",
			wantType: StatementShowDatabases,
		},
		{
			name:      "Multiline INSERT",
			sql:       "INSERT INTO users\n(id, name, email)\nVALUES\n(1, 'alice', 'alice@example.com')",
			wantType:  StatementInsert,
			wantTable: "users",
		},
		{
			name:     "Transaction with trailing semicolon",
			sql:      "BEGIN;",
			wantType: StatementBegin,
		},
		{
			name:     "Query with Unicode characters",
			sql:      "INSERT INTO users VALUES (1, '你好世界')",
			wantType: StatementInsert,
		},
		{
			name:     "Query with emoji",
			sql:      "INSERT INTO messages VALUES (1, 'Hello 👋')",
			wantType: StatementInsert,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.wantType {
				t.Errorf("Type = %v (%s), want %v", stmt.Type, statementTypeName(stmt.Type), tt.wantType)
			}

			if tt.wantDB != "" && stmt.Database != tt.wantDB {
				t.Errorf("Database = %v, want %v", stmt.Database, tt.wantDB)
			}

			if tt.wantTable != "" && stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}
		})
	}
}

// Helper function for better error messages
func statementTypeName(t StatementType) string {
	switch t {
	case StatementInsert:
		return "INSERT"
	case StatementReplace:
		return "REPLACE"
	case StatementUpdate:
		return "UPDATE"
	case StatementDelete:
		return "DELETE"
	case StatementLoadData:
		return "LOAD_DATA"
	case StatementDDL:
		return "DDL"
	case StatementDCL:
		return "DCL"
	case StatementBegin:
		return "BEGIN"
	case StatementCommit:
		return "COMMIT"
	case StatementRollback:
		return "ROLLBACK"
	case StatementSavepoint:
		return "SAVEPOINT"
	case StatementXA:
		return "XA"
	case StatementLock:
		return "LOCK"
	case StatementSelect:
		return "SELECT"
	case StatementAdmin:
		return "ADMIN"
	case StatementSet:
		return "SET"
	case StatementShowDatabases:
		return "SHOW_DATABASES"
	case StatementUseDatabase:
		return "USE_DATABASE"
	case StatementCreateDatabase:
		return "CREATE_DATABASE"
	case StatementDropDatabase:
		return "DROP_DATABASE"
	default:
		return "UNKNOWN"
	}
}

// TestParseStatement_MySQLCompatibility tests MySQL-specific syntax
func TestParseStatement_MySQLCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType StatementType
	}{
		{
			name:     "STRAIGHT_JOIN",
			sql:      "SELECT * FROM users STRAIGHT_JOIN orders ON users.id = orders.user_id",
			wantType: StatementUnsupported, // MySQL-only join hint
		},
		{
			name:     "FORCE INDEX",
			sql:      "SELECT * FROM users FORCE INDEX (idx_email) WHERE email = 'test@example.com'",
			wantType: StatementUnsupported, // MySQL-only hint
		},
		{
			name:     "USE INDEX",
			sql:      "SELECT * FROM users USE INDEX (idx_name) WHERE name = 'alice'",
			wantType: StatementUnsupported, // MySQL-only hint
		},
		{
			name:     "IGNORE INDEX",
			sql:      "SELECT * FROM users IGNORE INDEX (idx_id) WHERE id = 1",
			wantType: StatementUnsupported, // MySQL-only hint
		},
		{
			name:     "INSERT IGNORE",
			sql:      "INSERT IGNORE INTO users VALUES (1, 'alice')",
			wantType: StatementUnsupported, // MySQL-only, use INSERT OR IGNORE in SQLite
		},
		// MySQL-specific syntax - rejected by SQLite validation
		{
			name:     "INSERT DELAYED",
			sql:      "INSERT DELAYED INTO logs VALUES (1, 'message')",
			wantType: StatementUnsupported, // MySQL-only
		},
		{
			name:     "DELETE with LIMIT",
			sql:      "DELETE FROM logs WHERE old = 1 LIMIT 1000",
			wantType: StatementUnsupported, // DELETE LIMIT not in standard SQLite
		},
		{
			name:     "UPDATE with LIMIT",
			sql:      "UPDATE users SET active = 0 LIMIT 10",
			wantType: StatementUnsupported, // UPDATE LIMIT not in standard SQLite
		},
		{
			name:     "SELECT with LOCK IN SHARE MODE",
			sql:      "SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE",
			wantType: StatementUnsupported, // MySQL locking not in SQLite
		},
		{
			name:     "SELECT with FOR UPDATE",
			sql:      "SELECT * FROM users WHERE id = 1 FOR UPDATE",
			wantType: StatementUnsupported, // MySQL locking not in SQLite
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.wantType {
				t.Errorf("Type = %v (%s), want %v", stmt.Type, statementTypeName(stmt.Type), tt.wantType)
			}
		})
	}
}

// BenchmarkParseStatement_Vitess benchmarks the Vitess parser
func BenchmarkParseStatement_Vitess(b *testing.B) {
	queries := []string{
		"INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
		"SELECT * FROM users WHERE id = 1",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM logs WHERE created_at < '2020-01-01'",
		"CREATE TABLE test (id INT PRIMARY KEY, name TEXT)",
		"/* ApplicationName=DBeaver */ SHOW DATABASES",
		"INSERT INTO mydb.users VALUES (1, 'test')",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sql := queries[i%len(queries)]
		ParseStatement(sql)
	}
}
