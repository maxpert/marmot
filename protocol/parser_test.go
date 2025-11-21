package protocol

import (
	"strings"
	"testing"
)

func TestParseStatement_DML(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantType  StatementType
		wantTable string
	}{
		{
			name:      "INSERT",
			sql:       "INSERT INTO users VALUES (1, 'Alice')",
			wantType:  StatementInsert,
			wantTable: "users",
		},
		{
			name:      "INSERT case insensitive",
			sql:       "insert into customers values (1)",
			wantType:  StatementInsert,
			wantTable: "customers",
		},
		{
			name:      "UPDATE",
			sql:       "UPDATE users SET name = 'Bob' WHERE id = 1",
			wantType:  StatementUpdate,
			wantTable: "users",
		},
		{
			name:      "DELETE",
			sql:       "DELETE FROM orders WHERE status = 'cancelled'",
			wantType:  StatementDelete,
			wantTable: "orders",
		},
		{
			name:     "SELECT",
			sql:      "SELECT * FROM users",
			wantType: StatementSelect,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", stmt.Type, tt.wantType)
			}

			if tt.wantTable != "" && stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}

			// SQL is trimmed, so check it contains the original (after trimming)
			if stmt.SQL != strings.TrimSpace(tt.sql) {
				t.Errorf("SQL = %q, want %q", stmt.SQL, strings.TrimSpace(tt.sql))
			}
		})
	}
}

func TestParseStatement_DDL_CreateDrop(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
	}{
		{
			name:      "CREATE TABLE",
			sql:       "CREATE TABLE users (id INT, name TEXT)",
			wantTable: "users",
		},
		{
			name:      "CREATE TABLE IF NOT EXISTS",
			sql:       "CREATE TABLE IF NOT EXISTS products (id INT)",
			wantTable: "products",
		},
		{
			name:      "DROP TABLE",
			sql:       "DROP TABLE old_data",
			wantTable: "old_data",
		},
		{
			name:      "DROP TABLE IF EXISTS",
			sql:       "DROP TABLE IF EXISTS temp_table",
			wantTable: "temp_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want DDL", stmt.Type)
			}

			if stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}
		})
	}
}

func TestParseStatement_DDL_Alter(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
	}{
		{
			name:      "ALTER TABLE ADD COLUMN",
			sql:       "ALTER TABLE users ADD COLUMN email TEXT",
			wantTable: "users",
		},
		{
			name:      "ALTER TABLE DROP COLUMN",
			sql:       "ALTER TABLE products DROP COLUMN legacy_field",
			wantTable: "products",
		},
		{
			name:      "ALTER TABLE RENAME",
			sql:       "ALTER TABLE old_name RENAME TO new_name",
			wantTable: "old_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want DDL", stmt.Type)
			}

			if stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}
		})
	}
}

func TestParseStatement_DDL_Truncate(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
	}{
		{
			name:      "TRUNCATE TABLE",
			sql:       "TRUNCATE TABLE logs",
			wantTable: "logs",
		},
		{
			name:      "TRUNCATE without TABLE keyword",
			sql:       "TRUNCATE sessions",
			wantTable: "sessions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want DDL", stmt.Type)
			}

			if stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}
		})
	}
}

func TestParseStatement_DDL_RenameTable(t *testing.T) {
	stmt := ParseStatement("RENAME TABLE old_users TO new_users")

	if stmt.Type != StatementDDL {
		t.Errorf("Type = %v, want DDL", stmt.Type)
	}

	if stmt.TableName != "old_users" {
		t.Errorf("TableName = %v, want old_users", stmt.TableName)
	}
}

func TestParseStatement_DDL_Indexes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE INDEX",
			sql:  "CREATE INDEX idx_email ON users(email)",
		},
		{
			name: "CREATE UNIQUE INDEX",
			sql:  "CREATE UNIQUE INDEX idx_username ON users(username)",
		},
		{
			name: "DROP INDEX",
			sql:  "DROP INDEX idx_email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want DDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Views(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE VIEW",
			sql:  "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
		},
		{
			name: "CREATE OR REPLACE VIEW",
			sql:  "CREATE OR REPLACE VIEW user_summary AS SELECT id, name FROM users",
		},
		{
			name: "DROP VIEW",
			sql:  "DROP VIEW active_users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want DDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Triggers(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE TRIGGER",
			sql:  "CREATE TRIGGER update_timestamp AFTER UPDATE ON users BEGIN END",
		},
		{
			name: "DROP TRIGGER",
			sql:  "DROP TRIGGER update_timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want DDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_TransactionControl(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType StatementType
	}{
		{
			name:     "BEGIN",
			sql:      "BEGIN",
			wantType: StatementBegin,
		},
		{
			name:     "START TRANSACTION",
			sql:      "START TRANSACTION",
			wantType: StatementBegin,
		},
		{
			name:     "COMMIT",
			sql:      "COMMIT",
			wantType: StatementCommit,
		},
		{
			name:     "ROLLBACK",
			sql:      "ROLLBACK",
			wantType: StatementRollback,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", stmt.Type, tt.wantType)
			}
		})
	}
}

func TestParseStatement_CaseInsensitive(t *testing.T) {
	tests := []string{
		"insert into users values (1)",
		"INSERT INTO users VALUES (1)",
		"InSeRt InTo users VaLuEs (1)",
	}

	for _, sql := range tests {
		stmt := ParseStatement(sql)
		if stmt.Type != StatementInsert {
			t.Errorf("Failed to parse case-insensitive SQL: %s", sql)
		}
	}
}

func TestExtractConsistencyHint(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantLevel ConsistencyLevel
		wantFound bool
	}{
		{
			name:      "QUORUM hint",
			sql:       "/*+ CONSISTENCY(QUORUM) */ SELECT * FROM users",
			wantLevel: ConsistencyQuorum,
			wantFound: true,
		},
		{
			name:      "ALL hint",
			sql:       "/*+ CONSISTENCY(ALL) */ UPDATE users SET name = 'Bob'",
			wantLevel: ConsistencyAll,
			wantFound: true,
		},
		{
			name:      "ONE hint",
			sql:       "/*+ CONSISTENCY(ONE) */ INSERT INTO users VALUES (1)",
			wantLevel: ConsistencyOne,
			wantFound: true,
		},
		{
			name:      "LOCAL_ONE hint",
			sql:       "/*+ CONSISTENCY(LOCAL_ONE) */ DELETE FROM cache",
			wantLevel: ConsistencyLocalOne,
			wantFound: true,
		},
		{
			name:      "No hint",
			sql:       "SELECT * FROM users",
			wantLevel: ConsistencyLocalOne,
			wantFound: false,
		},
		{
			name:      "Hint with spaces",
			sql:       "/*+  CONSISTENCY  (  QUORUM  )  */ SELECT * FROM users",
			wantLevel: ConsistencyQuorum,
			wantFound: true,
		},
		{
			name:      "Invalid hint",
			sql:       "/*+ CONSISTENCY(INVALID) */ SELECT * FROM users",
			wantLevel: ConsistencyLocalOne,
			wantFound: false,
		},
		{
			name:      "Case insensitive hint",
			sql:       "/*+ consistency(quorum) */ SELECT * FROM users",
			wantLevel: ConsistencyQuorum,
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			level, found := ExtractConsistencyHint(tt.sql)

			if found != tt.wantFound {
				t.Errorf("Found = %v, want %v", found, tt.wantFound)
			}

			if tt.wantFound && level != tt.wantLevel {
				t.Errorf("Level = %v, want %v", level, tt.wantLevel)
			}
		})
	}
}

func TestIsMutation(t *testing.T) {
	mutations := []string{
		"INSERT INTO users VALUES (1)",
		"UPDATE users SET name = 'Alice'",
		"DELETE FROM logs WHERE old = 1",
		"CREATE TABLE test (id INT)",
		"DROP TABLE old_data",
		"ALTER TABLE users ADD COLUMN email TEXT",
		"TRUNCATE TABLE logs",
	}

	for _, sql := range mutations {
		stmt := ParseStatement(sql)
		if !IsMutation(stmt) {
			t.Errorf("Should be mutation: %s", sql)
		}
	}

	nonMutations := []string{
		"SELECT * FROM users",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
	}

	for _, sql := range nonMutations {
		stmt := ParseStatement(sql)
		if IsMutation(stmt) {
			t.Errorf("Should not be mutation: %s", sql)
		}
	}
}

func TestIsTransactionControl(t *testing.T) {
	txnControls := []string{
		"BEGIN",
		"START TRANSACTION",
		"COMMIT",
		"ROLLBACK",
	}

	for _, sql := range txnControls {
		stmt := ParseStatement(sql)
		if !IsTransactionControl(stmt) {
			t.Errorf("Should be transaction control: %s", sql)
		}
	}

	nonTxnControls := []string{
		"SELECT * FROM users",
		"INSERT INTO users VALUES (1)",
		"CREATE TABLE test (id INT)",
	}

	for _, sql := range nonTxnControls {
		stmt := ParseStatement(sql)
		if IsTransactionControl(stmt) {
			t.Errorf("Should not be transaction control: %s", sql)
		}
	}
}

func TestParseStatement_ComplexQueries(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantType  StatementType
		wantTable string
	}{
		{
			name:      "Multiline INSERT",
			sql:       "INSERT INTO users\n(id, name, email)\nVALUES\n(1, 'Alice', 'alice@example.com')",
			wantType:  StatementInsert,
			wantTable: "users",
		},
		{
			name:      "UPDATE with subquery",
			sql:       "UPDATE orders SET total = (SELECT SUM(price) FROM items) WHERE id = 1",
			wantType:  StatementUpdate,
			wantTable: "orders",
		},
		{
			name:      "DELETE with JOIN",
			sql:       "DELETE FROM logs WHERE created_at < DATE('now', '-30 days')",
			wantType:  StatementDelete,
			wantTable: "logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", stmt.Type, tt.wantType)
			}

			if tt.wantTable != "" && stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}
		})
	}
}

func BenchmarkParseStatement(b *testing.B) {
	sql := "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseStatement(sql)
	}
}

func BenchmarkExtractConsistencyHint(b *testing.B) {
	sql := "/*+ CONSISTENCY(QUORUM) */ SELECT * FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExtractConsistencyHint(sql)
	}
}

func TestParseStatement_DML_REPLACE(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
	}{
		{
			name:      "REPLACE INTO",
			sql:       "REPLACE INTO users VALUES (1, 'Alice')",
			wantTable: "users",
		},
		{
			name:      "REPLACE case insensitive",
			sql:       "replace into products values (1)",
			wantTable: "products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementReplace {
				t.Errorf("Type = %v, want StatementReplace", stmt.Type)
			}

			if stmt.TableName != tt.wantTable {
				t.Errorf("TableName = %v, want %v", stmt.TableName, tt.wantTable)
			}
		})
	}
}

func TestParseStatement_DML_LOAD(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "LOAD DATA",
			sql:  "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users",
		},
		{
			name: "LOAD XML",
			sql:  "LOAD XML INFILE '/tmp/data.xml' INTO TABLE products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementLoadData {
				t.Errorf("Type = %v, want StatementLoadData", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Database(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType StatementType
		expectedDB   string
	}{
		{
			name:         "CREATE DATABASE",
			sql:          "CREATE DATABASE testdb",
			expectedType: StatementCreateDatabase,
			expectedDB:   "testdb",
		},
		{
			name:         "CREATE SCHEMA",
			sql:          "CREATE SCHEMA myschema",
			expectedType: StatementCreateDatabase,
			expectedDB:   "myschema",
		},
		{
			name:         "DROP DATABASE",
			sql:          "DROP DATABASE olddb",
			expectedType: StatementDropDatabase,
			expectedDB:   "olddb",
		},
		{
			name:         "DROP SCHEMA",
			sql:          "DROP SCHEMA temp_schema",
			expectedType: StatementDropDatabase,
			expectedDB:   "temp_schema",
		},
		{
			name:         "ALTER DATABASE",
			sql:          "ALTER DATABASE mydb CHARACTER SET utf8mb4",
			expectedType: StatementDDL,
			expectedDB:   "",
		},
		{
			name:         "ALTER SCHEMA",
			sql:          "ALTER SCHEMA myschema DEFAULT CHARACTER SET utf8",
			expectedType: StatementDDL,
			expectedDB:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.expectedType {
				t.Errorf("Type = %v, want %v", stmt.Type, tt.expectedType)
			}

			if tt.expectedDB != "" && stmt.Database != tt.expectedDB {
				t.Errorf("Database = %v, want %v", stmt.Database, tt.expectedDB)
			}
		})
	}
}

func TestParseStatement_DDL_Procedures(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE PROCEDURE",
			sql:  "CREATE PROCEDURE calculate_total() BEGIN END",
		},
		{
			name: "DROP PROCEDURE",
			sql:  "DROP PROCEDURE IF EXISTS calculate_total",
		},
		{
			name: "ALTER PROCEDURE",
			sql:  "ALTER PROCEDURE my_proc SQL SECURITY INVOKER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Functions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE FUNCTION",
			sql:  "CREATE FUNCTION add_numbers(a INT, b INT) RETURNS INT RETURN a + b",
		},
		{
			name: "DROP FUNCTION",
			sql:  "DROP FUNCTION IF EXISTS add_numbers",
		},
		{
			name: "ALTER FUNCTION",
			sql:  "ALTER FUNCTION my_func COMMENT 'Updated function'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Events(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE EVENT",
			sql:  "CREATE EVENT cleanup_old_logs ON SCHEDULE EVERY 1 DAY DO DELETE FROM logs WHERE created_at < NOW() - INTERVAL 30 DAY",
		},
		{
			name: "DROP EVENT",
			sql:  "DROP EVENT IF EXISTS cleanup_old_logs",
		},
		{
			name: "ALTER EVENT",
			sql:  "ALTER EVENT my_event ON SCHEDULE EVERY 2 DAY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Tablespaces(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE TABLESPACE",
			sql:  "CREATE TABLESPACE ts1 ADD DATAFILE 'ts1.ibd' ENGINE=InnoDB",
		},
		{
			name: "DROP TABLESPACE",
			sql:  "DROP TABLESPACE ts1",
		},
		{
			name: "ALTER TABLESPACE",
			sql:  "ALTER TABLESPACE ts1 RENAME TO new_ts1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_LogfileGroups(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE LOGFILE GROUP",
			sql:  "CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' ENGINE=NDB",
		},
		{
			name: "DROP LOGFILE GROUP",
			sql:  "DROP LOGFILE GROUP lg1",
		},
		{
			name: "ALTER LOGFILE GROUP",
			sql:  "ALTER LOGFILE GROUP lg1 ADD UNDOFILE 'undo2.dat'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_Servers(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE SERVER",
			sql:  "CREATE SERVER s1 FOREIGN DATA WRAPPER mysql OPTIONS (HOST 'remote.host')",
		},
		{
			name: "DROP SERVER",
			sql:  "DROP SERVER s1",
		},
		{
			name: "ALTER SERVER",
			sql:  "ALTER SERVER s1 OPTIONS (HOST 'new.host')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_SpatialReference(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE SPATIAL REFERENCE SYSTEM",
			sql:  "CREATE SPATIAL REFERENCE SYSTEM 4120 NAME 'Greek' DEFINITION 'GEOGCS[\"Greek\"]'",
		},
		{
			name: "DROP SPATIAL REFERENCE SYSTEM",
			sql:  "DROP SPATIAL REFERENCE SYSTEM 4120",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_ResourceGroups(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE RESOURCE GROUP",
			sql:  "CREATE RESOURCE GROUP batch TYPE = USER VCPU = 0-3",
		},
		{
			name: "DROP RESOURCE GROUP",
			sql:  "DROP RESOURCE GROUP batch",
		},
		{
			name: "ALTER RESOURCE GROUP",
			sql:  "ALTER RESOURCE GROUP batch VCPU = 0-7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDDL {
				t.Errorf("Type = %v, want StatementDDL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DDL_AlterView(t *testing.T) {
	stmt := ParseStatement("ALTER VIEW active_users AS SELECT * FROM users WHERE status = 'active'")

	if stmt.Type != StatementDDL {
		t.Errorf("Type = %v, want StatementDDL", stmt.Type)
	}
}

func TestParseStatement_DCL_UserManagement(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE USER",
			sql:  "CREATE USER 'alice'@'localhost' IDENTIFIED BY 'password'",
		},
		{
			name: "DROP USER",
			sql:  "DROP USER 'alice'@'localhost'",
		},
		{
			name: "ALTER USER",
			sql:  "ALTER USER 'alice'@'localhost' IDENTIFIED BY 'newpass'",
		},
		{
			name: "RENAME USER",
			sql:  "RENAME USER 'alice'@'localhost' TO 'alice_new'@'localhost'",
		},
		{
			name: "SET PASSWORD",
			sql:  "SET PASSWORD FOR 'alice'@'localhost' = 'newpass'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDCL {
				t.Errorf("Type = %v, want StatementDCL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DCL_Privileges(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "GRANT privileges",
			sql:  "GRANT SELECT, INSERT ON mydb.* TO 'alice'@'localhost'",
		},
		{
			name: "GRANT ALL",
			sql:  "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%'",
		},
		{
			name: "REVOKE privileges",
			sql:  "REVOKE INSERT ON mydb.* FROM 'alice'@'localhost'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDCL {
				t.Errorf("Type = %v, want StatementDCL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_DCL_Roles(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CREATE ROLE",
			sql:  "CREATE ROLE 'app_developer'",
		},
		{
			name: "DROP ROLE",
			sql:  "DROP ROLE 'app_developer'",
		},
		{
			name: "SET ROLE",
			sql:  "SET ROLE 'app_developer'",
		},
		{
			name: "SET DEFAULT ROLE",
			sql:  "SET DEFAULT ROLE 'app_developer' TO 'alice'@'localhost'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementDCL {
				t.Errorf("Type = %v, want StatementDCL", stmt.Type)
			}
		})
	}
}

func TestParseStatement_Transaction_Savepoints(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType StatementType
	}{
		{
			name:     "SAVEPOINT",
			sql:      "SAVEPOINT sp1",
			wantType: StatementSavepoint,
		},
		{
			name:     "RELEASE SAVEPOINT",
			sql:      "RELEASE SAVEPOINT sp1",
			wantType: StatementSavepoint,
		},
		{
			name:     "SET TRANSACTION",
			sql:      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			wantType: StatementBegin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", stmt.Type, tt.wantType)
			}
		})
	}
}

func TestParseStatement_XA_Transactions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "XA START",
			sql:  "XA START 'xid1'",
		},
		{
			name: "XA END",
			sql:  "XA END 'xid1'",
		},
		{
			name: "XA PREPARE",
			sql:  "XA PREPARE 'xid1'",
		},
		{
			name: "XA COMMIT",
			sql:  "XA COMMIT 'xid1'",
		},
		{
			name: "XA ROLLBACK",
			sql:  "XA ROLLBACK 'xid1'",
		},
		{
			name: "XA RECOVER",
			sql:  "XA RECOVER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementXA {
				t.Errorf("Type = %v, want StatementXA", stmt.Type)
			}
		})
	}
}

func TestParseStatement_Lock_Statements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "LOCK TABLES",
			sql:  "LOCK TABLES users READ, products WRITE",
		},
		{
			name: "UNLOCK TABLES",
			sql:  "UNLOCK TABLES",
		},
		{
			name: "LOCK INSTANCE FOR BACKUP",
			sql:  "LOCK INSTANCE FOR BACKUP",
		},
		{
			name: "UNLOCK INSTANCE",
			sql:  "UNLOCK INSTANCE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementLock {
				t.Errorf("Type = %v, want StatementLock", stmt.Type)
			}
		})
	}
}

func TestParseStatement_Admin_Statements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "INSTALL PLUGIN",
			sql:  "INSTALL PLUGIN my_plugin SONAME 'plugin.so'",
		},
		{
			name: "UNINSTALL PLUGIN",
			sql:  "UNINSTALL PLUGIN my_plugin",
		},
		{
			name: "INSTALL COMPONENT",
			sql:  "INSTALL COMPONENT 'file://component_validate_password'",
		},
		{
			name: "UNINSTALL COMPONENT",
			sql:  "UNINSTALL COMPONENT 'file://component_validate_password'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.sql)

			if stmt.Type != StatementAdmin {
				t.Errorf("Type = %v, want StatementAdmin", stmt.Type)
			}
		})
	}
}

func TestIsMutation_ExtendedTypes(t *testing.T) {
	mutations := []string{
		"INSERT INTO users VALUES (1)",
		"REPLACE INTO users VALUES (1)",
		"UPDATE users SET name = 'Alice'",
		"DELETE FROM logs WHERE old = 1",
		"LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users",
		"CREATE TABLE test (id INT)",
		"DROP TABLE old_data",
		"ALTER TABLE users ADD COLUMN email TEXT",
		"TRUNCATE TABLE logs",
		"CREATE DATABASE testdb",
		"CREATE PROCEDURE test() BEGIN END",
		"CREATE USER 'alice'@'localhost'",
		"GRANT SELECT ON *.* TO 'alice'@'localhost'",
		"INSTALL PLUGIN my_plugin SONAME 'plugin.so'",
	}

	for _, sql := range mutations {
		stmt := ParseStatement(sql)
		if !IsMutation(stmt) {
			t.Errorf("Should be mutation: %s (type: %v)", sql, stmt.Type)
		}
	}

	nonMutations := []string{
		"SELECT * FROM users",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT sp1",
		"XA START 'xid1'",
		"LOCK TABLES users READ",
	}

	for _, sql := range nonMutations {
		stmt := ParseStatement(sql)
		if IsMutation(stmt) {
			t.Errorf("Should not be mutation: %s (type: %v)", sql, stmt.Type)
		}
	}
}

func TestIsTransactionControl_ExtendedTypes(t *testing.T) {
	txnControls := []string{
		"BEGIN",
		"START TRANSACTION",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
		"XA START 'xid1'",
		"XA COMMIT 'xid1'",
	}

	for _, sql := range txnControls {
		stmt := ParseStatement(sql)
		if !IsTransactionControl(stmt) {
			t.Errorf("Should be transaction control: %s (type: %v)", sql, stmt.Type)
		}
	}

	nonTxnControls := []string{
		"SELECT * FROM users",
		"INSERT INTO users VALUES (1)",
		"CREATE TABLE test (id INT)",
		"LOCK TABLES users READ",
		"GRANT SELECT ON *.* TO 'alice'@'localhost'",
	}

	for _, sql := range nonTxnControls {
		stmt := ParseStatement(sql)
		if IsTransactionControl(stmt) {
			t.Errorf("Should not be transaction control: %s (type: %v)", sql, stmt.Type)
		}
	}
}
