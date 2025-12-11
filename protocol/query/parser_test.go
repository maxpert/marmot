package query

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestExtractSystemVariables(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name     string
		sql      string
		wantVars []string
	}{
		{
			name:     "single system variable",
			sql:      "SELECT @@version",
			wantVars: []string{"VERSION"},
		},
		{
			name:     "DATABASE() function",
			sql:      "SELECT DATABASE()",
			wantVars: []string{"DATABASE()"},
		},
		{
			name:     "multiple system variables",
			sql:      "SELECT @@version, @@sql_mode",
			wantVars: []string{"VERSION", "SQL_MODE"},
		},
		{
			name:     "session variable",
			sql:      "SELECT @@session.autocommit",
			wantVars: []string{"AUTOCOMMIT"},
		},
		{
			name:     "global variable",
			sql:      "SELECT @@global.max_connections",
			wantVars: []string{"MAX_CONNECTIONS"},
		},
		{
			name:     "regular SELECT (no system vars)",
			sql:      "SELECT * FROM users",
			wantVars: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				t.Fatalf("expected SELECT statement, got %T", stmt)
			}

			vars := extractSystemVariables(sel)

			if len(vars) != len(tt.wantVars) {
				t.Errorf("got %d vars, want %d: got=%v, want=%v", len(vars), len(tt.wantVars), vars, tt.wantVars)
				return
			}

			for i, v := range vars {
				if v != tt.wantVars[i] {
					t.Errorf("var[%d] = %q, want %q", i, v, tt.wantVars[i])
				}
			}
		})
	}
}

func TestDetectVirtualTable(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name     string
		sql      string
		wantType VirtualTableType
	}{
		{
			name:     "MARMOT_CLUSTER_NODES",
			sql:      "SELECT * FROM MARMOT_CLUSTER_NODES",
			wantType: VirtualTableClusterNodes,
		},
		{
			name:     "MARMOT.CLUSTER_NODES",
			sql:      "SELECT * FROM MARMOT.CLUSTER_NODES",
			wantType: VirtualTableClusterNodes,
		},
		{
			name:     "lowercase marmot_cluster_nodes",
			sql:      "SELECT * FROM marmot_cluster_nodes",
			wantType: VirtualTableClusterNodes,
		},
		{
			name:     "regular table",
			sql:      "SELECT * FROM users",
			wantType: VirtualTableUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				t.Fatalf("expected SELECT statement, got %T", stmt)
			}

			vtType := detectVirtualTable(sel)
			if vtType != tt.wantType {
				t.Errorf("got %d, want %d", vtType, tt.wantType)
			}
		})
	}
}

func TestDetectInformationSchemaTable(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name     string
		sql      string
		wantType InformationSchemaTableType
	}{
		{
			name:     "INFORMATION_SCHEMA.TABLES",
			sql:      "SELECT * FROM INFORMATION_SCHEMA.TABLES",
			wantType: ISTableTables,
		},
		{
			name:     "INFORMATION_SCHEMA.COLUMNS",
			sql:      "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'users'",
			wantType: ISTableColumns,
		},
		{
			name:     "INFORMATION_SCHEMA.SCHEMATA",
			sql:      "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA",
			wantType: ISTableSchemata,
		},
		{
			name:     "INFORMATION_SCHEMA.STATISTICS",
			sql:      "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = 'users'",
			wantType: ISTableStatistics,
		},
		{
			name:     "lowercase information_schema.tables",
			sql:      "SELECT * FROM information_schema.tables",
			wantType: ISTableTables,
		},
		{
			name:     "regular table",
			sql:      "SELECT * FROM users",
			wantType: ISTableUnknown,
		},
		{
			name:     "unsupported IS table",
			sql:      "SELECT * FROM INFORMATION_SCHEMA.PARTITIONS",
			wantType: ISTableUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				t.Fatalf("expected SELECT statement, got %T", stmt)
			}

			isType := detectInformationSchemaTable(sel)
			if isType != tt.wantType {
				t.Errorf("got %d, want %d", isType, tt.wantType)
			}
		})
	}
}

func TestComplexQueries(t *testing.T) {
	pipeline, err := NewPipeline(100, nil)
	if err != nil {
		t.Fatalf("failed to create pipeline: %v", err)
	}

	tests := []struct {
		name             string
		sql              string
		wantType         StatementType
		wantSysVars      []string
		wantVirtualTable VirtualTableType
		wantISTableType  InformationSchemaTableType
	}{
		// UNION queries
		{
			name:     "simple UNION",
			sql:      "SELECT id FROM users UNION SELECT id FROM admins",
			wantType: StatementSelect,
		},
		{
			name:        "UNION with system variable in left",
			sql:         "SELECT @@version UNION SELECT 'test'",
			wantType:    StatementSystemVariable,
			wantSysVars: []string{"VERSION"},
		},
		{
			name:             "UNION with virtual table",
			sql:              "SELECT * FROM MARMOT_CLUSTER_NODES UNION SELECT 1, 'addr', 'ALIVE', 1",
			wantType:         StatementVirtualTable,
			wantVirtualTable: VirtualTableClusterNodes,
		},
		{
			name:            "UNION with INFORMATION_SCHEMA",
			sql:             "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mydb' UNION SELECT 'other'",
			wantType:        StatementInformationSchema,
			wantISTableType: ISTableTables,
		},

		// Subqueries
		{
			name:     "SELECT with subquery",
			sql:      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			wantType: StatementSelect,
		},
		{
			name:     "SELECT with correlated subquery",
			sql:      "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count FROM users u",
			wantType: StatementSelect,
		},

		// Window functions
		{
			name:     "SELECT with window function ROW_NUMBER",
			sql:      "SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as rn FROM users",
			wantType: StatementSelect,
		},
		{
			name:     "SELECT with window function RANK with PARTITION",
			sql:      "SELECT id, category, RANK() OVER (PARTITION BY category ORDER BY price DESC) as rnk FROM products",
			wantType: StatementSelect,
		},
		{
			name:     "SELECT with multiple window functions",
			sql:      "SELECT id, SUM(amount) OVER (ORDER BY created_at) as running_total, AVG(amount) OVER (PARTITION BY category) as cat_avg FROM orders",
			wantType: StatementSelect,
		},

		// CTEs (Common Table Expressions)
		{
			name:     "simple CTE",
			sql:      "WITH active_users AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM active_users",
			wantType: StatementSelect,
		},
		{
			name:     "recursive CTE",
			sql:      "WITH RECURSIVE nums AS (SELECT 1 as n UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT * FROM nums",
			wantType: StatementSelect,
		},
		{
			name:     "multiple CTEs",
			sql:      "WITH a AS (SELECT 1 as x), b AS (SELECT 2 as y) SELECT * FROM a, b",
			wantType: StatementSelect,
		},

		// Complex JOINs
		{
			name:     "multi-table JOIN",
			sql:      "SELECT u.name, o.total, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id",
			wantType: StatementSelect,
		},
		{
			name:     "LEFT JOIN with subquery",
			sql:      "SELECT u.*, s.total FROM users u LEFT JOIN (SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id) s ON u.id = s.user_id",
			wantType: StatementSelect,
		},

		// Aggregate queries
		{
			name:     "GROUP BY with HAVING",
			sql:      "SELECT category, COUNT(*) as cnt FROM products GROUP BY category HAVING COUNT(*) > 5",
			wantType: StatementSelect,
		},
		{
			name:     "DISTINCT with ORDER BY",
			sql:      "SELECT DISTINCT category FROM products ORDER BY category LIMIT 10",
			wantType: StatementSelect,
		},

		// Edge cases with system variables in complex queries
		{
			name:        "system variable with alias",
			sql:         "SELECT @@version AS ver, @@sql_mode AS mode",
			wantType:    StatementSystemVariable,
			wantSysVars: []string{"VERSION", "SQL_MODE"},
		},
		{
			name:        "DATABASE() in complex select",
			sql:         "SELECT DATABASE() as db, COUNT(*) as cnt FROM users",
			wantType:    StatementSystemVariable,
			wantSysVars: []string{"DATABASE()"},
		},

		// INFORMATION_SCHEMA complex queries
		// Note: JOIN between IS tables falls back to regular SELECT since our handler
		// only supports simple single-table IS queries
		{
			name:     "INFORMATION_SCHEMA with JOIN (treated as regular SELECT)",
			sql:      "SELECT t.TABLE_NAME, c.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLES t JOIN INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME WHERE t.TABLE_SCHEMA = 'mydb'",
			wantType: StatementSelect,
		},
		{
			name:            "simple INFORMATION_SCHEMA query",
			sql:             "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mydb'",
			wantType:        StatementInformationSchema,
			wantISTableType: ISTableTables,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewContext(tt.sql, nil)
			err := pipeline.Process(ctx)
			if err != nil {
				t.Fatalf("pipeline.Process failed: %v", err)
			}

			if ctx.StatementType != tt.wantType {
				t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
			}

			if len(tt.wantSysVars) > 0 {
				if len(ctx.SystemVarNames) != len(tt.wantSysVars) {
					t.Errorf("SystemVarNames count = %d, want %d: got=%v", len(ctx.SystemVarNames), len(tt.wantSysVars), ctx.SystemVarNames)
				} else {
					for i, v := range ctx.SystemVarNames {
						if v != tt.wantSysVars[i] {
							t.Errorf("SystemVarNames[%d] = %q, want %q", i, v, tt.wantSysVars[i])
						}
					}
				}
			}

			if tt.wantVirtualTable != VirtualTableUnknown {
				if ctx.VirtualTableType != tt.wantVirtualTable {
					t.Errorf("VirtualTableType = %d, want %d", ctx.VirtualTableType, tt.wantVirtualTable)
				}
			}

			if tt.wantISTableType != ISTableUnknown {
				if ctx.ISTableType != tt.wantISTableType {
					t.Errorf("ISTableType = %d, want %d", ctx.ISTableType, tt.wantISTableType)
				}
			}
		})
	}
}

func TestExtractInformationSchemaFilter(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name           string
		sql            string
		wantSchemaName string
		wantTableName  string
		wantColumnName string
	}{
		{
			name:           "simple TABLE_SCHEMA filter",
			sql:            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mydb'",
			wantSchemaName: "mydb",
		},
		{
			name:          "simple TABLE_NAME filter",
			sql:           "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'users'",
			wantTableName: "users",
		},
		{
			name:           "combined schema and table filter",
			sql:            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'users'",
			wantSchemaName: "mydb",
			wantTableName:  "users",
		},
		{
			name:           "SCHEMA_NAME filter (SCHEMATA table)",
			sql:            "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'testdb'",
			wantSchemaName: "testdb",
		},
		{
			name:           "double quoted values",
			sql:            `SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "mydb"`,
			wantSchemaName: "mydb",
		},
		{
			name:           "reverse order comparison",
			sql:            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE 'mydb' = TABLE_SCHEMA",
			wantSchemaName: "mydb",
		},
		{
			name: "no WHERE clause",
			sql:  "SELECT * FROM INFORMATION_SCHEMA.TABLES",
		},
		{
			name:           "complex query with multiple conditions",
			sql:            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'users' AND COLUMN_NAME = 'id'",
			wantSchemaName: "mydb",
			wantTableName:  "users",
			wantColumnName: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			sel, ok := stmt.(*sqlparser.Select)
			if !ok {
				t.Fatalf("expected SELECT statement")
			}

			filter := extractInformationSchemaFilter(sel)

			if filter.SchemaName != tt.wantSchemaName {
				t.Errorf("SchemaName = %q, want %q", filter.SchemaName, tt.wantSchemaName)
			}
			if filter.TableName != tt.wantTableName {
				t.Errorf("TableName = %q, want %q", filter.TableName, tt.wantTableName)
			}
			if filter.ColumnName != tt.wantColumnName {
				t.Errorf("ColumnName = %q, want %q", filter.ColumnName, tt.wantColumnName)
			}
		})
	}
}

func TestExplainTableStatement(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tests := []struct {
		name          string
		sql           string
		wantType      StatementType
		wantTableName string
		wantDatabase  string
	}{
		{
			name:          "EXPLAIN users",
			sql:           "EXPLAIN users",
			wantType:      StatementShowColumns,
			wantTableName: "users",
		},
		{
			name:          "EXPLAIN mydb.users",
			sql:           "EXPLAIN mydb.users",
			wantType:      StatementShowColumns,
			wantTableName: "users",
			wantDatabase:  "mydb",
		},
		{
			name:          "EXPLAIN products",
			sql:           "EXPLAIN products",
			wantType:      StatementShowColumns,
			wantTableName: "products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			ctx := &QueryContext{}
			classifyStatement(ctx, stmt)
			extractMetadata(ctx, stmt)

			if ctx.StatementType != tt.wantType {
				t.Errorf("StatementType = %d, want %d", ctx.StatementType, tt.wantType)
			}
			if ctx.TableName != tt.wantTableName {
				t.Errorf("TableName = %q, want %q", ctx.TableName, tt.wantTableName)
			}
			if ctx.Database != tt.wantDatabase {
				t.Errorf("Database = %q, want %q", ctx.Database, tt.wantDatabase)
			}
		})
	}
}
