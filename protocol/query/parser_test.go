package query

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

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
