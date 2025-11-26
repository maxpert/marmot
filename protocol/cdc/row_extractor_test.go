package cdc

import (
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestExtractRowData_Insert(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string // Expected columns in new_values
		wantErr  bool
	}{
		{
			name:     "simple insert",
			sql:      "INSERT INTO users (id, name, email) VALUES (123, 'John', 'john@example.com')",
			wantCols: []string{"id", "name", "email"},
			wantErr:  false,
		},
		{
			name:     "insert with string primary key",
			sql:      "INSERT INTO products (sku, name, price) VALUES ('ABC123', 'Widget', '19.99')",
			wantCols: []string{"sku", "name", "price"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			ast, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rowData, err := ExtractRowData(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rowData.NewValues) != len(tt.wantCols) {
				t.Errorf("NewValues count = %d, want %d", len(rowData.NewValues), len(tt.wantCols))
			}

			for _, col := range tt.wantCols {
				if _, ok := rowData.NewValues[col]; !ok {
					t.Errorf("Missing column in NewValues: %s", col)
				}
			}

			if len(rowData.OldValues) != 0 {
				t.Errorf("OldValues should be empty for INSERT, got %d entries", len(rowData.OldValues))
			}
		})
	}
}

func TestExtractRowData_Update(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string // Expected columns in new_values (includes SET + WHERE PK columns)
		wantErr  bool
	}{
		{
			name:     "simple update",
			sql:      "UPDATE users SET name = 'Jane', email = 'jane@example.com' WHERE id = 123",
			wantCols: []string{"name", "email", "id"}, // SET columns + PK from WHERE
			wantErr:  false,
		},
		{
			name:     "update single column",
			sql:      "UPDATE products SET price = '29.99' WHERE sku = 'ABC123'",
			wantCols: []string{"price", "sku"}, // SET column + PK from WHERE
			wantErr:  false,
		},
		{
			name:    "update without where",
			sql:     "UPDATE users SET status = 'active'",
			wantErr: true, // Multi-row updates not supported
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			ast, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rowData, err := ExtractRowData(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rowData.NewValues) != len(tt.wantCols) {
				t.Errorf("NewValues count = %d, want %d", len(rowData.NewValues), len(tt.wantCols))
			}

			for _, col := range tt.wantCols {
				if _, ok := rowData.NewValues[col]; !ok {
					t.Errorf("Missing column in NewValues: %s", col)
				}
			}
		})
	}
}

func TestExtractRowData_Delete(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "simple delete",
			sql:     "DELETE FROM users WHERE id = 123",
			wantErr: false,
		},
		{
			name:    "delete with string key",
			sql:     "DELETE FROM products WHERE sku = 'ABC123'",
			wantErr: false,
		},
		{
			name:    "delete without where",
			sql:     "DELETE FROM users",
			wantErr: true, // Multi-row deletes not supported
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			ast, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rowData, err := ExtractRowData(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// For DELETE, new_values should be empty
			if len(rowData.NewValues) != 0 {
				t.Errorf("NewValues should be empty for DELETE, got %d entries", len(rowData.NewValues))
			}
		})
	}
}

func TestExtractRowData_YCSBWorkload(t *testing.T) {
	// Test with actual YCSB-style queries
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "YCSB insert",
			sql:     "INSERT INTO usertable (YCSB_KEY, field0, field1) VALUES ('user123', 'data0', 'data1')",
			wantErr: false,
		},
		{
			name:    "YCSB update",
			sql:     "UPDATE usertable SET field0 = 'newdata' WHERE YCSB_KEY = 'user456'",
			wantErr: false,
		},
		{
			name:    "YCSB read (should fail - not DML)",
			sql:     "SELECT * FROM usertable WHERE YCSB_KEY = 'user789'",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			ast, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			_, err = ExtractRowData(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
		})
	}
}

func TestExtractAllRowData_MultiRowInsert(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols []string // Expected columns in each row's new_values
		wantErr  bool
	}{
		{
			name:     "single row insert",
			sql:      "INSERT INTO users (id, name) VALUES (1, 'Alice')",
			wantRows: 1,
			wantCols: []string{"id", "name"},
			wantErr:  false,
		},
		{
			name:     "two row insert",
			sql:      "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
			wantRows: 2,
			wantCols: []string{"id", "name"},
			wantErr:  false,
		},
		{
			name:     "three row insert",
			sql:      "INSERT INTO products (sku, name, price) VALUES ('A1', 'Widget', '9.99'), ('B2', 'Gadget', '19.99'), ('C3', 'Thing', '29.99')",
			wantRows: 3,
			wantCols: []string{"sku", "name", "price"},
			wantErr:  false,
		},
		{
			name:     "YCSB batch insert",
			sql:      "INSERT INTO usertable (YCSB_KEY, field0, field1, field2) VALUES ('user1', 'a', 'b', 'c'), ('user2', 'd', 'e', 'f'), ('user3', 'g', 'h', 'i'), ('user4', 'j', 'k', 'l')",
			wantRows: 4,
			wantCols: []string{"YCSB_KEY", "field0", "field1", "field2"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			ast, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rows, err := ExtractAllRowData(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractAllRowData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rows) != tt.wantRows {
				t.Errorf("ExtractAllRowData() got %d rows, want %d", len(rows), tt.wantRows)
				return
			}

			// Verify each row has the expected columns
			for i, row := range rows {
				if len(row.NewValues) != len(tt.wantCols) {
					t.Errorf("Row %d: NewValues count = %d, want %d", i, len(row.NewValues), len(tt.wantCols))
				}
				for _, col := range tt.wantCols {
					if _, ok := row.NewValues[col]; !ok {
						t.Errorf("Row %d: Missing column in NewValues: %s", i, col)
					}
				}
				// INSERT should have empty OldValues
				if len(row.OldValues) != 0 {
					t.Errorf("Row %d: OldValues should be empty for INSERT, got %d entries", i, len(row.OldValues))
				}
			}
		})
	}
}

func TestExtractAllRowData_UpdateDelete(t *testing.T) {
	// UPDATE and DELETE should return single-element slice
	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantErr  bool
	}{
		{
			name:     "update returns single row",
			sql:      "UPDATE users SET name = 'Jane' WHERE id = 1",
			wantRows: 1,
			wantErr:  false,
		},
		{
			name:     "delete returns single row",
			sql:      "DELETE FROM users WHERE id = 1",
			wantRows: 1,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			ast, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rows, err := ExtractAllRowData(ast)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractAllRowData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rows) != tt.wantRows {
				t.Errorf("ExtractAllRowData() got %d rows, want %d", len(rows), tt.wantRows)
			}
		})
	}
}
