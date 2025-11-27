package cdc

import (
	"testing"
)

func TestExtractRowDataFromSQLite_Insert(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string
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
		{
			name:     "INSERT OR IGNORE",
			sql:      "INSERT OR IGNORE INTO usertable (YCSB_KEY, field0) VALUES ('user123', 'data0')",
			wantCols: []string{"YCSB_KEY", "field0"},
			wantErr:  false,
		},
		{
			name:     "INSERT OR REPLACE",
			sql:      "INSERT OR REPLACE INTO usertable (YCSB_KEY, field0) VALUES ('user456', 'data1')",
			wantCols: []string{"YCSB_KEY", "field0"},
			wantErr:  false,
		},
		{
			name:    "insert without column list",
			sql:     "INSERT INTO users VALUES (1, 'John')",
			wantErr: true, // Column list required for CDC
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowData, err := ExtractRowDataFromSQL(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowDataFromSQL() error = %v, wantErr %v", err, tt.wantErr)
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

func TestExtractRowDataFromSQLite_Update(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string
		wantErr  bool
	}{
		{
			name:     "simple update",
			sql:      "UPDATE users SET name = 'Jane', email = 'jane@example.com' WHERE id = 123",
			wantCols: []string{"name", "email", "id"},
			wantErr:  false,
		},
		{
			name:     "update single column",
			sql:      "UPDATE products SET price = '29.99' WHERE sku = 'ABC123'",
			wantCols: []string{"price", "sku"},
			wantErr:  false,
		},
		{
			name:    "update without where",
			sql:     "UPDATE users SET status = 'active'",
			wantErr: true, // Multi-row updates not supported
		},
		{
			name:     "update with compound where",
			sql:      "UPDATE orders SET status = 'shipped' WHERE customer_id = 1 AND order_id = 100",
			wantCols: []string{"status", "customer_id", "order_id"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowData, err := ExtractRowDataFromSQL(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowDataFromSQL() error = %v, wantErr %v", err, tt.wantErr)
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

func TestExtractRowDataFromSQLite_Delete(t *testing.T) {
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
			rowData, err := ExtractRowDataFromSQL(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowDataFromSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// For DELETE, new_values should be empty
			if len(rowData.NewValues) != 0 {
				t.Errorf("NewValues should be empty for DELETE, got %d entries", len(rowData.NewValues))
			}

			// OldValues should have the PK from WHERE
			if len(rowData.OldValues) == 0 {
				t.Errorf("OldValues should contain PK from WHERE clause")
			}
		})
	}
}

func TestExtractRowDataFromSQLite_YCSBWorkload(t *testing.T) {
	// Test with actual YCSB-style queries that triggered the original issue
	tests := []struct {
		name     string
		sql      string
		wantCols []string
		wantErr  bool
	}{
		{
			name:     "YCSB INSERT OR IGNORE",
			sql:      "INSERT OR IGNORE INTO usertable (YCSB_KEY, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES ('user12345', 'val0', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9')",
			wantCols: []string{"YCSB_KEY", "field0", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9"},
			wantErr:  false,
		},
		{
			name:     "YCSB update",
			sql:      "UPDATE usertable SET field0 = 'newdata' WHERE YCSB_KEY = 'user456'",
			wantCols: []string{"field0", "YCSB_KEY"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowData, err := ExtractRowDataFromSQL(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractRowDataFromSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rowData.NewValues) != len(tt.wantCols) {
				t.Errorf("NewValues count = %d, want %d", len(rowData.NewValues), len(tt.wantCols))
				for k := range rowData.NewValues {
					t.Logf("  Got column: %s", k)
				}
			}

			for _, col := range tt.wantCols {
				if _, ok := rowData.NewValues[col]; !ok {
					t.Errorf("Missing column in NewValues: %s", col)
				}
			}
		})
	}
}

func TestExtractRowDataFromSQLite_UnsupportedStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "SELECT not supported",
			sql:  "SELECT * FROM users WHERE id = 1",
		},
		{
			name: "CREATE TABLE not supported",
			sql:  "CREATE TABLE test (id INTEGER PRIMARY KEY)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ExtractRowDataFromSQL(tt.sql)
			if err == nil {
				t.Errorf("Expected error for unsupported statement, got nil")
			}
		})
	}
}

func TestExtractAllRowDataFromSQLite_MultiRowInsert(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantRows int
		wantCols []string
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
			name:     "three row INSERT OR IGNORE",
			sql:      "INSERT OR IGNORE INTO products (sku, name, price) VALUES ('A1', 'Widget', '9.99'), ('B2', 'Gadget', '19.99'), ('C3', 'Thing', '29.99')",
			wantRows: 3,
			wantCols: []string{"sku", "name", "price"},
			wantErr:  false,
		},
		{
			name:     "four row INSERT OR REPLACE",
			sql:      "INSERT OR REPLACE INTO usertable (YCSB_KEY, field0, field1) VALUES ('user1', 'a', 'b'), ('user2', 'c', 'd'), ('user3', 'e', 'f'), ('user4', 'g', 'h')",
			wantRows: 4,
			wantCols: []string{"YCSB_KEY", "field0", "field1"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQLiteStatement(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rows, err := ExtractAllRowDataFromSQLite(stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractAllRowDataFromSQLite() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rows) != tt.wantRows {
				t.Errorf("ExtractAllRowDataFromSQLite() got %d rows, want %d", len(rows), tt.wantRows)
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

func TestExtractAllRowDataFromSQLite_UpdateDelete(t *testing.T) {
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
			stmt, err := ParseSQLiteStatement(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rows, err := ExtractAllRowDataFromSQLite(stmt)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractAllRowDataFromSQLite() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if len(rows) != tt.wantRows {
				t.Errorf("ExtractAllRowDataFromSQLite() got %d rows, want %d", len(rows), tt.wantRows)
			}
		})
	}
}
