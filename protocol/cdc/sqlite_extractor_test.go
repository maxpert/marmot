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
