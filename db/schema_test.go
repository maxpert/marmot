//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"testing"

	"github.com/maxpert/marmot/publisher"
)

func TestColumnSchema_Basics(t *testing.T) {
	tests := []struct {
		name     string
		col      ColumnSchema
		wantName string
		wantType string
		wantPK   bool
	}{
		{
			name: "simple text column",
			col: ColumnSchema{
				Name:     "username",
				Type:     "TEXT",
				Nullable: true,
				IsPK:     false,
				PKOrder:  0,
			},
			wantName: "username",
			wantType: "TEXT",
			wantPK:   false,
		},
		{
			name: "primary key integer column",
			col: ColumnSchema{
				Name:     "id",
				Type:     "INTEGER",
				Nullable: false,
				IsPK:     true,
				PKOrder:  1,
			},
			wantName: "id",
			wantType: "INTEGER",
			wantPK:   true,
		},
		{
			name: "nullable real column",
			col: ColumnSchema{
				Name:     "price",
				Type:     "REAL",
				Nullable: true,
				IsPK:     false,
				PKOrder:  0,
			},
			wantName: "price",
			wantType: "REAL",
			wantPK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.col.Name != tt.wantName {
				t.Errorf("Name = %v, want %v", tt.col.Name, tt.wantName)
			}
			if tt.col.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", tt.col.Type, tt.wantType)
			}
			if tt.col.IsPK != tt.wantPK {
				t.Errorf("IsPK = %v, want %v", tt.col.IsPK, tt.wantPK)
			}
		})
	}
}

func TestTableSchema_ToPublisherSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      *TableSchema
		wantCols    int
		wantPKCount int
		checkCol    int
		wantColName string
		wantColIsPK bool
	}{
		{
			name: "simple table with single PK",
			schema: &TableSchema{
				Columns:     []string{"id", "name", "email"},
				PrimaryKeys: []string{"id"},
				PKIndices:   []int{0},
			},
			wantCols:    3,
			wantPKCount: 1,
			checkCol:    0,
			wantColName: "id",
			wantColIsPK: true,
		},
		{
			name: "table with composite PK",
			schema: &TableSchema{
				Columns:     []string{"user_id", "role_id", "granted_at"},
				PrimaryKeys: []string{"user_id", "role_id"},
				PKIndices:   []int{0, 1},
			},
			wantCols:    3,
			wantPKCount: 2,
			checkCol:    1,
			wantColName: "role_id",
			wantColIsPK: true,
		},
		{
			name: "non-PK column check",
			schema: &TableSchema{
				Columns:     []string{"id", "name", "email"},
				PrimaryKeys: []string{"id"},
				PKIndices:   []int{0},
			},
			wantCols:    3,
			wantPKCount: 1,
			checkCol:    2,
			wantColName: "email",
			wantColIsPK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pubSchema := tt.schema.ToPublisherSchema()

			if len(pubSchema.Columns) != tt.wantCols {
				t.Errorf("ToPublisherSchema() columns count = %v, want %v",
					len(pubSchema.Columns), tt.wantCols)
			}

			pkCount := 0
			for _, col := range pubSchema.Columns {
				if col.IsPK {
					pkCount++
				}
			}
			if pkCount != tt.wantPKCount {
				t.Errorf("ToPublisherSchema() PK count = %v, want %v",
					pkCount, tt.wantPKCount)
			}

			if tt.checkCol >= 0 && tt.checkCol < len(pubSchema.Columns) {
				col := pubSchema.Columns[tt.checkCol]
				if col.Name != tt.wantColName {
					t.Errorf("Column[%d].Name = %v, want %v",
						tt.checkCol, col.Name, tt.wantColName)
				}
				if col.IsPK != tt.wantColIsPK {
					t.Errorf("Column[%d].IsPK = %v, want %v",
						tt.checkCol, col.IsPK, tt.wantColIsPK)
				}
			}
		})
	}
}

func TestTableSchema_ToPublisherSchema_AllColumnsPresent(t *testing.T) {
	schema := &TableSchema{
		Columns:     []string{"id", "name", "email", "age"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	pubSchema := schema.ToPublisherSchema()

	// Verify all columns are present in order
	expectedCols := []string{"id", "name", "email", "age"}
	for i, expected := range expectedCols {
		if i >= len(pubSchema.Columns) {
			t.Fatalf("Missing column %d: %s", i, expected)
		}
		if pubSchema.Columns[i].Name != expected {
			t.Errorf("Column[%d].Name = %v, want %v",
				i, pubSchema.Columns[i].Name, expected)
		}
	}
}

func TestTableSchema_ToPublisherSchema_ColumnInfoDefaults(t *testing.T) {
	schema := &TableSchema{
		Columns:     []string{"id"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	pubSchema := schema.ToPublisherSchema()

	if len(pubSchema.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(pubSchema.Columns))
	}

	col := pubSchema.Columns[0]

	// Type should be empty (not available in lightweight schema)
	if col.Type != "" {
		t.Errorf("Type = %v, want empty string", col.Type)
	}

	// Nullable should default to true
	if !col.Nullable {
		t.Errorf("Nullable = false, want true")
	}
}

func TestTableSchema_GetColumnTypes(t *testing.T) {
	schema := &TableSchema{
		Columns:     []string{"id", "name"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	types := schema.GetColumnTypes()

	// Should return empty map for lightweight schema
	if len(types) != 0 {
		t.Errorf("GetColumnTypes() = %v, want empty map", types)
	}
}

func TestTableSchema_GetAutoIncrementCol(t *testing.T) {
	tests := []struct {
		name   string
		schema *TableSchema
		want   string
	}{
		{
			name: "single INTEGER PK (future support)",
			schema: &TableSchema{
				Columns:     []string{"id", "name"},
				PrimaryKeys: []string{"id"},
				PKIndices:   []int{0},
			},
			want: "", // Returns empty for now
		},
		{
			name: "composite PK",
			schema: &TableSchema{
				Columns:     []string{"user_id", "role_id"},
				PrimaryKeys: []string{"user_id", "role_id"},
				PKIndices:   []int{0, 1},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.schema.GetAutoIncrementCol()
			if got != tt.want {
				t.Errorf("GetAutoIncrementCol() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTableSchema_CalculateSchemaVersion(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		schema    *TableSchema
		wantZero  bool
	}{
		{
			name:      "simple table",
			tableName: "users",
			schema: &TableSchema{
				Columns:     []string{"id", "name"},
				PrimaryKeys: []string{"id"},
				PKIndices:   []int{0},
			},
			wantZero: false,
		},
		{
			name:      "composite PK",
			tableName: "user_roles",
			schema: &TableSchema{
				Columns:     []string{"user_id", "role_id"},
				PrimaryKeys: []string{"user_id", "role_id"},
				PKIndices:   []int{0, 1},
			},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := tt.schema.CalculateSchemaVersion(tt.tableName)

			if tt.wantZero && version != 0 {
				t.Errorf("CalculateSchemaVersion() = %v, want 0", version)
			}
			if !tt.wantZero && version == 0 {
				t.Errorf("CalculateSchemaVersion() = 0, want non-zero")
			}
		})
	}
}

func TestTableSchema_CalculateSchemaVersion_Deterministic(t *testing.T) {
	schema := &TableSchema{
		Columns:     []string{"id", "name", "email"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	// Calculate version multiple times
	v1 := schema.CalculateSchemaVersion("users")
	v2 := schema.CalculateSchemaVersion("users")
	v3 := schema.CalculateSchemaVersion("users")

	if v1 != v2 || v2 != v3 {
		t.Errorf("CalculateSchemaVersion() not deterministic: v1=%v, v2=%v, v3=%v",
			v1, v2, v3)
	}
}

func TestTableSchema_CalculateSchemaVersion_PKOrderIndependent(t *testing.T) {
	// Same PKs, different order in PrimaryKeys slice
	schema1 := &TableSchema{
		Columns:     []string{"user_id", "role_id"},
		PrimaryKeys: []string{"user_id", "role_id"},
		PKIndices:   []int{0, 1},
	}

	schema2 := &TableSchema{
		Columns:     []string{"user_id", "role_id"},
		PrimaryKeys: []string{"role_id", "user_id"},
		PKIndices:   []int{1, 0},
	}

	v1 := schema1.CalculateSchemaVersion("user_roles")
	v2 := schema2.CalculateSchemaVersion("user_roles")

	// Should be same because PKs are sorted before hashing
	if v1 != v2 {
		t.Errorf("CalculateSchemaVersion() not PK-order independent: v1=%v, v2=%v",
			v1, v2)
	}
}

func TestTableSchema_CalculateSchemaVersion_DifferentTables(t *testing.T) {
	schema := &TableSchema{
		Columns:     []string{"id", "name"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	v1 := schema.CalculateSchemaVersion("users")
	v2 := schema.CalculateSchemaVersion("products")

	// Different table names should produce different versions
	if v1 == v2 {
		t.Errorf("CalculateSchemaVersion() same for different tables: v1=%v, v2=%v",
			v1, v2)
	}
}

func TestTableSchema_CalculateSchemaVersion_DifferentPKs(t *testing.T) {
	schema1 := &TableSchema{
		Columns:     []string{"id", "name"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	schema2 := &TableSchema{
		Columns:     []string{"id", "name"},
		PrimaryKeys: []string{"name"},
		PKIndices:   []int{1},
	}

	v1 := schema1.CalculateSchemaVersion("users")
	v2 := schema2.CalculateSchemaVersion("users")

	// Different PKs should produce different versions
	if v1 == v2 {
		t.Errorf("CalculateSchemaVersion() same for different PKs: v1=%v, v2=%v",
			v1, v2)
	}
}

func TestTableSchema_ToPublisherSchema_Integration(t *testing.T) {
	// Create a realistic schema
	schema := &TableSchema{
		Columns:     []string{"id", "username", "email", "created_at"},
		PrimaryKeys: []string{"id"},
		PKIndices:   []int{0},
	}

	pubSchema := schema.ToPublisherSchema()

	// Verify structure matches publisher.TableSchema
	var _ publisher.TableSchema = pubSchema

	// Verify columns
	expectedColumns := map[string]bool{
		"id":         true,
		"username":   true,
		"email":      true,
		"created_at": true,
	}

	for _, col := range pubSchema.Columns {
		if !expectedColumns[col.Name] {
			t.Errorf("Unexpected column: %s", col.Name)
		}
		delete(expectedColumns, col.Name)
	}

	if len(expectedColumns) > 0 {
		t.Errorf("Missing columns: %v", expectedColumns)
	}

	// Verify PK marking
	if !pubSchema.Columns[0].IsPK {
		t.Errorf("First column (id) should be marked as PK")
	}
	for i := 1; i < len(pubSchema.Columns); i++ {
		if pubSchema.Columns[i].IsPK {
			t.Errorf("Column %d (%s) should not be marked as PK",
				i, pubSchema.Columns[i].Name)
		}
	}
}
