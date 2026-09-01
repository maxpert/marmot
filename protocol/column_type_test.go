package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInferColumnTypes(t *testing.T) {
	ts := time.Date(2026, 9, 1, 16, 50, 35, 0, time.UTC)

	cases := []struct {
		name string
		rows [][]interface{}
		cols int
		want []byte
	}{
		{
			name: "integers become BIGINT",
			rows: [][]interface{}{{int64(1)}, {int64(2)}},
			cols: 1,
			want: []byte{ColumnTypeLongLong},
		},
		{
			name: "NULLs carry no type and are skipped",
			rows: [][]interface{}{{nil}, {int64(7)}, {nil}},
			cols: 1,
			want: []byte{ColumnTypeLongLong},
		},
		{
			name: "all-NULL column falls back to text",
			rows: [][]interface{}{{nil}, {nil}},
			cols: 1,
			want: []byte{ColumnTypeVarString},
		},
		{
			name: "no rows falls back to text",
			rows: nil,
			cols: 2,
			want: []byte{ColumnTypeVarString, ColumnTypeVarString},
		},
		{
			name: "mixed storage classes fall back to text",
			rows: [][]interface{}{{int64(1)}, {"two"}},
			cols: 1,
			want: []byte{ColumnTypeVarString},
		},
		{
			name: "text after mixing stays text",
			rows: [][]interface{}{{"a"}, {int64(1)}, {int64(2)}},
			cols: 1,
			want: []byte{ColumnTypeVarString},
		},
		{
			name: "floats, bools and times",
			rows: [][]interface{}{{1.5, true, ts}},
			cols: 3,
			want: []byte{ColumnTypeDouble, ColumnTypeTiny, ColumnTypeDateTime},
		},
		{
			name: "bytes stay text because TEXT and BLOB are indistinguishable",
			rows: [][]interface{}{{[]byte("hello")}},
			cols: 1,
			want: []byte{ColumnTypeVarString},
		},
		{
			name: "columns are inferred independently",
			rows: [][]interface{}{{int64(1), "a"}, {int64(2), "b"}},
			cols: 2,
			want: []byte{ColumnTypeLongLong, ColumnTypeVarString},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, InferColumnTypes(tc.rows, tc.cols))
		})
	}
}

// TestInferColumnTypesShortRow guards against a ragged row panicking the writer.
func TestInferColumnTypesShortRow(t *testing.T) {
	rows := [][]interface{}{{int64(1)}}
	require.Equal(t,
		[]byte{ColumnTypeLongLong, ColumnTypeVarString},
		InferColumnTypes(rows, 2))
}

func TestColumnTypeForDeclType(t *testing.T) {
	cases := map[string]byte{
		"":                 ColumnTypeVarString,
		"INTEGER":          ColumnTypeLongLong,
		"smallint":         ColumnTypeLongLong,
		"BIGINT":           ColumnTypeLongLong,
		"INT UNSIGNED":     ColumnTypeLongLong,
		"varchar(255)":     ColumnTypeVarString,
		"TEXT":             ColumnTypeVarString,
		"CLOB":             ColumnTypeVarString,
		"blob":             ColumnTypeVarString,
		"REAL":             ColumnTypeDouble,
		"DOUBLE PRECISION": ColumnTypeDouble,
		"FLOAT":            ColumnTypeDouble,
		"datetime":         ColumnTypeDateTime,
		"TIMESTAMP":        ColumnTypeDateTime,
		"BOOLEAN":          ColumnTypeTiny,
		"NUMERIC":          ColumnTypeVarString,
	}
	for decl, want := range cases {
		require.Equalf(t, want, ColumnTypeForDeclType(decl), "decltype %q", decl)
	}
}
