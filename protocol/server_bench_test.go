package protocol

import (
	"bytes"
	"testing"
)

func BenchmarkWritePacket(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}
	payload := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writePacket(buf, 0, payload)
	}
}

func BenchmarkWritePacket_Small(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}
	payload := make([]byte, 64)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writePacket(buf, 0, payload)
	}
}

func BenchmarkWriteBinaryResultSet_Small(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "id", Type: 0x08},   // LONGLONG
			{Name: "name", Type: 0xFD}, // VAR_STRING
		},
		Rows: [][]interface{}{
			{int64(1), "test"},
			{int64(2), "data"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writeBinaryResultSet(buf, 0, rs)
	}
}

func BenchmarkWriteBinaryResultSet_Medium(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}

	cols := []ColumnDef{
		{Name: "id", Type: 0x08},     // LONGLONG
		{Name: "name", Type: 0xFD},   // VAR_STRING
		{Name: "value", Type: 0x05},  // DOUBLE
		{Name: "count", Type: 0x03},  // LONG
		{Name: "active", Type: 0x01}, // TINY
	}

	rows := make([][]interface{}, 10)
	for i := range rows {
		rows[i] = []interface{}{
			int64(i),
			"test_value",
			float64(i) * 1.5,
			int32(i * 100),
			int8(1),
		}
	}

	rs := &ResultSet{Columns: cols, Rows: rows}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writeBinaryResultSet(buf, 0, rs)
	}
}

func BenchmarkWriteBinaryResultSet_Large(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}

	cols := make([]ColumnDef, 10)
	for i := range cols {
		cols[i] = ColumnDef{Name: "col", Type: 0x08} // LONGLONG
	}

	rows := make([][]interface{}, 100)
	for i := range rows {
		row := make([]interface{}, 10)
		for j := range row {
			row[j] = int64(i * j)
		}
		rows[i] = row
	}

	rs := &ResultSet{Columns: cols, Rows: rows}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writeBinaryResultSet(buf, 0, rs)
	}
}

func BenchmarkWriteResultSet_Small(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}
	rs := &ResultSet{
		Columns: []ColumnDef{
			{Name: "id", Type: 0x08},
			{Name: "name", Type: 0xFD},
		},
		Rows: [][]interface{}{
			{int64(1), "test"},
			{int64(2), "data"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writeResultSet(buf, 0, rs)
	}
}

func BenchmarkWriteResultSet_Large(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}

	cols := make([]ColumnDef, 10)
	for i := range cols {
		cols[i] = ColumnDef{Name: "col", Type: 0xFD} // VAR_STRING
	}

	rows := make([][]interface{}, 100)
	for i := range rows {
		row := make([]interface{}, 10)
		for j := range row {
			row[j] = "test_value_string"
		}
		rows[i] = row
	}

	rs := &ResultSet{Columns: cols, Rows: rows}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writeResultSet(buf, 0, rs)
	}
}

// BenchmarkWriteBinaryResultSet_MixedTypes tests with various column types
func BenchmarkWriteBinaryResultSet_MixedTypes(b *testing.B) {
	s := &MySQLServer{}
	buf := &bytes.Buffer{}

	cols := []ColumnDef{
		{Name: "tiny", Type: 0x01},     // TINY
		{Name: "short", Type: 0x02},    // SHORT
		{Name: "long", Type: 0x03},     // LONG
		{Name: "longlong", Type: 0x08}, // LONGLONG
		{Name: "float", Type: 0x04},    // FLOAT
		{Name: "double", Type: 0x05},   // DOUBLE
		{Name: "string", Type: 0xFD},   // VAR_STRING
	}

	rows := make([][]interface{}, 50)
	for i := range rows {
		rows[i] = []interface{}{
			int8(i % 127),
			int16(i * 10),
			int32(i * 100),
			int64(i * 1000),
			float32(i) * 1.5,
			float64(i) * 2.5,
			"test_string_value",
		}
	}

	rs := &ResultSet{Columns: cols, Rows: rows}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = s.writeBinaryResultSet(buf, 0, rs)
	}
}
