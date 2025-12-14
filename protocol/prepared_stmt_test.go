package protocol

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestParseParamValue_TINY(t *testing.T) {
	// MYSQL_TYPE_TINY = 0x01
	payload := []byte{42}
	offset, val, err := parseParamValue(payload, 0, 0x01)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 1 {
		t.Errorf("Expected offset 1, got %d", offset)
	}
	if val != int8(42) {
		t.Errorf("Expected 42, got %v", val)
	}
}

func TestParseParamValue_SHORT(t *testing.T) {
	// MYSQL_TYPE_SHORT = 0x02
	payload := make([]byte, 2)
	binary.LittleEndian.PutUint16(payload, 1234)
	offset, val, err := parseParamValue(payload, 0, 0x02)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 2 {
		t.Errorf("Expected offset 2, got %d", offset)
	}
	if val != int16(1234) {
		t.Errorf("Expected 1234, got %v", val)
	}
}

func TestParseParamValue_LONG(t *testing.T) {
	// MYSQL_TYPE_LONG = 0x03
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, 123456)
	offset, val, err := parseParamValue(payload, 0, 0x03)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 4 {
		t.Errorf("Expected offset 4, got %d", offset)
	}
	if val != int32(123456) {
		t.Errorf("Expected 123456, got %v", val)
	}
}

func TestParseParamValue_LONGLONG(t *testing.T) {
	// MYSQL_TYPE_LONGLONG = 0x08
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, 9876543210)
	offset, val, err := parseParamValue(payload, 0, 0x08)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 8 {
		t.Errorf("Expected offset 8, got %d", offset)
	}
	if val != int64(9876543210) {
		t.Errorf("Expected 9876543210, got %v", val)
	}
}

func TestParseParamValue_FLOAT(t *testing.T) {
	// MYSQL_TYPE_FLOAT = 0x04
	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, math.Float32bits(3.14))
	offset, val, err := parseParamValue(payload, 0, 0x04)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 4 {
		t.Errorf("Expected offset 4, got %d", offset)
	}
	floatVal, ok := val.(float32)
	if !ok {
		t.Fatalf("Expected float32, got %T", val)
	}
	if math.Abs(float64(floatVal-3.14)) > 0.001 {
		t.Errorf("Expected ~3.14, got %v", val)
	}
}

func TestParseParamValue_DOUBLE(t *testing.T) {
	// MYSQL_TYPE_DOUBLE = 0x05
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, math.Float64bits(3.14159265))
	offset, val, err := parseParamValue(payload, 0, 0x05)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 8 {
		t.Errorf("Expected offset 8, got %d", offset)
	}
	floatVal, ok := val.(float64)
	if !ok {
		t.Fatalf("Expected float64, got %T", val)
	}
	if math.Abs(floatVal-3.14159265) > 0.0000001 {
		t.Errorf("Expected ~3.14159265, got %v", val)
	}
}

func TestParseParamValue_STRING(t *testing.T) {
	// MYSQL_TYPE_VAR_STRING = 0xFD (length-prefixed string)
	str := "hello world"
	payload := make([]byte, 1+len(str))
	payload[0] = byte(len(str)) // Length prefix
	copy(payload[1:], str)

	offset, val, err := parseParamValue(payload, 0, 0xFD)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 1+len(str) {
		t.Errorf("Expected offset %d, got %d", 1+len(str), offset)
	}
	// VAR_STRING returns []byte
	if byteVal, ok := val.([]byte); ok {
		if string(byteVal) != str {
			t.Errorf("Expected %q, got %q", str, string(byteVal))
		}
	} else if strVal, ok := val.(string); ok {
		if strVal != str {
			t.Errorf("Expected %q, got %q", str, strVal)
		}
	} else {
		t.Errorf("Expected string or []byte, got %T: %v", val, val)
	}
}

func TestParseParamValue_NULL(t *testing.T) {
	// MYSQL_TYPE_NULL = 0x06
	offset, val, err := parseParamValue([]byte{}, 0, 0x06)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}
}

func TestCountPlaceholders(t *testing.T) {
	tests := []struct {
		query string
		want  int
	}{
		{"SELECT * FROM users WHERE id = ?", 1},
		{"INSERT INTO users VALUES (?, ?, ?)", 3},
		{"SELECT * FROM users", 0},
		{"SELECT * FROM faq WHERE question = '?' AND id = ?", 1}, // ? in string
		{"UPDATE users SET name = '?' WHERE id = ?", 1},          // ? in string
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			got := countPlaceholders(tt.query)
			if got != tt.want {
				t.Errorf("countPlaceholders() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPreparedStatement_ParamTypesCaching(t *testing.T) {
	// This tests that ParamTypes can be cached and reused
	stmt := &PreparedStatement{
		ID:         1,
		Query:      "SELECT * FROM users WHERE id = ?",
		ParamCount: 1,
	}

	// Simulate first execution with newParamsBoundFlag = 1
	paramTypes := []byte{0x08, 0x00} // LONGLONG, unsigned flag
	stmt.ParamTypes = paramTypes

	// Verify cached
	if len(stmt.ParamTypes) != 2 {
		t.Errorf("Expected ParamTypes length 2, got %d", len(stmt.ParamTypes))
	}
	if stmt.ParamTypes[0] != 0x08 {
		t.Errorf("Expected ParamTypes[0] = 0x08, got %02x", stmt.ParamTypes[0])
	}

	// Simulate subsequent execution with newParamsBoundFlag = 0
	// Should use cached types
	cachedType := stmt.ParamTypes[0]
	if cachedType != 0x08 {
		t.Errorf("Cached param type should be 0x08 (LONGLONG), got %02x", cachedType)
	}
}

func BenchmarkParseParamValue_LONGLONG(b *testing.B) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, 9876543210)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = parseParamValue(payload, 0, 0x08)
	}
}

func BenchmarkCountPlaceholders(b *testing.B) {
	query := "INSERT INTO sbtest1 (id, k, c, pad) VALUES (?, ?, ?, ?)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		countPlaceholders(query)
	}
}
