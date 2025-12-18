package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodedCapturedRow_RoundtripSerialization(t *testing.T) {
	tests := []struct {
		name string
		row  *EncodedCapturedRow
	}{
		{
			name: "INSERT operation with new values",
			row: &EncodedCapturedRow{
				Table:     "users",
				Op:        uint8(OpTypeInsert),
				IntentKey: "users/id=1",
				NewValues: map[string][]byte{
					"id":   []byte("1"),
					"name": []byte("Alice"),
					"age":  []byte("30"),
				},
			},
		},
		{
			name: "UPDATE operation with old and new values",
			row: &EncodedCapturedRow{
				Table:     "products",
				Op:        uint8(OpTypeUpdate),
				IntentKey: "products/id=42",
				OldValues: map[string][]byte{
					"id":    []byte("42"),
					"price": []byte("99.99"),
					"stock": []byte("10"),
				},
				NewValues: map[string][]byte{
					"id":    []byte("42"),
					"price": []byte("89.99"),
					"stock": []byte("15"),
				},
			},
		},
		{
			name: "DELETE operation with old values only",
			row: &EncodedCapturedRow{
				Table:     "orders",
				Op:        uint8(OpTypeDelete),
				IntentKey: "orders/id=100",
				OldValues: map[string][]byte{
					"id":     []byte("100"),
					"total":  []byte("250.00"),
					"status": []byte("completed"),
				},
			},
		},
		{
			name: "Empty values",
			row: &EncodedCapturedRow{
				Table:     "empty_table",
				Op:        uint8(OpTypeInsert),
				IntentKey: "empty_table/id=1",
			},
		},
		{
			name: "Nil maps",
			row: &EncodedCapturedRow{
				Table:     "nil_table",
				Op:        uint8(OpTypeDelete),
				IntentKey: "nil_table/id=1",
				OldValues: nil,
				NewValues: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeRow(tt.row)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := DecodeRow(encoded)
			require.NoError(t, err)
			require.NotNil(t, decoded)

			assert.Equal(t, tt.row.Table, decoded.Table)
			assert.Equal(t, tt.row.Op, decoded.Op)
			assert.Equal(t, tt.row.IntentKey, decoded.IntentKey)
			assert.Equal(t, tt.row.OldValues, decoded.OldValues)
			assert.Equal(t, tt.row.NewValues, decoded.NewValues)
		})
	}
}

func TestEncodedCapturedRow_AllOpTypes(t *testing.T) {
	tests := []struct {
		name string
		op   OpType
	}{
		{"OpTypeInsert", OpTypeInsert},
		{"OpTypeReplace", OpTypeReplace},
		{"OpTypeUpdate", OpTypeUpdate},
		{"OpTypeDelete", OpTypeDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := &EncodedCapturedRow{
				Table:     "test_table",
				Op:        uint8(tt.op),
				IntentKey: "test_table/id=1",
				NewValues: map[string][]byte{
					"id": []byte("1"),
				},
			}

			encoded, err := EncodeRow(row)
			require.NoError(t, err)

			decoded, err := DecodeRow(encoded)
			require.NoError(t, err)

			assert.Equal(t, uint8(tt.op), decoded.Op)
		})
	}
}

func TestEncodedCapturedRow_InvalidInput(t *testing.T) {
	t.Run("Decode nil data returns error", func(t *testing.T) {
		_, err := DecodeRow(nil)
		assert.Error(t, err)
	})

	t.Run("Decode empty data returns error", func(t *testing.T) {
		_, err := DecodeRow([]byte{})
		assert.Error(t, err)
	})

	t.Run("Decode invalid msgpack data returns error", func(t *testing.T) {
		_, err := DecodeRow([]byte{0xFF, 0xFF, 0xFF})
		assert.Error(t, err)
	})
}

func TestEncodedCapturedRow_LargeValues(t *testing.T) {
	largeValue := make([]byte, 10000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	row := &EncodedCapturedRow{
		Table:     "large_table",
		Op:        uint8(OpTypeInsert),
		IntentKey: "large_table/id=1",
		NewValues: map[string][]byte{
			"id":   []byte("1"),
			"blob": largeValue,
		},
	}

	encoded, err := EncodeRow(row)
	require.NoError(t, err)

	decoded, err := DecodeRow(encoded)
	require.NoError(t, err)

	assert.Equal(t, largeValue, decoded.NewValues["blob"])
}

func BenchmarkEncodeRow(b *testing.B) {
	row := &EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeUpdate),
		IntentKey: "users/id=1",
		OldValues: map[string][]byte{
			"id":    []byte("1"),
			"name":  []byte("Alice"),
			"email": []byte("alice@example.com"),
			"age":   []byte("30"),
		},
		NewValues: map[string][]byte{
			"id":    []byte("1"),
			"name":  []byte("Alice Smith"),
			"email": []byte("alice.smith@example.com"),
			"age":   []byte("31"),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := EncodeRow(row)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeRow(b *testing.B) {
	row := &EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeUpdate),
		IntentKey: "users/id=1",
		OldValues: map[string][]byte{
			"id":    []byte("1"),
			"name":  []byte("Alice"),
			"email": []byte("alice@example.com"),
			"age":   []byte("30"),
		},
		NewValues: map[string][]byte{
			"id":    []byte("1"),
			"name":  []byte("Alice Smith"),
			"email": []byte("alice.smith@example.com"),
			"age":   []byte("31"),
		},
	}

	encoded, err := EncodeRow(row)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecodeRow(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeDecodeRow(b *testing.B) {
	row := &EncodedCapturedRow{
		Table:     "users",
		Op:        uint8(OpTypeUpdate),
		IntentKey: "users/id=1",
		OldValues: map[string][]byte{
			"id":    []byte("1"),
			"name":  []byte("Alice"),
			"email": []byte("alice@example.com"),
			"age":   []byte("30"),
		},
		NewValues: map[string][]byte{
			"id":    []byte("1"),
			"name":  []byte("Alice Smith"),
			"email": []byte("alice.smith@example.com"),
			"age":   []byte("31"),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeRow(row)
		if err != nil {
			b.Fatal(err)
		}
		_, err = DecodeRow(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
