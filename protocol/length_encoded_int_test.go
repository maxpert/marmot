package protocol

import (
	"bytes"
	"testing"
)

func TestReadLengthEncodedInt(t *testing.T) {
	tests := []struct {
		name  string
		in    []byte
		want  uint64
		wantN int
	}{
		{"empty", nil, 0, 0},
		{"one byte", []byte{0xFA}, 250, 1},
		{"0xFC two bytes", []byte{0xFC, 0x34, 0x12}, 0x1234, 3},
		{"0xFC truncated", []byte{0xFC, 0x34}, 0, 0},
		{"0xFD three bytes, smallest", []byte{0xFD, 0x00, 0x00, 0x01}, 1 << 16, 4},
		{"0xFD three bytes, largest", []byte{0xFD, 0xFF, 0xFF, 0xFF}, 1<<24 - 1, 4},
		{"0xFD exact slice, nothing after", []byte{0xFD, 0x70, 0x11, 0x01}, 70000, 4},
		{"0xFD followed by data", []byte{0xFD, 0x70, 0x11, 0x01, 0xAA, 0xBB}, 70000, 4},
		{"0xFD truncated", []byte{0xFD, 0x70, 0x11}, 0, 0},
		{"0xFE eight bytes", []byte{0xFE, 1, 0, 0, 0, 0, 0, 0, 0}, 1, 9},
		{"0xFE truncated", []byte{0xFE, 1, 0, 0}, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, n := readLengthEncodedInt(tt.in)
			if got != tt.want || n != tt.wantN {
				t.Errorf("readLengthEncodedInt(% x) = (%d, %d), want (%d, %d)", tt.in, got, n, tt.want, tt.wantN)
			}
		})
	}
}

// A BLOB parameter of 64 KiB..16 MiB is sent with the 3-byte 0xFD length prefix. Reading that prefix
// used to call binary.LittleEndian.Uint32 on a 3-byte slice and panic, killing the server process.
func TestParseParamValue_BLOB_ThreeByteLength(t *testing.T) {
	size := 70000 // > 65535, so the client encodes the length as 0xFD + 3 bytes
	blob := bytes.Repeat([]byte{0xAB}, size)
	payload := append([]byte{0xFD, byte(size), byte(size >> 8), byte(size >> 16)}, blob...)

	offset, val, err := parseParamValue(payload, 0, 0xFC /* MYSQL_TYPE_BLOB */, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if offset != len(payload) {
		t.Errorf("Expected offset %d, got %d", len(payload), offset)
	}
	got, ok := val.([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got %T", val)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("Blob mismatch: got %d bytes, want %d", len(got), len(blob))
	}
}
