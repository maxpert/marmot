package protocol

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatTextValue(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
		want string
	}{
		{"time renders as MySQL datetime", time.Date(2026, 9, 1, 16, 50, 35, 0, time.UTC), "2026-09-01 16:50:35"},
		{"time keeps microseconds when present", time.Date(2026, 9, 1, 16, 50, 35, 123456000, time.UTC), "2026-09-01 16:50:35.123456"},
		{"true is 1", true, "1"},
		{"false is 0", false, "0"},
		{"string passes through", "hello", "hello"},
		{"bytes become text", []byte("bytes"), "bytes"},
		{"integers format normally", int64(42), "42"},
		{"floats format normally", 1.5, "1.5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FormatTextValue(tc.val))
		})
	}
}

// TestFormatTextValueRejectsGoTimeSyntax pins the defect that motivated this:
// Go's default rendering is not parseable by a MySQL client.
func TestFormatTextValueRejectsGoTimeSyntax(t *testing.T) {
	got := FormatTextValue(time.Date(2026, 9, 1, 16, 50, 35, 0, time.UTC))
	require.NotContains(t, got, "UTC")
	require.NotContains(t, got, "+0000")
}

func TestWriteBinaryDateTime(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		want []byte
	}{
		{
			name: "date and time uses the 7 byte form",
			in:   time.Date(2026, 9, 1, 16, 50, 35, 0, time.UTC),
			want: []byte{7, 0xEA, 0x07, 9, 1, 16, 50, 35},
		},
		{
			name: "midnight uses the 4 byte date form",
			in:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			want: []byte{4, 0xEA, 0x07, 9, 1},
		},
		{
			name: "microseconds use the 11 byte form",
			in:   time.Date(2026, 9, 1, 16, 50, 35, 123456000, time.UTC),
			want: []byte{11, 0xEA, 0x07, 9, 1, 16, 50, 35, 0x40, 0xE2, 0x01, 0x00},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeBinaryDateTime(&buf, tc.in)
			require.Equal(t, tc.want, buf.Bytes())
		})
	}
}

// TestWriteBinaryValueDateTime checks the writer routes temporal column types to
// the binary date encoding rather than the text fallback.
func TestWriteBinaryValueDateTime(t *testing.T) {
	var buf bytes.Buffer
	scratch := make([]byte, 8)
	writeBinaryValue(&buf, scratch, ColumnTypeDateTime, time.Date(2026, 9, 1, 16, 50, 35, 0, time.UTC))
	require.Equal(t, []byte{7, 0xEA, 0x07, 9, 1, 16, 50, 35}, buf.Bytes())
}

func TestWriteBinaryValueBool(t *testing.T) {
	for _, tc := range []struct {
		in   bool
		want byte
	}{{true, 1}, {false, 0}} {
		var buf bytes.Buffer
		writeBinaryValue(&buf, make([]byte, 8), ColumnTypeTiny, tc.in)
		require.Equal(t, []byte{tc.want}, buf.Bytes())
	}
}

func TestColumnCharset(t *testing.T) {
	require.Equal(t, uint16(45), columnCharset(ColumnTypeVarString), "text is advertised with a character collation")
	require.Equal(t, uint16(63), columnCharset(ColumnTypeLongLong), "non-text is advertised as binary")
	require.Equal(t, uint16(63), columnCharset(ColumnTypeDateTime))
}
