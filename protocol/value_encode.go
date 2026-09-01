package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
)

// mysqlDateTimeLayout is MySQL's textual DATETIME rendering. Go's default
// formatting of a time.Time ("2026-09-01 16:50:35 +0000 UTC") is not parseable
// by MySQL clients, so every text-protocol timestamp goes through this.
const (
	mysqlDateTimeLayout      = "2006-01-02 15:04:05"
	mysqlDateTimeMicroLayout = "2006-01-02 15:04:05.000000"
)

// FormatTextValue renders a value for the MySQL text protocol.
//
// Most values format the same way Go prints them, but time.Time and bool do
// not: MySQL expects "2026-09-01 16:50:35" rather than Go's zone-suffixed form,
// and 1/0 rather than true/false. A client that parses either of those Go
// renderings as a typed value fails.
func FormatTextValue(val interface{}) string {
	switch v := val.(type) {
	case time.Time:
		return formatMySQLTime(v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatMySQLTime renders a timestamp as MySQL does, keeping the fractional
// part only when it carries information.
func formatMySQLTime(t time.Time) string {
	if t.Nanosecond() != 0 {
		return t.Format(mysqlDateTimeMicroLayout)
	}
	return t.Format(mysqlDateTimeLayout)
}

// writeBinaryDateTime encodes a timestamp in the MySQL binary protocol's
// length-prefixed form: 0 bytes for the zero value, 4 for a bare date, 7 with a
// time component, and 11 when microseconds are present.
func writeBinaryDateTime(buf interface{ Write([]byte) (int, error) }, t time.Time) {
	year, month, day := t.Date()
	hour, minute, sec := t.Clock()
	micro := uint32(t.Nanosecond() / 1000)

	switch {
	case year == 0 && month == 1 && day == 1 && hour == 0 && minute == 0 && sec == 0 && micro == 0:
		_, _ = buf.Write([]byte{0})
	case micro != 0:
		out := make([]byte, 12)
		out[0] = 11
		binary.LittleEndian.PutUint16(out[1:3], uint16(year))
		out[3], out[4] = byte(month), byte(day)
		out[5], out[6], out[7] = byte(hour), byte(minute), byte(sec)
		binary.LittleEndian.PutUint32(out[8:12], micro)
		_, _ = buf.Write(out)
	case hour == 0 && minute == 0 && sec == 0:
		out := make([]byte, 5)
		out[0] = 4
		binary.LittleEndian.PutUint16(out[1:3], uint16(year))
		out[3], out[4] = byte(month), byte(day)
		_, _ = buf.Write(out)
	default:
		out := make([]byte, 8)
		out[0] = 7
		binary.LittleEndian.PutUint16(out[1:3], uint16(year))
		out[3], out[4] = byte(month), byte(day)
		out[5], out[6], out[7] = byte(hour), byte(minute), byte(sec)
		_, _ = buf.Write(out)
	}
}
