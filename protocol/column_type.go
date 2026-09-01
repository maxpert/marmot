package protocol

import (
	"strings"
	"time"
)

// MySQL protocol column type codes used for result set metadata.
// See https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
const (
	ColumnTypeTiny      byte = 0x01
	ColumnTypeDouble    byte = 0x05
	ColumnTypeLongLong  byte = 0x08
	ColumnTypeDateTime  byte = 0x0C
	ColumnTypeVarString byte = 0xFD
)

// InferColumnTypes derives one MySQL column type per column from the values a
// result set actually holds.
//
// SQLite is dynamically typed: a storage class belongs to a value, not to a
// column, so one column can hold an integer in one row and text in the next.
// The MySQL protocol declares a single type per column, which clients use to
// decode. Lenient clients coerce whatever they are given, but strict ones
// (sqlx, for instance) refuse to decode an integer out of a column declared as
// text, so declaring everything VAR_STRING makes those clients fail on values
// that are perfectly valid.
//
// A concrete type is claimed only when every non-NULL value in the column
// agrees on it. Mixed columns and all-NULL columns stay VAR_STRING, which every
// client can decode and which writeBinaryValue renders as text, so a column
// that cannot be typed confidently is never encoded as something it is not.
func InferColumnTypes(rows [][]interface{}, numCols int) []byte {
	types := make([]byte, numCols)
	// -1 marks "no non-NULL value seen yet" and lets the first value win
	// without confusing an unseen column with one that resolved to VAR_STRING.
	resolved := make([]bool, numCols)

	for _, row := range rows {
		for i := 0; i < numCols && i < len(row); i++ {
			if types[i] == ColumnTypeVarString && resolved[i] {
				continue // already mixed or textual; nothing can narrow it
			}
			val := row[i]
			if val == nil {
				continue // NULL carries no type information
			}
			t := columnTypeOf(val)
			switch {
			case !resolved[i]:
				types[i] = t
				resolved[i] = true
			case types[i] != t:
				// Storage classes disagree across rows; only text can
				// represent every value in this column faithfully.
				types[i] = ColumnTypeVarString
			}
		}
	}

	for i := range types {
		if !resolved[i] {
			types[i] = ColumnTypeVarString
		}
	}
	return types
}

// columnTypeOf maps the Go value produced by the SQLite driver to the MySQL
// type that encodes it without loss. Anything unrecognised is text, which
// writeBinaryValue formats via fmt and every client can read.
func columnTypeOf(val interface{}) byte {
	switch val.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		// SQLite integers are 64-bit regardless of the declared column width.
		return ColumnTypeLongLong
	case float32, float64:
		return ColumnTypeDouble
	case bool:
		return ColumnTypeTiny
	case time.Time:
		return ColumnTypeDateTime
	case []byte:
		// The driver yields []byte for both TEXT and BLOB storage classes, and
		// MySQL distinguishes them by charset rather than type code, so bytes
		// cannot be typed as binary here without mislabelling text.
		return ColumnTypeVarString
	default:
		return ColumnTypeVarString
	}
}

// ColumnTypeForDeclType maps a SQLite declared column type to the MySQL type
// code that best represents it, for use where no value is available yet (the
// COM_STMT_PREPARE response, which describes a statement before it runs).
//
// The substring tests are SQLite's own type affinity rules, which are defined
// in terms of substrings of the declared type rather than a fixed vocabulary,
// so "BIGINT", "smallint", and "INT UNSIGNED" all land on INTEGER affinity.
// See https://sqlite.org/datatype3.html#determination_of_column_affinity.
// An empty declared type means an expression, where only the value can say.
func ColumnTypeForDeclType(declType string) byte {
	d := strings.ToUpper(declType)
	switch {
	case d == "":
		return ColumnTypeVarString
	case strings.Contains(d, "INT"):
		return ColumnTypeLongLong
	case strings.Contains(d, "CHAR"), strings.Contains(d, "CLOB"), strings.Contains(d, "TEXT"):
		return ColumnTypeVarString
	case strings.Contains(d, "BLOB"):
		return ColumnTypeVarString
	case strings.Contains(d, "REAL"), strings.Contains(d, "FLOA"), strings.Contains(d, "DOUB"):
		return ColumnTypeDouble
	case strings.Contains(d, "DATE"), strings.Contains(d, "TIME"):
		// Not part of SQLite affinity, which has no temporal class, but the
		// driver hands these back as time.Time and MySQL clients expect a
		// temporal type code.
		return ColumnTypeDateTime
	case strings.Contains(d, "BOOL"):
		return ColumnTypeTiny
	default:
		return ColumnTypeVarString
	}
}
