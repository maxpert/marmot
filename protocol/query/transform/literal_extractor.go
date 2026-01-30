package transform

import (
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
)

// ExtractLiterals walks the AST and replaces literal values with argument placeholders.
// Returns the extracted values in order of appearance, suitable for parameterized execution.
//
// This solves the binary data problem: SQLite's sqlite3_bind_* functions handle
// binary data correctly, unlike embedded literals in SQL strings.
//
// Example:
//
//	Input:  INSERT INTO t VALUES ('text', 123, 'binary\x00data')
//	Output: INSERT INTO t VALUES (:v1, :v2, :v3)
//	Params: [string("text"), int64(123), string("binary\x00data")]
func ExtractLiterals(stmt sqlparser.Statement) []interface{} {
	if stmt == nil {
		return nil
	}

	var params []interface{}
	counter := 0

	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		lit, ok := cursor.Node().(*sqlparser.Literal)
		if !ok {
			return true
		}

		// Extract value based on literal type
		var value interface{}
		switch lit.Type {
		case sqlparser.StrVal:
			// String literals - use string type for proper TEXT storage in SQLite
			// SQLite's database/sql driver will handle both text and binary data correctly
			value = string(lit.Val)

		case sqlparser.IntVal:
			// Integer literals - parse to int64
			val, err := strconv.ParseInt(lit.Val, 10, 64)
			if err != nil {
				// Fallback to string if parsing fails
				value = lit.Val
			} else {
				value = val
			}

		case sqlparser.FloatVal:
			// Float literals (scientific notation: 1e10, 1.5e-3) - parse to float64
			val, err := strconv.ParseFloat(lit.Val, 64)
			if err != nil {
				// Fallback to string if parsing fails
				value = lit.Val
			} else {
				value = val
			}

		case sqlparser.HexVal:
			// Hex literals (X'deadbeef') - decode to []byte
			decoded, err := lit.HexDecode()
			if err != nil {
				// Fallback to string if decoding fails
				value = lit.Val
			} else {
				value = decoded
			}

		case sqlparser.HexNum:
			// Hex numbers (0xFF) - parse as int64
			val, err := strconv.ParseInt(lit.Val, 0, 64)
			if err != nil {
				// Fallback to string if parsing fails
				value = lit.Val
			} else {
				value = val
			}

		default:
			// Unknown types - try parsing as float (handles DecimalVal = Type 2)
			// Examples: 4.5, 3.14, 0.5
			if val, err := strconv.ParseFloat(lit.Val, 64); err == nil {
				value = val
			} else {
				// Keep as string if not numeric
				value = lit.Val
			}
		}

		// Append value to params
		params = append(params, value)

		// Replace literal with named placeholder :v1, :v2, etc.
		counter++
		placeholder := sqlparser.NewArgument(":v" + strconv.Itoa(counter))
		cursor.Replace(placeholder)

		return true
	}, nil)

	// Return nil if no literals were found
	if len(params) == 0 {
		return nil
	}

	return params
}
