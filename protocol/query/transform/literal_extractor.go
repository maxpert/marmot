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
//
// The AST can also already contain placeholders of its own - e.g. a client's
// own `?` bind marks in a prepared statement, still present as
// *sqlparser.Argument nodes because they were never literal values to begin
// with. order records, for EVERY placeholder in the final statement (both
// those pre-existing ones and the newly-extracted ones) in the same
// left-to-right order they will serialize in, whether that slot's value at
// execution time comes from the caller's own bind values (true) or from
// params (false, consumed in order) - see protocol.MergeExecParams, which
// uses this to interleave the two sources correctly instead of assuming one
// always comes before the other (a literal can appear before, after, or
// between a statement's own placeholders in the source SQL). order is nil
// when the statement has no pre-existing placeholders, matching today's
// wire-params-XOR-extracted-params callers.
func ExtractLiterals(stmt sqlparser.Statement) (params []interface{}, order []bool) {
	if stmt == nil {
		return nil, nil
	}

	var hasArgument bool
	counter := 0

	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		if _, ok := cursor.Node().(*sqlparser.Argument); ok {
			hasArgument = true
			order = append(order, true)
			return true
		}

		lit, ok := cursor.Node().(*sqlparser.Literal)
		if !ok {
			return true
		}
		order = append(order, false)

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
		return nil, nil
	}

	// order is only useful to callers when there is something to interleave:
	// a statement made only of literals (the common non-prepared case) needs
	// no positional merge, so keep returning nil there too.
	if !hasArgument {
		return params, nil
	}

	return params, order
}
