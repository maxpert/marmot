// Package intmarker encodes a MySQL integer column's declared width into the
// SQLite DDL text, and reads it back.
//
// SQLite has one integer type, so the transpiler collapses TINYINT..INT to
// INTEGER and the declared width is lost. It cannot be kept by storing the
// MySQL type word instead: only the exact token INTEGER makes a PRIMARY KEY an
// alias of the rowid, and "INT PRIMARY KEY" does not, which would make
// LAST_INSERT_ID() report an unrelated internal rowid. So the width travels as
// a comment beside the type, which SQLite preserves verbatim in
// sqlite_master.sql and normalises away everywhere else.
//
// The grammar is /*M:<bits>[u][a][:<floor>]*/ - bits in {8,16,24,32}, u for
// UNSIGNED, a for an explicitly declared AUTO_INCREMENT, and an optional
// decimal <floor> - the base floor a client's AUTO_INCREMENT=N table option
// declared (N-1), present only when non-zero. It is deliberately NOT MySQL's
// /*! ... */ executable-comment syntax: that syntax asks another MySQL to
// execute the contents, which is the opposite of what this is for.
package intmarker

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	markerOpen  = "/*M:"
	markerClose = "*/"
)

// Attributes are what a marker records about one column.
type Attributes struct {
	// Bits is the declared width: 8, 16, 24 or 32. Zero means "no marker",
	// which is the 64-bit path and every table created before markers existed.
	Bits int
	// Unsigned mirrors the MySQL UNSIGNED modifier.
	Unsigned bool
	// ExplicitAutoInc records that the column was declared AUTO_INCREMENT,
	// as opposed to merely being a narrow integer.
	ExplicitAutoInc bool
	// AutoIncFloor is the base floor a client's AUTO_INCREMENT=N table option
	// declared: N-1. Zero means the client did not declare one. Only ever set
	// on the marker of the column ExplicitAutoInc names.
	AutoIncFloor uint64
}

// Marked reports whether a marker was present.
func (a Attributes) Marked() bool { return a.Bits != 0 }

// WidthMax returns the largest value the declared column can hold.
//
// It is the single source of this number: the range allocator, the claim
// precondition and the participant that validates a claim must all derive the
// ceiling from the same code, or a claim accepted by one is rejected by
// another. Zero bits means no marker and therefore no narrow ceiling; the
// caller uses its existing 64-bit path.
func (a Attributes) WidthMax() uint64 {
	if a.Bits == 0 {
		return 0
	}
	if a.Unsigned {
		return uint64(1)<<uint(a.Bits) - 1
	}
	return uint64(1)<<uint(a.Bits-1) - 1
}

// BitsForType maps a MySQL integer type word to its marker width. It returns
// ok == false for BIGINT and for anything that is not an integer type: BIGINT
// keeps the existing 64-bit path and is deliberately not marked, so a table
// declared BIGINT has DDL text byte-identical to what it had before markers.
func BitsForType(mysqlType string) (int, bool) {
	switch strings.ToUpper(strings.TrimSpace(mysqlType)) {
	case "TINYINT":
		return 8, true
	case "SMALLINT":
		return 16, true
	case "MEDIUMINT":
		return 24, true
	case "INT", "INTEGER":
		return 32, true
	default:
		return 0, false
	}
}

// Encode renders a marker, or "" when there is nothing to record. A caller that
// gets "" must emit no comment at all rather than an empty one, so unmarked DDL
// stays byte-identical.
func Encode(a Attributes) string {
	if a.Bits == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(markerOpen)
	b.WriteString(strconv.Itoa(a.Bits))
	if a.Unsigned {
		b.WriteByte('u')
	}
	if a.ExplicitAutoInc {
		b.WriteByte('a')
	}
	if a.AutoIncFloor != 0 {
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(a.AutoIncFloor, 10))
	}
	b.WriteString(markerClose)
	return b.String()
}

// parseMarkerBody reads the text between /*M: and */.
func parseMarkerBody(body string) (Attributes, error) {
	var a Attributes
	i := 0
	for i < len(body) && body[i] >= '0' && body[i] <= '9' {
		i++
	}
	if i == 0 {
		return a, fmt.Errorf("marker has no width: %q", body)
	}
	bits, err := strconv.Atoi(body[:i])
	if err != nil {
		return a, fmt.Errorf("marker width %q: %w", body[:i], err)
	}
	switch bits {
	case 8, 16, 24, 32:
		a.Bits = bits
	default:
		return a, fmt.Errorf("marker width %d is not one of 8, 16, 24, 32", bits)
	}
	for i < len(body) {
		switch body[i] {
		case 'u':
			a.Unsigned = true
			i++
		case 'a':
			a.ExplicitAutoInc = true
			i++
		case ':':
			floorStr := body[i+1:]
			floor, err := strconv.ParseUint(floorStr, 10, 64)
			if err != nil {
				return Attributes{}, fmt.Errorf("marker floor %q: %w", floorStr, err)
			}
			a.AutoIncFloor = floor
			i = len(body)
		default:
			return a, fmt.Errorf("marker flag %q is not u, a or :<floor>", body[i])
		}
	}
	return a, nil
}
