package intmarker

import "strings"

// Decode reads every marker in one table's sqlite_master.sql text and returns
// them keyed by column name, lowercased.
//
// It must attribute each marker to the column whose definition contains it, so
// it walks the column list rather than searching the whole string: a marker
// sits immediately after its column's type, and a naive scan would hand the
// first marker to the first column whatever the text between them said.
//
// Text that is not DDL structure is skipped rather than parsed: single-quoted
// strings, double-quoted, backticked and bracketed identifiers, line comments,
// and block comments that are not markers. A marker appearing inside any of
// those is not a marker, and a column named like one is a name.
//
// Anything it cannot parse yields no entry rather than an error. The caller's
// fallback for an absent marker is the 64-bit path, which is the behaviour
// every table had before markers existed, so a malformed marker degrades to
// today's behaviour rather than failing a schema load.
func Decode(createSQL string) map[string]Attributes {
	body, ok := columnListBody(createSQL)
	if !ok {
		return nil
	}

	var out map[string]Attributes
	for _, part := range splitTopLevel(body) {
		name, ok := leadingIdentifier(part)
		if !ok {
			continue
		}
		attrs, ok := findMarker(part)
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[string]Attributes)
		}
		out[strings.ToLower(name)] = attrs
	}
	return out
}

// columnListBody returns the text between the parentheses that open the column
// list, skipping anything quoted so a parenthesis inside a table name or a
// comment does not open it.
func columnListBody(createSQL string) (string, bool) {
	i, ok := scanToColumnList(createSQL)
	if !ok {
		return "", false
	}
	depth := 0
	for j := i; j < len(createSQL); j++ {
		if skip, next := skipNonCode(createSQL, j); skip {
			j = next - 1
			continue
		}
		switch createSQL[j] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return createSQL[i+1 : j], true
			}
		}
	}
	return "", false
}

// scanToColumnList finds the opening parenthesis of the column list.
func scanToColumnList(s string) (int, bool) {
	for i := 0; i < len(s); i++ {
		if skip, next := skipNonCode(s, i); skip {
			i = next - 1
			continue
		}
		if s[i] == '(' {
			return i, true
		}
	}
	return 0, false
}

// skipNonCode reports whether position i begins a region that is not DDL
// structure, and where that region ends. A marker is deliberately NOT skipped:
// it is the one block comment this package reads.
func skipNonCode(s string, i int) (bool, int) {
	switch {
	case strings.HasPrefix(s[i:], markerOpen):
		return false, i
	case s[i] == '\'':
		return true, closingQuote(s, i, '\'')
	case s[i] == '"':
		return true, closingQuote(s, i, '"')
	case s[i] == '`':
		return true, closingQuote(s, i, '`')
	case s[i] == '[':
		return true, indexAfter(s, i+1, "]")
	case strings.HasPrefix(s[i:], "--"):
		if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
			return true, i + nl + 1
		}
		return true, len(s)
	case strings.HasPrefix(s[i:], "/*"):
		return true, indexAfter(s, i+2, "*/")
	}
	return false, i
}

// closingQuote finds the end of a quoted region, honouring SQL's doubled-quote
// escape (” inside '...', "" inside "...", “ inside `...`).
func closingQuote(s string, start int, q byte) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] != q {
			continue
		}
		if i+1 < len(s) && s[i+1] == q {
			i++
			continue
		}
		return i + 1
	}
	return len(s)
}

func indexAfter(s string, from int, sep string) int {
	if idx := strings.Index(s[from:], sep); idx >= 0 {
		return from + idx + len(sep)
	}
	return len(s)
}

// splitTopLevel splits a column list on commas that are not inside parentheses
// or any non-code region, so "DEFAULT (a, b)" stays with its column.
func splitTopLevel(body string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(body); i++ {
		if skip, next := skipNonCode(body, i); skip {
			i = next - 1
			continue
		}
		switch body[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, body[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, body[start:])
}

// leadingIdentifier returns a column definition's name, unquoting whichever
// form the DDL used.
func leadingIdentifier(part string) (string, bool) {
	i := 0
	for i < len(part) && isSpace(part[i]) {
		i++
	}
	if i >= len(part) {
		return "", false
	}
	switch part[i] {
	case '`', '"':
		q := part[i]
		end := closingQuote(part, i, q)
		if end <= i+1 {
			return "", false
		}
		inner := part[i+1 : end-1]
		return strings.ReplaceAll(inner, string([]byte{q, q}), string(q)), true
	case '[':
		end := indexAfter(part, i+1, "]")
		if end <= i+1 {
			return "", false
		}
		return part[i+1 : end-1], true
	}
	start := i
	for i < len(part) && !isSpace(part[i]) && part[i] != '(' {
		i++
	}
	if i == start {
		return "", false
	}
	return part[start:i], true
}

// findMarker returns the first marker in one column definition, skipping any
// that appear inside a quoted region.
func findMarker(part string) (Attributes, bool) {
	for i := 0; i < len(part); i++ {
		if strings.HasPrefix(part[i:], markerOpen) {
			end := strings.Index(part[i+len(markerOpen):], markerClose)
			if end < 0 {
				return Attributes{}, false
			}
			body := part[i+len(markerOpen) : i+len(markerOpen)+end]
			attrs, err := parseMarkerBody(body)
			if err != nil {
				return Attributes{}, false
			}
			return attrs, true
		}
		if skip, next := skipNonCode(part, i); skip {
			i = next - 1
		}
	}
	return Attributes{}, false
}

// Strip removes every marker from DDL text, for the paths that hand
// sqlite_master.sql to a client. Markers inside quoted regions are left alone,
// because there they are data.
//
// It also repairs the spacing the removal would otherwise leave: a marker is
// emitted as "INTEGER /*M:32a*/ PRIMARY KEY", so removing just the comment
// leaves a double space, and "INTEGER /*M:8*/," leaves a space before the
// comma. Both are visible to anyone reading SHOW CREATE TABLE.
func Strip(createSQL string) string {
	out := make([]byte, 0, len(createSQL))
	for i := 0; i < len(createSQL); i++ {
		if strings.HasPrefix(createSQL[i:], markerOpen) {
			if end := strings.Index(createSQL[i+len(markerOpen):], markerClose); end >= 0 {
				i = i + len(markerOpen) + end + len(markerClose) - 1
				out = repairSpacing(out, createSQL, i+1)
				continue
			}
		}
		if skip, next := skipNonCode(createSQL, i); skip {
			out = append(out, createSQL[i:next]...)
			i = next - 1
			continue
		}
		out = append(out, createSQL[i])
	}
	return string(out)
}

// repairSpacing removes the one space a deleted marker leaves behind. It drops
// the space that preceded the marker when what follows is a delimiter or more
// whitespace, so exactly one separator survives between the tokens that were
// on either side.
func repairSpacing(out []byte, src string, next int) []byte {
	if len(out) == 0 || !isSpace(out[len(out)-1]) {
		return out
	}
	if next >= len(src) {
		return out[:len(out)-1]
	}
	if isSpace(src[next]) || src[next] == ',' || src[next] == ')' {
		return out[:len(out)-1]
	}
	return out
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
