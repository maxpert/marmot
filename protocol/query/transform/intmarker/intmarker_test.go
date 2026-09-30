package intmarker

import "testing"

// TestEncodeRoundTrip pins the grammar in both directions.
func TestEncodeRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		attrs Attributes
		want  string
	}{
		{"int, signed, auto-increment", Attributes{Bits: 32, ExplicitAutoInc: true}, "/*M:32a*/"},
		// Mutation: drop the Unsigned branch in Encode. This and the next fire,
		// and every unsigned column silently loses half its range.
		{"int, unsigned, auto-increment", Attributes{Bits: 32, Unsigned: true, ExplicitAutoInc: true}, "/*M:32ua*/"},
		{"smallint, unsigned, no auto-increment", Attributes{Bits: 16, Unsigned: true}, "/*M:16u*/"},
		{"tinyint, plain", Attributes{Bits: 8}, "/*M:8*/"},
		{"mediumint, auto-increment", Attributes{Bits: 24, ExplicitAutoInc: true}, "/*M:24a*/"},
		// BIGINT and anything unmarked must render nothing at all, or the DDL
		// text of an existing table stops being byte-identical.
		// Mutation: return "/*M:0*/" for the zero value.
		{"unmarked renders nothing", Attributes{}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Encode(tc.attrs)
			if got != tc.want {
				t.Fatalf("Encode = %q, want %q", got, tc.want)
			}
			if tc.want == "" {
				return
			}
			// Round-trip through a minimal column definition.
			decoded := Decode("CREATE TABLE t (id INTEGER " + got + " PRIMARY KEY)")
			attrs, ok := decoded["id"]
			if !ok {
				t.Fatalf("Decode lost the marker %q entirely", got)
			}
			if attrs != tc.attrs {
				t.Errorf("round trip = %+v, want %+v", attrs, tc.attrs)
			}
		})
	}
}

// TestWidthMax is the single source of the ceiling every other component
// derives its bound from, so the numbers are pinned against MySQL's ranges
// rather than recomputed at each use.
//
// Mutation: use 1<<bits for the signed case; every signed row fires.
func TestWidthMax(t *testing.T) {
	cases := []struct {
		attrs Attributes
		want  uint64
	}{
		{Attributes{Bits: 8}, 127},
		{Attributes{Bits: 8, Unsigned: true}, 255},
		{Attributes{Bits: 16}, 32767},
		{Attributes{Bits: 16, Unsigned: true}, 65535},
		{Attributes{Bits: 24}, 8388607},
		{Attributes{Bits: 24, Unsigned: true}, 16777215},
		{Attributes{Bits: 32}, 2147483647},
		{Attributes{Bits: 32, Unsigned: true}, 4294967295},
		// No marker means no narrow ceiling; the caller keeps its 64-bit path.
		// Mutation: return a non-zero default.
		{Attributes{}, 0},
	}
	for _, tc := range cases {
		if got := tc.attrs.WidthMax(); got != tc.want {
			t.Errorf("WidthMax(%+v) = %d, want %d", tc.attrs, got, tc.want)
		}
	}
}

// TestBitsForType pins that BIGINT is deliberately unmarked.
// Mutation: return 64 for BIGINT; an existing BIGINT table's DDL text changes.
func TestBitsForType(t *testing.T) {
	marked := map[string]int{"TINYINT": 8, "smallint": 16, "MediumInt": 24, "INT": 32, "INTEGER": 32}
	for typ, want := range marked {
		bits, ok := BitsForType(typ)
		if !ok || bits != want {
			t.Errorf("BitsForType(%q) = %d, %v; want %d, true", typ, bits, ok, want)
		}
	}
	for _, typ := range []string{"BIGINT", "bigint", "TEXT", "BLOB", "DECIMAL", ""} {
		if bits, ok := BitsForType(typ); ok {
			t.Errorf("BitsForType(%q) = %d, true; want unmarked", typ, bits)
		}
	}
}

// TestDecodeAttributesMarkersToTheRightColumn is the decoder's whole job. A
// scan that simply walked the markers in order, or searched the whole string,
// would pass a single-column case and corrupt every real table.
func TestDecodeAttributesMarkersToTheRightColumn(t *testing.T) {
	// Mutation: attribute markers positionally (nth marker -> nth column);
	// "name" gains event_id's marker and the assertions below fire.
	const ddl = "CREATE TABLE `events` (\n" +
		"  tenant TEXT NOT NULL,\n" +
		"  name TEXT DEFAULT 'no /*M:8*/ marker here',\n" +
		"  event_id INTEGER /*M:32a*/ PRIMARY KEY,\n" +
		"  qty INTEGER /*M:16u*/ DEFAULT (1 + 2),\n" +
		"  note TEXT\n" +
		")"

	got := Decode(ddl)

	if len(got) != 2 {
		t.Fatalf("decoded %d markers, want 2: %+v", len(got), got)
	}
	if attrs := got["event_id"]; attrs != (Attributes{Bits: 32, ExplicitAutoInc: true}) {
		t.Errorf("event_id = %+v, want 32/signed/auto", attrs)
	}
	// A marker after which a parenthesised DEFAULT follows must still land on
	// its own column, and the paren must not swallow the next column.
	// Mutation: split the column list on every comma regardless of depth.
	if attrs := got["qty"]; attrs != (Attributes{Bits: 16, Unsigned: true}) {
		t.Errorf("qty = %+v, want 16/unsigned/no-auto", attrs)
	}
	// The marker-looking text inside a string literal is data, not a marker.
	// Mutation: drop the string-literal skip in findMarker.
	if _, ok := got["name"]; ok {
		t.Error("a marker inside a string literal was decoded as a marker")
	}
	if _, ok := got["tenant"]; ok {
		t.Error("an unmarked column gained a marker")
	}
}

// TestDecodeEdgeCases covers the shapes that would otherwise be found in
// production rather than in a test.
func TestDecodeEdgeCases(t *testing.T) {
	t.Run("table with no markers decodes to nothing", func(t *testing.T) {
		// Mutation: return a non-nil empty map and have the caller treat
		// "present" as "marked".
		if got := Decode("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); len(got) != 0 {
			t.Errorf("decoded %+v, want nothing", got)
		}
	})

	t.Run("a column named like a marker is a name", func(t *testing.T) {
		got := Decode("CREATE TABLE t (`/*M:32a*/` TEXT, id INTEGER /*M:8u*/ PRIMARY KEY)")
		if attrs := got["id"]; attrs != (Attributes{Bits: 8, Unsigned: true}) {
			t.Errorf("id = %+v, want 8/unsigned", attrs)
		}
		if len(got) != 1 {
			t.Errorf("decoded %d markers, want 1: %+v", len(got), got)
		}
	})

	t.Run("table constraints are not columns", func(t *testing.T) {
		got := Decode("CREATE TABLE t (a INTEGER /*M:24*/, b TEXT, PRIMARY KEY (a, b))")
		if len(got) != 1 {
			t.Errorf("decoded %d markers, want 1: %+v", len(got), got)
		}
	})

	t.Run("a malformed marker degrades to unmarked", func(t *testing.T) {
		// 64 is not a legal width; the column must fall back to the 64-bit
		// path rather than failing the schema load.
		// Mutation: accept any integer width in parseMarkerBody.
		if got := Decode("CREATE TABLE t (id INTEGER /*M:64a*/ PRIMARY KEY)"); len(got) != 0 {
			t.Errorf("decoded %+v from a malformed marker, want nothing", got)
		}
		if got := Decode("CREATE TABLE t (id INTEGER /*M:32z*/ PRIMARY KEY)"); len(got) != 0 {
			t.Errorf("decoded %+v from an unknown flag, want nothing", got)
		}
	})
}

// TestStrip pins what a client sees. SHOW CREATE TABLE hands sqlite_master.sql
// to the client verbatim, so an unstripped marker would appear in every schema
// dump and in any tool that round-trips DDL.
func TestStrip(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			// Mutation: return the input unchanged; the marker leaks.
			name: "marker and its trailing space are removed",
			in:   "CREATE TABLE t (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)",
			want: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
		},
		{
			name: "several markers",
			in:   "CREATE TABLE t (a INTEGER /*M:8*/, b INTEGER /*M:16u*/ DEFAULT (1))",
			want: "CREATE TABLE t (a INTEGER, b INTEGER DEFAULT (1))",
		},
		{
			// Inside a string literal the text is data and must survive.
			// Mutation: strip with a plain string replace.
			name: "marker-shaped text inside a literal survives",
			in:   "CREATE TABLE t (v TEXT DEFAULT 'keep /*M:8*/ me')",
			want: "CREATE TABLE t (v TEXT DEFAULT 'keep /*M:8*/ me')",
		},
		{
			// Unmarked DDL must come back byte-identical, or every existing
			// table's SHOW CREATE output changes.
			// Mutation: unconditionally collapse whitespace.
			name: "unmarked DDL is unchanged",
			in:   "CREATE TABLE t (id INTEGER PRIMARY KEY,  v TEXT)",
			want: "CREATE TABLE t (id INTEGER PRIMARY KEY,  v TEXT)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Strip(tc.in); got != tc.want {
				t.Errorf("Strip =\n %q\nwant\n %q", got, tc.want)
			}
		})
	}
}

// TestAutoIncFloorGrammar pins the ":<floor>" extension to the marker grammar
// that carries AUTO_INCREMENT=N. A marker with no floor must render byte-identical
// to what Encode produced before AutoIncFloor existed, and a floor must
// round-trip through Decode exactly like every other flag.
//
// Mutation: encode the floor even when it is 0, or drop it from parseMarkerBody;
// the "no floor" and "round trip" cases below both fire.
func TestAutoIncFloorGrammar(t *testing.T) {
	cases := []struct {
		name  string
		attrs Attributes
		want  string
	}{
		{"auto-increment, no declared floor, byte-identical to pre-floor grammar",
			Attributes{Bits: 32, ExplicitAutoInc: true}, "/*M:32a*/"},
		{"auto-increment, unsigned, no declared floor, byte-identical",
			Attributes{Bits: 16, Unsigned: true, ExplicitAutoInc: true}, "/*M:16ua*/"},
		{"auto-increment with declared floor",
			Attributes{Bits: 32, ExplicitAutoInc: true, AutoIncFloor: 4999}, "/*M:32a:4999*/"},
		{"unsigned auto-increment with declared floor",
			Attributes{Bits: 8, Unsigned: true, ExplicitAutoInc: true, AutoIncFloor: 200}, "/*M:8ua:200*/"},
		{"plain narrow column, no auto-increment, floor is meaningless and stays off",
			Attributes{Bits: 16}, "/*M:16*/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Encode(tc.attrs)
			if got != tc.want {
				t.Fatalf("Encode = %q, want %q", got, tc.want)
			}
			decoded := Decode("CREATE TABLE t (id INTEGER " + got + " PRIMARY KEY)")
			attrs, ok := decoded["id"]
			if !ok {
				t.Fatalf("Decode lost the marker %q entirely", got)
			}
			if attrs != tc.attrs {
				t.Errorf("round trip = %+v, want %+v", attrs, tc.attrs)
			}
		})
	}
}

// TestDecodeMalformedFloorIsRejected mirrors the existing "malformed marker
// degrades to no marker" contract for the new ":<floor>" suffix: a floor that
// is not a valid decimal number must fail to parse, and Decode's caller-facing
// behaviour is to omit the column entirely rather than propagate a partial
// Attributes.
func TestDecodeMalformedFloorIsRejected(t *testing.T) {
	cases := []string{
		"CREATE TABLE t (id INTEGER /*M:32a:*/ PRIMARY KEY)",     // empty floor
		"CREATE TABLE t (id INTEGER /*M:32a:12x*/ PRIMARY KEY)",  // non-numeric floor
		"CREATE TABLE t (id INTEGER /*M:32:4999a*/ PRIMARY KEY)", // floor before a flag
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			decoded := Decode(sql)
			if _, ok := decoded["id"]; ok {
				t.Fatalf("expected malformed marker to yield no entry, got %+v", decoded["id"])
			}
		})
	}
}

// TestStripRemovesFloorMarker pins that Strip (decode.go:229), which matches
// only markerOpen/markerClose and treats everything between them as one
// opaque span, already removes the ":<floor>" extension (TestAutoIncFloorGrammar)
// with no code change: a longer body between the same two delimiters is still
// one span to Strip. A multi-column CREATE TABLE carries a floor marker on one
// column and a plain marker on another so both forms, and the spacing repair
// around each, are proven in the same pass.
//
// Mutation: require the close brace immediately after the flags, breaking the
// floor form's longer span; this fires because the floor marker survives.
func TestStripRemovesFloorMarker(t *testing.T) {
	in := "CREATE TABLE t (id INTEGER /*M:32a:4999*/ PRIMARY KEY, count INTEGER /*M:16u*/ DEFAULT 0, v TEXT)"
	want := "CREATE TABLE t (id INTEGER PRIMARY KEY, count INTEGER DEFAULT 0, v TEXT)"
	if got := Strip(in); got != want {
		t.Errorf("Strip =\n %q\nwant\n %q", got, want)
	}
}
