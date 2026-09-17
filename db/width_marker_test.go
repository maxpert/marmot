//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"testing"
)

// TestWidthMarkerSurvivesToTheSchemaCache walks the whole path this step
// builds: the transpiler writes a marker into the CREATE TABLE text, SQLite
// stores that text verbatim, and the schema cache reads the width back out of
// it. Each hop is checked, because a marker that is written but never read is
// indistinguishable from one that is read but never written.
func TestWidthMarkerSurvivesToTheSchemaCache(t *testing.T) {
	source := newRowidTestDatabase(t, 1)

	// The DDL as the transpiler would emit it for
	// "CREATE TABLE widths (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	//  small SMALLINT, big BIGINT, name TEXT)".
	const ddl = "CREATE TABLE widths (" +
		"id INTEGER /*M:32ua*/ PRIMARY KEY, " +
		"small INTEGER /*M:16*/, " +
		"big INTEGER, " +
		"name TEXT)"
	if err := execAndReload(source, ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Hop 1: SQLite kept the marker in sqlite_master.
	// Mutation: none needed - if SQLite normalised it away the next hop fails.
	schema, err := source.GetCachedTableSchema("widths")
	if err != nil {
		t.Fatalf("GetCachedTableSchema: %v", err)
	}

	// Hop 2: PRAGMA-derived type is plain INTEGER, which is why the width
	// cannot be read from there. This is the hazard the design names.
	// Mutation: read the width from ColumnSchema.Type instead of the marker.
	byName := map[string]ColumnSchema{}
	for _, col := range schema.FullColumns {
		byName[col.Name] = col
		if col.Name != "name" && col.Type != "INTEGER" {
			t.Errorf("column %s has PRAGMA type %q, want plain INTEGER", col.Name, col.Type)
		}
	}

	// Hop 3: the width came back.
	// Mutation: parse markers from PRAGMA table_info instead of CreateSQL;
	// every width below becomes zero.
	if got := byName["id"]; got.DeclaredWidth != 32 || !got.Unsigned || !got.ExplicitAutoInc {
		t.Errorf("id = width %d unsigned %v autoinc %v, want 32/true/true",
			got.DeclaredWidth, got.Unsigned, got.ExplicitAutoInc)
	}
	if got := byName["small"]; got.DeclaredWidth != 16 || got.Unsigned || got.ExplicitAutoInc {
		t.Errorf("small = width %d unsigned %v autoinc %v, want 16/false/false",
			got.DeclaredWidth, got.Unsigned, got.ExplicitAutoInc)
	}
	// BIGINT and non-integer columns carry no marker and keep the 64-bit path.
	// Mutation: default DeclaredWidth to 64 instead of 0.
	if got := byName["big"]; got.DeclaredWidth != 0 {
		t.Errorf("big = width %d, want 0 (unmarked, 64-bit path)", got.DeclaredWidth)
	}
	if got := byName["name"]; got.DeclaredWidth != 0 {
		t.Errorf("name = width %d, want 0", got.DeclaredWidth)
	}
}

// TestMarkerDoesNotBreakRowidAliasing is the hazard the design calls out: the
// stored type token must remain exactly INTEGER, because only that spelling
// makes a PRIMARY KEY an alias of the rowid. "INT PRIMARY KEY" does not, and
// storing the MySQL type word there would make LAST_INSERT_ID() report an
// unrelated internal rowid - a silent wrong answer.
//
// Mutation: emit "INT /*M:32a*/" instead of "INTEGER /*M:32a*/". The inserted
// id comes back NULL and the first assertion fires.
func TestMarkerDoesNotBreakRowidAliasing(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	if err := execAndReload(source, "CREATE TABLE aliased (id INTEGER /*M:32a*/ PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err := source.GetWriteDB().Exec("INSERT INTO aliased (id, v) VALUES (NULL, 'x')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id, rowid int64
	if err := source.GetWriteDB().QueryRow("SELECT id, rowid FROM aliased").Scan(&id, &rowid); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if id == 0 {
		t.Fatal("id came back 0 or NULL: the marker broke rowid aliasing")
	}
	if id != rowid {
		t.Errorf("id = %d but rowid = %d; the column is no longer a rowid alias", id, rowid)
	}

	// And the schema cache still recognises it as the auto-increment column,
	// which is what the injection rule keys on.
	// Mutation: have loadSchema reject a type string containing a marker.
	schema, err := source.GetCachedTableSchema("aliased")
	if err != nil {
		t.Fatalf("GetCachedTableSchema: %v", err)
	}
	if schema.GetAutoIncrementCol() != "id" {
		t.Errorf("auto-increment column = %q, want id", schema.GetAutoIncrementCol())
	}
}

// TestUnmarkedTableIsByteIdentical pins the migration promise: a table whose
// DDL carries no marker behaves exactly as it did before markers existed, and
// its stored text is unchanged.
//
// The byte-identical guarantee is about BIGINT and about pre-existing tables,
// NOT about the word INTEGER. A column declared INTEGER in MySQL is 32 bits -
// INTEGER and INT are the same type there - so a new table declaring INTEGER is
// marked, and that is asserted separately by
// TestIntegerSpelledOutIsMarked. This test therefore feeds DDL that already
// looks like stored SQLite output, which is what an old table's sqlite_master
// text looks like.
//
// Mutation: emit a marker for BIGINT, or for unmarked INTEGER columns read
// back from an old table.
func TestUnmarkedTableIsByteIdentical(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	const ddl = "CREATE TABLE legacy (id INTEGER PRIMARY KEY, n INTEGER, v TEXT)"
	if err := execAndReload(source, ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	var stored string
	if err := source.GetWriteDB().QueryRow(
		"SELECT sql FROM sqlite_master WHERE type='table' AND name='legacy'").Scan(&stored); err != nil {
		t.Fatalf("SELECT sql: %v", err)
	}
	if stored != ddl {
		t.Errorf("stored DDL =\n %q\nwant\n %q", stored, ddl)
	}

	schema, err := source.GetCachedTableSchema("legacy")
	if err != nil {
		t.Fatalf("GetCachedTableSchema: %v", err)
	}
	for _, col := range schema.FullColumns {
		if col.DeclaredWidth != 0 || col.Unsigned || col.ExplicitAutoInc {
			t.Errorf("column %s picked up marker attributes from an unmarked table: %+v", col.Name, col)
		}
	}
}
