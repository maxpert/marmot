package rules

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maxpert/marmot/protocol/query/transform"
	"vitess.io/vitess/go/vt/sqlparser"
)

// seqGenerator hands out 1000, 1001, ... so an injected id is recognisable in
// the serialized statement and the order of injection is observable.
type seqGenerator struct{ n atomic.Uint64 }

func (g *seqGenerator) NextID() uint64 { return 1000 + g.n.Add(1) - 1 }

// lookupFor builds the schema lookup the rule receives.
func lookupFor(tables map[string]transform.SchemaInfo) SchemaLookup {
	return func(table string) *transform.SchemaInfo {
		info, ok := tables[table]
		if !ok {
			return nil
		}
		return &info
	}
}

func parseOne(t *testing.T, sql string) sqlparser.Statement {
	t.Helper()
	p, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		t.Fatalf("sqlparser.New: %v", err)
	}
	stmt, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return stmt
}

// idFirst is a table whose auto-increment column is its first column; idThird
// is one where it is the third. The ordinal is the whole point of the
// column-less arms: a rule that assumed position 0 passes idFirst and corrupts
// idThird.
var injectionSchemas = map[string]transform.SchemaInfo{
	"idfirst": {AutoIncrementColumn: "id", AutoIncrementOrdinal: 0},
	"idthird": {AutoIncrementColumn: "id", AutoIncrementOrdinal: 2},
	"plain":   {AutoIncrementColumn: "", AutoIncrementOrdinal: -1},
}

func TestAutoIncrementIDRuleInjectionShapes(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		// want is the serialized statement after the rule ran. Empty means
		// "must be left exactly as parsed".
		want        string
		wantApplied bool
	}{
		{
			// Mutation: restore the `len(insert.Columns) == 0 -> return` early
			// exit in analyzeInsert; no id is injected and want no longer matches.
			name:        "column-less single row at ordinal 0",
			sql:         "insert into idfirst values (null, 'a')",
			want:        "insert into idfirst values (1000, 'a')",
			wantApplied: true,
		},
		{
			// Mutation: inject once instead of per row (hoist newIDLiteral out
			// of the loop); the three ids collapse to one value.
			name:        "column-less multi row mixes NULL, 0 and DEFAULT",
			sql:         "insert into idfirst values (null, 'a'), (0, 'b'), (default, 'c')",
			want:        "insert into idfirst values (1000, 'a'), (1001, 'b'), (1002, 'c')",
			wantApplied: true,
		},
		{
			// Mutation: use 0 instead of info.AutoIncrementOrdinal for the
			// column-less case; the id lands in the first column.
			name:        "column-less substitutes at a non-zero ordinal",
			sql:         "insert into idthird values ('x', 'y', null)",
			want:        "insert into idthird values ('x', 'y', 1000)",
			wantApplied: true,
		},
		{
			// Mutation: drop the *sqlparser.Default arm from needsIDInjection;
			// DEFAULT reaches SQLite and is a syntax error there.
			name:        "DEFAULT in a listed auto-increment column",
			sql:         "insert into idfirst (id, v) values (default, 'a')",
			want:        "insert into idfirst(id, v) values (1000, 'a')",
			wantApplied: true,
		},
		{
			// Mutation: delete the `len(insert.Columns) > 0` append branch;
			// the column is never added.
			name:        "explicit column list omitting the auto-increment column",
			sql:         "insert into idfirst (v) values ('a'), ('b')",
			want:        "insert into idfirst(v, id) values ('a', 1000), ('b', 1001)",
			wantApplied: true,
		},
		{
			// Mutation: make needsIDInjection return true for any Literal;
			// mysqldump's explicit ids get overwritten and the dump no longer
			// restores the ids it recorded.
			name:        "mysqldump shape: column-less with an explicit id is untouched",
			sql:         "insert into idfirst values (7, 'a'), (8, 'b')",
			want:        "",
			wantApplied: false,
		},
		{
			// Mutation: same as above, for the listed-column form.
			name:        "explicit non-zero id in a listed column is untouched",
			sql:         "insert into idfirst (id, v) values (7, 'a')",
			want:        "",
			wantApplied: false,
		},
		{
			// Mutation: drop the HasAutoIncrement guard; the rule injects into
			// a table that has no auto-increment column at all.
			name:        "table without an auto-increment column is untouched",
			sql:         "insert into plain values (1, 'a')",
			want:        "",
			wantApplied: false,
		},
		{
			// Mutation: drop the `colIdx >= 0 -> return nil, nil` arm on the
			// non-Values path; a projecting INSERT ... SELECT starts erroring.
			name:        "INSERT ... SELECT projecting the auto-increment column is untouched",
			sql:         "insert into idfirst (id, v) select id, v from other",
			want:        "",
			wantApplied: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rule := NewAutoIncrementIDRule(&seqGenerator{})
			lookup := lookupFor(injectionSchemas)
			stmt := parseOne(t, tc.sql)
			before := sqlparser.String(stmt)

			needs := rule.NeedsIDInjection(stmt, lookup)
			got, applied, err := rule.ApplyAST(stmt, lookup)
			if err != nil {
				t.Fatalf("ApplyAST returned an error for %q: %v", tc.sql, err)
			}
			if applied != tc.wantApplied {
				t.Errorf("applied = %v, want %v, for %q", applied, tc.wantApplied, tc.sql)
			}
			// The transpiler calls ApplyAST only when NeedsIDInjection says so
			// and bypasses its SQL cache on the same answer, so a gate that
			// disagrees with the rewrite makes the rewrite unreachable.
			// Mutation: restore the independent column-less/Values checks in
			// NeedsIDInjection and this fires on every column-less arm.
			if needs != tc.wantApplied {
				t.Errorf("NeedsIDInjection = %v but ApplyAST applied = %v, for %q", needs, tc.wantApplied, tc.sql)
			}

			want := tc.want
			if want == "" {
				want = before
			}
			if out := sqlparser.String(got); out != want {
				t.Errorf("statement after rule:\n got: %s\nwant: %s", out, want)
			}
		})
	}
}

func TestAutoIncrementIDRuleRejectsUnprojectedInsertSelect(t *testing.T) {
	rule := NewAutoIncrementIDRule(&seqGenerator{})
	lookup := lookupFor(injectionSchemas)

	for _, sql := range []string{
		"insert into idfirst select id, v from other",
		"insert into idfirst (v) select v from other",
	} {
		stmt := parseOne(t, sql)

		// Mutation: restore the `insert.Rows.(sqlparser.Values)` early return
		// in analyzeInsert. The statement is then accepted, SQLite assigns the
		// rowid, and two nodes both mint 1, 2, 3.
		if !rule.NeedsIDInjection(stmt, lookup) {
			t.Fatalf("NeedsIDInjection = false for %q: the transpiler would cache it and never call ApplyAST", sql)
		}

		_, applied, err := rule.ApplyAST(stmt, lookup)
		if err == nil {
			t.Fatalf("ApplyAST accepted %q; want a rejection", sql)
		}
		if applied {
			t.Errorf("ApplyAST reported applied = true alongside an error for %q", sql)
		}

		// Mutation: return a plain fmt.Errorf instead of a CodedError; the
		// client then sees ER_UNKNOWN_ERROR (1105) instead of 1235.
		var coded *transform.CodedError
		if !errors.As(err, &coded) {
			t.Fatalf("error for %q is %T, want *transform.CodedError", sql, err)
		}
		if coded.Code != transform.ErrCodeNotSupportedYet {
			t.Errorf("error code = %d, want %d (ER_NOT_SUPPORTED_YET) for %q", coded.Code, transform.ErrCodeNotSupportedYet, sql)
		}
		// 1235's MySQL message template is "This version of MySQL doesn't yet
		// support '%s'". A client that matches on it must still recognise ours.
		// Mutation: reword the message without the template.
		if !strings.HasPrefix(coded.Message, "This version of MySQL doesn't yet support '") {
			t.Errorf("message %q does not follow MySQL's ER_NOT_SUPPORTED_YET template", coded.Message)
		}
		// The message must name the column the client has to add, or the
		// error is unactionable.
		// Mutation: drop the column name from the message.
		if !strings.Contains(coded.Message, "`id`") {
			t.Errorf("error message %q does not name the auto-increment column", coded.Message)
		}
	}
}

// TestAutoIncrementIDRuleRejectsRowConstructor covers MySQL 8.0.19+'s table
// value constructor. BOTH forms must be refused with 1235: the column-less one
// because the rule cannot index the tuple positionally, and the column-listed
// one because it otherwise passes the gate carrying a literal NULL id and
// reaches SQLite, which cannot parse VALUES ROW(...) at all. Relying on that
// parse error is an admission-shaped guard - it happens to fail closed today
// for a reason unrelated to id assignment.
func TestAutoIncrementIDRuleRejectsRowConstructor(t *testing.T) {
	rule := NewAutoIncrementIDRule(&seqGenerator{})
	lookup := lookupFor(injectionSchemas)

	for _, sql := range []string{
		"insert into idfirst values row(null, 'a')",
		"insert into idfirst (id, v) values row(null, 'a')",
	} {
		stmt := parseOne(t, sql)

		// Mutation: drop the *sqlparser.ValuesStatement arm in analyzeInsert.
		// The column-listed form then returns nothing-to-do and this fires.
		if !rule.NeedsIDInjection(stmt, lookup) {
			t.Fatalf("NeedsIDInjection = false for %q: the transpiler would cache it and never call ApplyAST", sql)
		}
		_, applied, err := rule.ApplyAST(stmt, lookup)
		if err == nil {
			t.Fatalf("ApplyAST accepted %q; want a 1235 rejection", sql)
		}
		if applied {
			t.Errorf("ApplyAST reported applied = true alongside an error for %q", sql)
		}
		var coded *transform.CodedError
		if !errors.As(err, &coded) {
			t.Fatalf("error for %q is %T, want *transform.CodedError", sql, err)
		}
		if coded.Code != transform.ErrCodeNotSupportedYet {
			t.Errorf("error code = %d, want %d for %q", coded.Code, transform.ErrCodeNotSupportedYet, sql)
		}
		// Mutation: reuse the INSERT ... SELECT wording here; it names a
		// projection that a row constructor does not have.
		if !strings.Contains(coded.Message, "VALUES ROW") {
			t.Errorf("message %q does not name the row-constructor shape, for %q", coded.Message, sql)
		}
		if !strings.HasPrefix(coded.Message, "This version of MySQL doesn't yet support '") {
			t.Errorf("message %q does not follow MySQL's ER_NOT_SUPPORTED_YET template", coded.Message)
		}
	}
}

func TestAutoIncrementIDRuleNoLookupOrGenerator(t *testing.T) {
	stmt := parseOne(t, "insert into idfirst values (null, 'a')")

	// Mutation: drop either nil guard in ApplyAST/NeedsIDInjection; the rule
	// dereferences a nil generator or a nil lookup and panics.
	rule := NewAutoIncrementIDRule(nil)
	if rule.NeedsIDInjection(stmt, lookupFor(injectionSchemas)) {
		t.Error("NeedsIDInjection = true with a nil generator")
	}
	if _, applied, err := rule.ApplyAST(stmt, lookupFor(injectionSchemas)); applied || err != nil {
		t.Errorf("ApplyAST with a nil generator: applied=%v err=%v", applied, err)
	}

	rule = NewAutoIncrementIDRule(&seqGenerator{})
	if rule.NeedsIDInjection(stmt, nil) {
		t.Error("NeedsIDInjection = true with a nil schema lookup")
	}
	if _, applied, err := rule.ApplyAST(stmt, nil); applied || err != nil {
		t.Errorf("ApplyAST with a nil schema lookup: applied=%v err=%v", applied, err)
	}
}

// TestAutoIncrementIDRuleUnknownTable pins that a lookup returning nil - the
// table is not in the schema cache - is not a nil dereference.
// Mutation: read info.AutoIncrementColumn directly instead of going through
// info.HasAutoIncrement(), which is the nil-safe accessor.
func TestAutoIncrementIDRuleUnknownTable(t *testing.T) {
	rule := NewAutoIncrementIDRule(&seqGenerator{})
	lookup := lookupFor(injectionSchemas)
	stmt := parseOne(t, "insert into nosuchtable values (null, 'a')")

	if rule.NeedsIDInjection(stmt, lookup) {
		t.Error("NeedsIDInjection = true for a table the lookup does not know")
	}
	if _, applied, err := rule.ApplyAST(stmt, lookup); applied || err != nil {
		t.Errorf("ApplyAST on an unknown table: applied=%v err=%v", applied, err)
	}
}
