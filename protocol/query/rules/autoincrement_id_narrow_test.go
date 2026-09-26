package rules

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/transform"
	"vitess.io/vitess/go/vt/sqlparser"
)

// fakeNarrow records what the rule asks of the narrow allocator. Allocate
// hands out ids from next upward; Admit moves next past an explicit id, or
// refuses it with admitErr.
type fakeNarrow struct {
	next      uint64
	allocs    []int
	observed  []uint64
	allocErr  error
	admitErr  error
	database  string
	lastWidth uint64
}

func (f *fakeNarrow) Allocate(database, table string, widthMax uint64, n int) (uint64, error) {
	f.database, f.lastWidth = database, widthMax
	f.allocs = append(f.allocs, n)
	if f.allocErr != nil {
		return 0, f.allocErr
	}
	first := f.next
	f.next += uint64(n)
	return first, nil
}

func (f *fakeNarrow) Admit(database, table string, widthMax, explicit uint64) error {
	f.observed = append(f.observed, explicit)
	if f.admitErr != nil {
		return f.admitErr
	}
	if explicit >= f.next {
		f.next = explicit + 1
	}
	return nil
}

// narrowSchemas: a signed INT AUTO_INCREMENT table with the id first, one
// with it third, a TINYINT one, and a narrow INTEGER PRIMARY KEY not declared
// AUTO_INCREMENT.
var narrowSchemas = map[string]transform.SchemaInfo{
	"alias":  {AutoIncrementColumn: "id", AutoIncrementOrdinal: 0, AutoIncrementWidth: 32, Database: "lldap"},
	"groups": {AutoIncrementColumn: "group_id", AutoIncrementOrdinal: 0, AutoIncrementWidth: 32, AutoIncrementExplicit: true, Database: "lldap"},
	"late":   {AutoIncrementColumn: "id", AutoIncrementOrdinal: 2, AutoIncrementWidth: 32, AutoIncrementExplicit: true, Database: "lldap"},
	"tiny":   {AutoIncrementColumn: "id", AutoIncrementOrdinal: 0, AutoIncrementWidth: 8, AutoIncrementExplicit: true, Database: "lldap"},
}

func applyNarrow(t *testing.T, sql string, narrow NarrowAllocator) (string, bool, error) {
	t.Helper()
	rule := NewAutoIncrementIDRule(&seqGenerator{})
	stmt := parseOne(t, sql)
	got, applied, _, err := rule.ApplyAST(stmt, lookupFor(narrowSchemas), narrow, nil)
	if err != nil {
		return "", applied, err
	}
	return sqlparser.String(got), applied, nil
}

// TestNarrowInjectionShapes pins every injection shape on a marked column:
// the ids come from the narrow allocator, one Allocate per statement for
// exactly the rows that need one, contiguous in row order.
//
// Mutation: route marked columns to the wide generator. The 1000-series ids
// of seqGenerator appear instead of 1-series ids and every case fires.
func TestNarrowInjectionShapes(t *testing.T) {
	cases := []struct {
		name       string
		sql        string
		want       string
		wantAllocs []int
	}{
		{"column list omitting the id", "insert into groups (name) values ('a'), ('b'), ('c')",
			"insert into `groups`(`name`, group_id) values ('a', 1), ('b', 2), ('c', 3)", []int{3}},
		{"column-less at ordinal 0, NULL 0 and DEFAULT alike", "insert into groups values (null, 'a'), (0, 'b'), (default, 'c')",
			"insert into `groups` values (1, 'a'), (2, 'b'), (3, 'c')", []int{3}},
		{"column-less at a non-zero ordinal", "insert into late values ('x', 'y', null)",
			"insert into late values ('x', 'y', 1)", []int{1}},
		{"listed id with DEFAULT", "insert into groups (group_id, name) values (default, 'a')",
			"insert into `groups`(group_id, `name`) values (1, 'a')", []int{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			narrow := &fakeNarrow{next: 1}
			got, applied, err := applyNarrow(t, tc.sql, narrow)
			if err != nil {
				t.Fatalf("ApplyAST: %v", err)
			}
			if !applied || got != tc.want {
				t.Fatalf("got %q (applied=%v), want %q", got, applied, tc.want)
			}
			if fmt.Sprint(narrow.allocs) != fmt.Sprint(tc.wantAllocs) {
				t.Fatalf("Allocate calls %v, want %v: a statement takes its ids in one call", narrow.allocs, tc.wantAllocs)
			}
			if narrow.database != "lldap" || narrow.lastWidth != 2147483647 {
				t.Fatalf("allocated for %q with ceiling %d, want lldap / 2147483647", narrow.database, narrow.lastWidth)
			}
		})
	}
}

// TestNarrowExplicitIDsAreAdmittedBeforeAllocating pins explicit literal ids:
// every one is admitted, in ascending order, before any id is generated, so a
// statement mixing explicit and generated ids never generates one it also
// supplies, and a statement of explicit ids only is still admitted.
//
// Mutation: skip Admit when a statement carries explicit ids. "explicit ids
// were not admitted" fires, and the mixed statement collides.
func TestNarrowExplicitIDsAreAdmittedBeforeAllocating(t *testing.T) {
	narrow := &fakeNarrow{next: 1}
	got, applied, err := applyNarrow(t, "insert into groups values (null, 'a'), (2, 'b'), (null, 'c')", narrow)
	if err != nil {
		t.Fatalf("ApplyAST: %v", err)
	}
	if want := "insert into `groups` values (3, 'a'), (2, 'b'), (4, 'c')"; !applied || got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	dump := &fakeNarrow{next: 1}
	got, applied, err = applyNarrow(t, "insert into groups values (900, 'b'), (7, 'a')", dump)
	if err != nil {
		t.Fatalf("ApplyAST: %v", err)
	}
	if fmt.Sprint(dump.observed) != "[7 900]" {
		t.Fatalf("explicit ids were not admitted in ascending order: %v", dump.observed)
	}
	if len(dump.allocs) != 0 || applied {
		t.Fatalf("a statement of explicit ids generated ids: allocs=%v applied=%v", dump.allocs, applied)
	}
	if want := "insert into `groups` values (900, 'b'), (7, 'a')"; got != want {
		t.Fatalf("explicit ids were rewritten: %q", got)
	}
}

// TestNarrowErrorsReachTheClientWithMySQLCodes pins the three refusals: an
// explicit id past the column is 1264, a full column is 1062 on the column's
// maximum, and an allocator that cannot reach a quorum is the retryable 1205.
//
// Mutation: map every allocator error to one code. One of the arms fires.
func TestNarrowErrorsReachTheClientWithMySQLCodes(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		allocErr error
		wantCode uint16
		wantMsg  string
	}{
		{"explicit id above the column", "insert into tiny values (128, 'a')", nil, mysqlcode.ErrCodeDataOutOfRange,
			"Out of range value for column 'id' at row 1"},
		{"column exhausted", "insert into tiny values (null, 'a')", fmt.Errorf("claim: %w", id.ErrRangeExhausted),
			mysqlcode.ErrCodeDupEntry, "Duplicate entry '127' for key 'tiny.PRIMARY'"},
		{"quorum unavailable", "insert into tiny values (null, 'a')", errors.New("prepare quorum not achieved"),
			mysqlcode.ErrCodeLockTimeout, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := applyNarrow(t, tc.sql, &fakeNarrow{next: 1, allocErr: tc.allocErr})
			var coded *transform.CodedError
			if !errors.As(err, &coded) || coded.Code != tc.wantCode {
				t.Fatalf("err = %v, want MySQL code %d", err, tc.wantCode)
			}
			if tc.wantMsg != "" && coded.Message != tc.wantMsg {
				t.Fatalf("message %q, want %q", coded.Message, tc.wantMsg)
			}
		})
	}
}

// TestNarrowWithoutAllocatorIsRefused pins that a path with no narrow
// allocator never falls back to wide ids for a marked column.
//
// Mutation: fall back to the wide generator when narrow is nil. The 53-bit
// shape of the bug this work fixes returns and "a marked column got a wide
// id" fires.
func TestNarrowWithoutAllocatorIsRefused(t *testing.T) {
	got, applied, err := applyNarrow(t, "insert into groups (name) values ('a')", nil)
	if err == nil {
		t.Fatalf("a marked column got a wide id: %q (applied=%v)", got, applied)
	}
}

// TestWideTableKeepsTheWideGenerator pins that an unmarked table - BIGINT,
// and every table created before markers existed - never touches the narrow
// allocator.
//
// Mutation: route every auto-increment table to the narrow allocator.
// "an unmarked table used the narrow allocator" fires.
func TestWideTableKeepsTheWideGenerator(t *testing.T) {
	narrow := &fakeNarrow{next: 1}
	rule := NewAutoIncrementIDRule(&seqGenerator{})
	stmt := parseOne(t, "insert into idfirst (v) values ('a')")
	got, applied, _, err := rule.ApplyAST(stmt, lookupFor(injectionSchemas), narrow, nil)
	if err != nil || !applied {
		t.Fatalf("ApplyAST: applied=%v err=%v", applied, err)
	}
	if len(narrow.allocs) != 0 || len(narrow.observed) != 0 {
		t.Fatalf("an unmarked table used the narrow allocator")
	}
	if want := "insert into idfirst(v, id) values ('a', 1000)"; sqlparser.String(got) != want {
		t.Fatalf("got %q, want %q", sqlparser.String(got), want)
	}
}

// TestNarrowExplicitIDAtOrBelowTheBaseIsRefused pins the refusal the rule
// gives an id the allocator will not admit: ER_NOT_SUPPORTED_YET naming the
// column and the base, before any row is written.
//
// Mutation: map a *id.BelowBaseError like any allocation failure. The code
// becomes 1205 and this fires.
func TestNarrowExplicitIDAtOrBelowTheBaseIsRefused(t *testing.T) {
	narrow := &fakeNarrow{next: 1, admitErr: &id.BelowBaseError{ID: 30, Base: 128}}
	_, _, err := applyNarrow(t, "insert into groups values (30, 'x')", narrow)
	var coded *transform.CodedError
	if !errors.As(err, &coded) || coded.Code != mysqlcode.ErrCodeNotSupportedYet {
		t.Fatalf("err = %v, want MySQL code %d", err, mysqlcode.ErrCodeNotSupportedYet)
	}
	if !strings.Contains(coded.Message, "group_id") || !strings.Contains(coded.Message, "128") {
		t.Fatalf("refusal %q does not name the column and the base", coded.Message)
	}
	if len(narrow.allocs) != 0 {
		t.Fatalf("ids were generated for a refused statement")
	}
}

// TestNarrowKeyNotDeclaredAutoIncrementKeepsTheWidePath pins that a narrow
// INTEGER PRIMARY KEY not declared AUTO_INCREMENT never touches the narrow
// allocator: MySQL never generates its ids, so the ids clients write there
// are theirs, and the admission rule for AUTO_INCREMENT columns does not
// apply.
//
// Mutation: make IsNarrow ignore AutoIncrementExplicit. "a key not declared
// AUTO_INCREMENT used the narrow allocator" fires.
func TestNarrowKeyNotDeclaredAutoIncrementKeepsTheWidePath(t *testing.T) {
	narrow := &fakeNarrow{next: 1}
	got, applied, err := applyNarrow(t, "insert into alias values (7, 'x')", narrow)
	if err != nil || applied {
		t.Fatalf("a client id in a key not declared AUTO_INCREMENT was rewritten: %q applied=%v err=%v", got, applied, err)
	}
	if _, _, err := applyNarrow(t, "insert into alias values (null, 'x')", narrow); err != nil {
		t.Fatalf("ApplyAST: %v", err)
	}
	if len(narrow.allocs) != 0 || len(narrow.observed) != 0 {
		t.Fatalf("a key not declared AUTO_INCREMENT used the narrow allocator")
	}
}

// TestNarrowBoundNullOrZeroIDIsGenerated is F3: a prepared INSERT whose id
// placeholder is bound to NULL or 0 gets a generated id, exactly as the
// literal NULL does, and never reaches SQLite, which would assign its own
// rowid. The id is bound in the placeholder's place rather than written into
// the statement, so the caller's other values keep their positions. An
// explicit bound id, and a wide table, are left alone.
//
// Mutation: make boundRequestsID return false for nil. The first row's
// placeholder gets no id and "a placeholder bound to NULL got no id" fires.
func TestNarrowBoundNullOrZeroIDIsGenerated(t *testing.T) {
	apply := func(sql string, schemas map[string]transform.SchemaInfo, bound ...interface{}) (string, map[int]uint64, *fakeNarrow) {
		t.Helper()
		narrow := &fakeNarrow{next: 100}
		got, _, ids, err := NewAutoIncrementIDRule(&seqGenerator{}).ApplyAST(parseOne(t, sql), lookupFor(schemas), narrow, bound)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return sqlparser.String(got), ids, narrow
	}

	sql, ids, narrow := apply("INSERT INTO groups (group_id, name) VALUES (?, ?), (?, ?), (?, ?)", narrowSchemas,
		nil, "a", int64(7), "b", int64(0), "c")
	if want := map[int]uint64{0: 100, 4: 101}; fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("a placeholder bound to NULL got no id: bound ids %v, want %v", ids, want)
	}
	if !strings.Contains(sql, "values (:v1, :v2), (:v3, :v4), (:v5, :v6)") {
		t.Fatalf("the statement's placeholders were rewritten: %s", sql)
	}
	if fmt.Sprint(narrow.allocs) != "[2]" {
		t.Fatalf("allocations %v, want one of 2", narrow.allocs)
	}

	if _, ids, _ := apply("INSERT INTO groups (group_id, name) VALUES (?, ?)", narrowSchemas, []byte("0"), "a"); ids[0] != 100 {
		t.Fatalf("a placeholder bound to '0' got no id: %v", ids)
	}
	if _, ids, narrow := apply("INSERT INTO groups (group_id, name) VALUES (?, ?)", narrowSchemas, int64(9), "a"); len(ids) != 0 || len(narrow.allocs) != 0 {
		t.Fatalf("an explicit bound id was replaced: %v, allocations %v", ids, narrow.allocs)
	}
	if _, ids, _ := apply("INSERT INTO idfirst (id, name) VALUES (?, ?)", injectionSchemas, nil, "a"); len(ids) != 0 {
		t.Fatalf("a wide table's bound NULL was replaced: %v", ids)
	}
}
