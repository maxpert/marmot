package transform

import (
	"strings"
	"testing"

	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestIntTypeRule_Name(t *testing.T) {
	rule := &IntTypeRule{}
	if rule.Name() != "IntType" {
		t.Errorf("Name() = %q, want %q", rule.Name(), "IntType")
	}
}

func TestIntTypeRule_Priority(t *testing.T) {
	rule := &IntTypeRule{}
	if rule.Priority() != 5 {
		t.Errorf("Priority() = %d, want %d", rule.Priority(), 5)
	}
}

// TestIntTypeRule_ModifiesAST verifies that IntTypeRule modifies the AST correctly
// even though it always defers to CreateTableRule for serialization.
func TestIntTypeRule_ModifiesAST(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		shouldModify  bool
		checkUnsigned bool
		checkAutoInc  bool
		checkIntType  bool
	}{
		{
			name:         "INT AUTO_INCREMENT stripped",
			input:        "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
			shouldModify: true,
			checkAutoInc: true,
			checkIntType: true,
		},
		{
			name:          "BIGINT UNSIGNED AUTO_INCREMENT stripped",
			input:         "CREATE TABLE wp_users (ID BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT, name VARCHAR(100))",
			shouldModify:  true,
			checkUnsigned: true,
			checkAutoInc:  true,
		},
		{
			name:          "INT UNSIGNED stripped",
			input:         "CREATE TABLE items (id INT UNSIGNED NOT NULL, data TEXT)",
			shouldModify:  true,
			checkUnsigned: true,
		},
		{
			name:          "TINYINT UNSIGNED stripped",
			input:         "CREATE TABLE flags (active TINYINT UNSIGNED DEFAULT 0)",
			shouldModify:  true,
			checkUnsigned: true,
			checkIntType:  true,
		},
		{
			name:         "INT converted to INTEGER",
			input:        "CREATE TABLE orders (id INT PRIMARY KEY, total REAL)",
			shouldModify: true,
			checkIntType: true,
		},
		{
			name:         "VARCHAR column unchanged - no modification",
			input:        "CREATE TABLE names (id VARCHAR(50) PRIMARY KEY, value TEXT)",
			shouldModify: false,
		},
		{
			name:         "not a CREATE TABLE - returns immediately",
			input:        "INSERT INTO users (id, name) VALUES (1, 'test')",
			shouldModify: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &IntTypeRule{}
			_, err = rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			// IntTypeRule always returns ErrRuleNotApplicable for CREATE TABLE
			// (defers to CreateTableRule for serialization)
			if err != ErrRuleNotApplicable {
				t.Errorf("expected ErrRuleNotApplicable, got %v", err)
			}

			if !tt.shouldModify {
				return
			}

			// Verify AST was modified
			create, ok := stmt.(*sqlparser.CreateTable)
			if !ok {
				return // Non-CREATE TABLE statement
			}

			for _, col := range create.TableSpec.Columns {
				if col.Type == nil {
					continue
				}

				// Check UNSIGNED was stripped
				if tt.checkUnsigned && col.Type.Unsigned {
					t.Errorf("UNSIGNED should have been stripped from column %s", col.Name.String())
				}

				// Check AUTO_INCREMENT was stripped
				if tt.checkAutoInc && col.Type.Options != nil && col.Type.Options.Autoincrement {
					t.Errorf("AUTO_INCREMENT should have been stripped from column %s", col.Name.String())
				}

				// Check integer type was converted to INTEGER
				if tt.checkIntType && isIntegerType(strings.ToUpper(storedTypeToken(col.Type.Type))) {
					if storedTypeToken(col.Type.Type) != "INTEGER" {
						t.Errorf("integer type should have been converted to INTEGER for column %s, got %s", col.Name.String(), col.Type.Type)
					}
				}
			}
		})
	}
}

func TestIntTypeRule_WordPressSchema(t *testing.T) {
	// Real WordPress CREATE TABLE statement
	input := `CREATE TABLE wp_users (
		ID bigint(20) unsigned NOT NULL auto_increment,
		user_login varchar(60) NOT NULL default '',
		user_pass varchar(255) NOT NULL default '',
		user_status int(11) NOT NULL default '0',
		PRIMARY KEY (ID)
	)`

	stmt, err := sqlparser.NewTestParser().Parse(input)
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rule := &IntTypeRule{}
	_, err = rule.Transform(stmt, nil, nil, "wordpress", &SQLiteSerializer{})

	// Should defer to CreateTableRule
	if err != ErrRuleNotApplicable {
		t.Errorf("expected ErrRuleNotApplicable, got %v", err)
	}

	// Verify AST was modified
	create := stmt.(*sqlparser.CreateTable)
	for _, col := range create.TableSpec.Columns {
		if col.Type == nil {
			continue
		}

		// Check UNSIGNED was stripped
		if col.Type.Unsigned {
			t.Errorf("UNSIGNED should have been stripped from column %s", col.Name.String())
		}

		// Check AUTO_INCREMENT was stripped
		if col.Type.Options != nil && col.Type.Options.Autoincrement {
			t.Errorf("AUTO_INCREMENT should have been stripped from column %s", col.Name.String())
		}

		// Check integer types were converted to INTEGER
		if isIntegerType(strings.ToUpper(storedTypeToken(col.Type.Type))) {
			if storedTypeToken(col.Type.Type) != "INTEGER" {
				t.Errorf("integer type should be INTEGER for column %s, got %s", col.Name.String(), col.Type.Type)
			}
		}
	}
}

func TestIntTypeRule_AllIntTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"TINYINT", "CREATE TABLE t (col TINYINT UNSIGNED)"},
		{"SMALLINT", "CREATE TABLE t (col SMALLINT UNSIGNED)"},
		{"MEDIUMINT", "CREATE TABLE t (col MEDIUMINT UNSIGNED)"},
		{"INT", "CREATE TABLE t (col INT UNSIGNED)"},
		{"INTEGER", "CREATE TABLE t (col INTEGER UNSIGNED)"},
		{"BIGINT", "CREATE TABLE t (col BIGINT UNSIGNED)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, _ := sqlparser.NewTestParser().Parse(tt.input)

			rule := &IntTypeRule{}
			_, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			// Should defer to CreateTableRule
			if err != ErrRuleNotApplicable {
				t.Errorf("expected ErrRuleNotApplicable for %s, got %v", tt.name, err)
			}

			// Verify AST was modified
			create := stmt.(*sqlparser.CreateTable)
			for _, col := range create.TableSpec.Columns {
				if col.Type == nil {
					continue
				}
				if col.Type.Unsigned {
					t.Errorf("%s: UNSIGNED should have been stripped", tt.name)
				}
				if storedTypeToken(col.Type.Type) != "INTEGER" {
					t.Errorf("%s: stored type token should be INTEGER, got %s", tt.name, col.Type.Type)
				}
			}
		})
	}
}

func TestIntTypeRule_DeferToCreateTableRule(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "With non-primary KEY definition",
			input: "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(100), KEY idx_email (email))",
		},
		{
			name:  "With UNIQUE KEY definition",
			input: "CREATE TABLE users (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), UNIQUE KEY idx_name (name))",
		},
		{
			name:  "With multiple KEY definitions",
			input: "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, user_id INT UNSIGNED, title VARCHAR(100), KEY idx_user (user_id), KEY idx_title (title))",
		},
		{
			name:  "With PRIMARY KEY only",
			input: "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.input)
			if err != nil {
				t.Fatalf("failed to parse SQL: %v", err)
			}

			rule := &IntTypeRule{}
			result, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})

			// Should return ErrRuleNotApplicable to defer to CreateTableRule
			if err != ErrRuleNotApplicable {
				t.Errorf("expected ErrRuleNotApplicable to defer to CreateTableRule, got %v", err)
			}
			if result != nil {
				t.Errorf("expected nil result when deferring to CreateTableRule, got %v", result)
			}

			// Verify that the AST was modified (AUTO_INCREMENT removed, type changed to INTEGER)
			create := stmt.(*sqlparser.CreateTable)
			for _, col := range create.TableSpec.Columns {
				if col.Type == nil {
					continue
				}
				// Check that AUTO_INCREMENT was stripped
				if col.Type.Options != nil && col.Type.Options.Autoincrement {
					t.Errorf("AUTO_INCREMENT should have been removed from column %s", col.Name.String())
				}
				// Check that integer types were converted to INTEGER
				if isIntegerType(strings.ToUpper(storedTypeToken(col.Type.Type))) {
					if storedTypeToken(col.Type.Type) != "INTEGER" {
						t.Errorf("integer type should have been converted to INTEGER for column %s, got %s", col.Name.String(), col.Type.Type)
					}
				}
			}
		})
	}
}

// storedTypeToken returns the SQLite type word without the width marker the
// transpiler now appends. The assertions below are NOT loosened by it: the
// stored token must still be exactly "INTEGER", because only that spelling
// aliases the rowid. What the marker adds is checked separately, by
// TestIntTypeRuleEmitsWidthMarker.
func storedTypeToken(declared string) string {
	if i := strings.Index(declared, "/*M:"); i >= 0 {
		return strings.TrimSpace(declared[:i])
	}
	return declared
}

// TestIntTypeRuleEmitsWidthMarker pins what the marker adds on CREATE TABLE.
// The stored token must still be exactly "INTEGER" - that is asserted by the
// tests above and by the storedTypeToken helper - and the declared width, the
// UNSIGNED modifier and an explicit AUTO_INCREMENT must survive beside it.
func TestIntTypeRuleEmitsWidthMarker(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		column string
		want   string
	}{
		// Mutation: capture the attributes AFTER the strips instead of before.
		// Unsigned and ExplicitAutoInc are already false by then, so every row
		// with u or a loses it.
		{"INT AUTO_INCREMENT", "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY)", "id", "INTEGER /*M:32a*/"},
		{"INT UNSIGNED AUTO_INCREMENT", "CREATE TABLE t (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", "id", "INTEGER /*M:32ua*/"},
		{"TINYINT", "CREATE TABLE t (flag TINYINT)", "flag", "INTEGER /*M:8*/"},
		{"SMALLINT UNSIGNED", "CREATE TABLE t (qty SMALLINT UNSIGNED)", "qty", "INTEGER /*M:16u*/"},
		{"MEDIUMINT", "CREATE TABLE t (n MEDIUMINT)", "n", "INTEGER /*M:24*/"},
		{"INTEGER spelled out", "CREATE TABLE t (id INTEGER AUTO_INCREMENT PRIMARY KEY)", "id", "INTEGER /*M:32a*/"},
		// BIGINT is deliberately unmarked: it keeps the existing 64-bit path,
		// and an existing BIGINT table's DDL text must be byte-identical to
		// what it was before markers existed.
		// Mutation: return 64, true from BitsForType for BIGINT.
		{"BIGINT is unmarked", "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY)", "id", "INTEGER"},
		{"BIGINT UNSIGNED is unmarked", "CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", "id", "INTEGER"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			rule := &IntTypeRule{}
			if _, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
				t.Fatalf("Transform returned %v, want ErrRuleNotApplicable", err)
			}

			create := stmt.(*sqlparser.CreateTable)
			var found bool
			for _, col := range create.TableSpec.Columns {
				if col.Name.String() != tc.column {
					continue
				}
				found = true
				if col.Type.Type != tc.want {
					t.Errorf("declared type = %q, want %q", col.Type.Type, tc.want)
				}
			}
			if !found {
				t.Fatalf("column %q not found", tc.column)
			}
		})
	}
}

// TestIntTypeRuleMarkerSurvivesSerialization pins the marker through the step
// that actually produces the DDL SQLite stores. A marker held only in the AST
// and dropped by the serializer would be invisible until a restart read
// sqlite_master back.
//
// Mutation: emit the marker as a separate AST field the serializer ignores.
func TestIntTypeRuleMarkerSurvivesSerialization(t *testing.T) {
	stmt, err := sqlparser.NewTestParser().Parse(
		"CREATE TABLE t (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rule := &IntTypeRule{}
	if _, err := rule.Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
		t.Fatalf("Transform returned %v, want ErrRuleNotApplicable", err)
	}

	serialized := (&SQLiteSerializer{}).Serialize(stmt)
	if !strings.Contains(serialized, "/*M:32ua*/") {
		t.Fatalf("serialized DDL lost the marker:\n%s", serialized)
	}
	// And it must decode back to the column it belongs to.
	decoded := intmarker.Decode(serialized)
	if got := decoded["id"]; got != (intmarker.Attributes{Bits: 32, Unsigned: true, ExplicitAutoInc: true}) {
		t.Errorf("decoded %+v from %q", got, serialized)
	}
}

// TestIntegerSpelledOutIsMarked records a deliberate choice: INTEGER is
// marked, although a looser reading would leave BIGINT *and* INTEGER unmarked.
//
// Marking INTEGER is correct and the design text is loose: in MySQL, INTEGER is
// a synonym for INT and is 32 bits, so a column declared INTEGER AUTO_INCREMENT
// overflows at 2147483647 exactly as INT does. Treating it as unmarked would
// route it to the 64-bit path and reproduce the overflow this work exists to
// fix. Only BIGINT is genuinely 64-bit and genuinely unmarked.
//
// Mutation: make BitsForType return ok == false for "INTEGER"; the first row
// fires. Make it return true for "BIGINT"; the second fires.
func TestIntegerSpelledOutIsMarked(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"INTEGER is 32 bits, like INT", "CREATE TABLE t (id INTEGER AUTO_INCREMENT PRIMARY KEY)", "INTEGER /*M:32a*/"},
		{"BIGINT is the only unmarked integer", "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY)", "INTEGER"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if _, err := (&IntTypeRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
				t.Fatalf("Transform returned %v, want ErrRuleNotApplicable", err)
			}
			got := stmt.(*sqlparser.CreateTable).TableSpec.Columns[0].Type.Type
			if got != tc.want {
				t.Errorf("declared type = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCreateTableAutoIncrementOptionBecomesMarkerFloor pins the
// AUTO_INCREMENT=N floor end to end: a client's table-level AUTO_INCREMENT=N option must survive
// the transpiler not as a table option (CreateTableRule discards
// TableSpec.Options entirely - see create_table.go) but as the floor riding in
// the AUTO_INCREMENT column's own marker, because DDL replicates as raw SQL
// text and every node must derive the same floor from that text alone.
//
// Runs both rules in the same priority order the transpiler pipeline uses
// (IntTypeRule, priority 5, mutates the AST; CreateTableRule, priority 10,
// clears TableSpec.Options and serializes), matching how transpiler.go
// actually drives them.
//
// Mutation: read the AUTO_INCREMENT=N option after CreateTableRule has
// already nilled TableSpec.Options; floor is always 0 and this test's core
// assertion (":4999") never appears.
func TestCreateTableAutoIncrementOptionBecomesMarkerFloor(t *testing.T) {
	stmt, err := sqlparser.NewTestParser().Parse(
		"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT) AUTO_INCREMENT=5000")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if _, err := (&IntTypeRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
		t.Fatalf("IntTypeRule.Transform returned %v, want ErrRuleNotApplicable", err)
	}

	results, err := (&CreateTableRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("CreateTableRule.Transform: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one statement")
	}
	sql := results[0].SQL

	// N=5000 means the next id MySQL issues is 5000, so the stored base floor
	// is N-1: the id treated as already issued just before it.
	if !strings.Contains(sql, "/*M:32a:4999*/") {
		t.Fatalf("emitted SQL missing floor marker /*M:32a:4999*/, got: %q", sql)
	}
	// The raw table option must not itself leak into the SQLite text - SQLite
	// has no AUTO_INCREMENT=N table option syntax.
	if strings.Contains(strings.ToUpper(sql), "AUTO_INCREMENT=5000") ||
		strings.Contains(strings.ToUpper(sql), "AUTO_INCREMENT = 5000") {
		t.Fatalf("emitted SQL leaked the raw table option: %q", sql)
	}

	decoded := intmarker.Decode(sql)
	got, ok := decoded["id"]
	if !ok {
		t.Fatalf("Decode found no marker for column id in: %q", sql)
	}
	want := intmarker.Attributes{Bits: 32, ExplicitAutoInc: true, AutoIncFloor: 4999}
	if got != want {
		t.Errorf("decoded attributes = %+v, want %+v", got, want)
	}
}

// TestCreateTableNoAutoIncrementOptionMeansNoFloor guards the other direction:
// a CREATE TABLE with no AUTO_INCREMENT=N table option must emit a marker
// byte-identical to what the transpiler produced before floors existed.
func TestCreateTableNoAutoIncrementOptionMeansNoFloor(t *testing.T) {
	stmt, err := sqlparser.NewTestParser().Parse(
		"CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := (&IntTypeRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{}); err != ErrRuleNotApplicable {
		t.Fatalf("IntTypeRule.Transform returned %v, want ErrRuleNotApplicable", err)
	}
	results, err := (&CreateTableRule{}).Transform(stmt, nil, nil, "testdb", &SQLiteSerializer{})
	if err != nil {
		t.Fatalf("CreateTableRule.Transform: %v", err)
	}
	sql := results[0].SQL
	if !strings.Contains(sql, "/*M:32a*/") {
		t.Fatalf("expected unfloored marker /*M:32a*/, got: %q", sql)
	}
	if strings.Contains(sql, ":4999") || strings.Contains(sql, "/*M:32a:") {
		t.Fatalf("no AUTO_INCREMENT=N was declared, but a floor was emitted anyway: %q", sql)
	}
}
