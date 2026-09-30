package rules

import (
	"errors"
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/transform"
)

func TestClaimTableGuardRule_NameAndPriority(t *testing.T) {
	rule := &ClaimTableGuardRule{}
	if got := rule.Name(); got != "ClaimTableGuard" {
		t.Errorf("Name() = %q, want %q", got, "ClaimTableGuard")
	}
	// Must run before every other transform rule, so a rejection is decided
	// on the AST exactly as the client sent it.
	if got := rule.Priority(); got != 0 {
		t.Errorf("Priority() = %d, want 0", got)
	}
}

func TestClaimTableGuardRule_Transform(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantBlocked bool
	}{
		{name: "INSERT into claim table", sql: "INSERT INTO __marmot__autoinc (tbl, base, owner, granted_at) VALUES ('t', 1, 1, 1)", wantBlocked: true},
		{name: "REPLACE into claim table", sql: "REPLACE INTO __marmot__autoinc (tbl, base, owner, granted_at) VALUES ('t', 1, 1, 1)", wantBlocked: true},
		{name: "UPDATE claim table", sql: "UPDATE __marmot__autoinc SET base = 100 WHERE tbl = 't'", wantBlocked: true},
		{name: "DELETE from claim table", sql: "DELETE FROM __marmot__autoinc WHERE tbl = 't'", wantBlocked: true},
		{name: "DROP TABLE claim table", sql: "DROP TABLE __marmot__autoinc", wantBlocked: true},
		{name: "ALTER TABLE claim table", sql: "ALTER TABLE __marmot__autoinc ADD COLUMN extra INT", wantBlocked: true},
		{name: "TRUNCATE TABLE claim table", sql: "TRUNCATE TABLE __marmot__autoinc", wantBlocked: true},
		{name: "RENAME TABLE away from claim table", sql: "RENAME TABLE __marmot__autoinc TO some_table", wantBlocked: true},
		{name: "RENAME TABLE onto claim table", sql: "RENAME TABLE some_table TO __marmot__autoinc", wantBlocked: true},
		{name: "CREATE TABLE re-creating claim table", sql: "CREATE TABLE __marmot__autoinc (tbl TEXT PRIMARY KEY)", wantBlocked: true},
		{name: "case-varied claim table name", sql: "DELETE FROM __MARMOT__AUTOINC WHERE tbl = 't'", wantBlocked: true},
		{name: "backtick-quoted claim table name", sql: "DELETE FROM `__marmot__autoinc` WHERE tbl = 't'", wantBlocked: true},
		{name: "database-qualified claim table name", sql: "DELETE FROM `mydb`.`__marmot__autoinc` WHERE tbl = 't'", wantBlocked: true},

		{name: "SELECT from claim table is allowed", sql: "SELECT * FROM __marmot__autoinc", wantBlocked: false},
		{name: "ordinary table with similar prefix", sql: "DELETE FROM marmot_autoinc WHERE tbl = 't'", wantBlocked: false},
		{name: "ordinary table embedding the name", sql: "DELETE FROM x__marmot__autoincy WHERE tbl = 't'", wantBlocked: false},
		{name: "unrelated INSERT", sql: "INSERT INTO users (id, name) VALUES (1, 'a')", wantBlocked: false},
		{name: "unrelated UPDATE", sql: "UPDATE users SET name = 'b' WHERE id = 1", wantBlocked: false},
		{name: "unrelated DDL", sql: "DROP TABLE users", wantBlocked: false},
	}

	rule := &ClaimTableGuardRule{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)

			results, err := rule.Transform(stmt, nil, nil, "", nil)

			if !tt.wantBlocked {
				if !errors.Is(err, transform.ErrRuleNotApplicable) {
					t.Fatalf("Transform() error = %v, want ErrRuleNotApplicable", err)
				}
				if results != nil {
					t.Fatalf("Transform() results = %v, want nil", results)
				}
				return
			}

			var coded *transform.CodedError
			if !errors.As(err, &coded) {
				t.Fatalf("Transform() error = %v (%T), want *transform.CodedError", err, err)
			}
			if coded.Code != mysqlcode.ErrCodeTableAccessDenied {
				t.Errorf("Code = %d, want %d (ER_TABLEACCESS_DENIED_ERROR)", coded.Code, mysqlcode.ErrCodeTableAccessDenied)
			}
			if results != nil {
				t.Errorf("Transform() results = %v, want nil on rejection", results)
			}
		})
	}
}

// TestClaimTableGuardRule_ConstantIsSingleSource pins that the rule guards
// exactly common.AutoIncClaimTableName - the one Go literal for the claim
// table's name, shared with db.AutoIncClaimTable.
func TestClaimTableGuardRule_ConstantIsSingleSource(t *testing.T) {
	if common.AutoIncClaimTableName != "__marmot__autoinc" {
		t.Fatalf("common.AutoIncClaimTableName = %q, want __marmot__autoinc", common.AutoIncClaimTableName)
	}

	rule := &ClaimTableGuardRule{}
	sql := "DELETE FROM " + common.AutoIncClaimTableName + " WHERE tbl = 't'"
	_, err := rule.Transform(parseOne(t, sql), nil, nil, "", nil)
	var coded *transform.CodedError
	if !errors.As(err, &coded) {
		t.Fatalf("Transform() error = %v, want *transform.CodedError", err)
	}
}
