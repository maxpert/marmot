package cfg

import "testing"

// TestDDLImplicitCommitDefault pins the shipped default. MySQL has no
// transactional DDL: schema changes implicitly commit the open transaction, and
// clients written against MySQL depend on that, so it is the default here.
//
// This lives in cfg because Config holds the defaults directly; a test that
// changes the flag would otherwise be reading its own mutation back.
func TestDDLImplicitCommitDefault(t *testing.T) {
	if !Config.Transaction.DDLImplicitCommit {
		t.Fatal("ddl_implicit_commit must default to true to match MySQL semantics")
	}
}
