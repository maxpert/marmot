package handlers

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// fakeDatabaseProvider serves one in-memory SQLite database.
type fakeDatabaseProvider struct{ db *sql.DB }

func (f fakeDatabaseProvider) ListDatabases() []string         { return []string{"testdb"} }
func (f fakeDatabaseProvider) DatabaseExists(name string) bool { return name == "testdb" }
func (f fakeDatabaseProvider) GetDatabaseReadConnection(string) (*sql.DB, error) {
	return f.db, nil
}

// TestShowCreateTableStripsWidthMarker pins what a client sees. SHOW CREATE
// TABLE hands sqlite_master.sql back verbatim, so without stripping, Marmot's
// own width bookkeeping would appear in every schema dump - and a tool that
// round-trips the output would feed the comment back in.
//
// Mutation: return createSQL instead of intmarker.Strip(createSQL); the first
// assertion fires.
func TestShowCreateTableStripsWidthMarker(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const ddl = "CREATE TABLE marked (id INTEGER /*M:32ua*/ PRIMARY KEY, qty INTEGER /*M:16*/, v TEXT)"
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Precondition: SQLite really did keep the marker, or the assertion below
	// would pass for the wrong reason.
	var stored string
	if err := db.QueryRow("SELECT sql FROM sqlite_master WHERE name='marked'").Scan(&stored); err != nil {
		t.Fatalf("SELECT sql: %v", err)
	}
	if !strings.Contains(stored, "/*M:") {
		t.Fatalf("fixture: SQLite did not preserve the marker in sqlite_master: %q", stored)
	}

	handler := NewMetadataHandler(fakeDatabaseProvider{db: db}, "__marmot_system")
	rs, err := handler.HandleShowCreateTable("testdb", "marked")
	if err != nil {
		t.Fatalf("HandleShowCreateTable: %v", err)
	}
	if len(rs.Rows) != 1 || len(rs.Rows[0]) != 2 {
		t.Fatalf("result shape = %d rows, want one row of two columns", len(rs.Rows))
	}

	shown, ok := rs.Rows[0][1].(string)
	if !ok {
		t.Fatalf("Create Table column is %T, want string", rs.Rows[0][1])
	}
	if strings.Contains(shown, "/*M:") {
		t.Errorf("SHOW CREATE TABLE leaked a width marker to the client:\n%s", shown)
	}
	// The rest of the DDL must survive intact, or stripping has eaten schema.
	// Mutation: strip everything between the first "/*" and the last "*/".
	for _, want := range []string{"CREATE TABLE marked", "id INTEGER PRIMARY KEY", "qty INTEGER", "v TEXT"} {
		if !strings.Contains(shown, want) {
			t.Errorf("stripped DDL lost %q:\n%s", want, shown)
		}
	}
}
