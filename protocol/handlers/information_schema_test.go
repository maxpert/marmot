package handlers

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

// metadataDeadline bounds every metadata call in these tests. A metadata read
// that waits on a connection never returns, so a missed deadline is the
// failure being tested for, not slowness.
const metadataDeadline = 5 * time.Second

// onePoolProvider serves one file database whose read pool has exactly one
// connection, the smallest pool a node can be configured with. A metadata
// handler that issues a query while another's rows are open waits forever for
// that connection, which is the F-P1 wedge.
type onePoolProvider struct {
	read *sql.DB
}

func (p onePoolProvider) ListDatabases() []string         { return []string{"testdb"} }
func (p onePoolProvider) DatabaseExists(name string) bool { return name == "testdb" }
func (p onePoolProvider) GetDatabaseReadConnection(name string) (*sql.DB, error) {
	return p.read, nil
}

func newOnePoolHandler(t *testing.T) *MetadataHandler {
	t.Helper()
	path := filepath.Join(t.TempDir(), "testdb.db")

	setup, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	require.NoError(t, err)
	for _, ddl := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT DEFAULT 'none')",
		"CREATE INDEX idx_users_email ON users(email)",
		"CREATE TABLE posts (id INTEGER PRIMARY KEY, body TEXT)",
		"CREATE TABLE __marmot__bookkeeping (k TEXT)",
	} {
		_, err := setup.Exec(ddl)
		require.NoError(t, err)
	}
	require.NoError(t, setup.Close())

	read, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	require.NoError(t, err)
	read.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = read.Close() })

	return NewMetadataHandler(onePoolProvider{read: read}, "__marmot_system")
}

// answerWithin runs one metadata call and fails the test if it has not
// answered by metadataDeadline.
func answerWithin(t *testing.T, name string, call func() (*protocol.ResultSet, error)) *protocol.ResultSet {
	t.Helper()
	type answer struct {
		rs  *protocol.ResultSet
		err error
	}
	done := make(chan answer, 1)
	go func() {
		rs, err := call()
		done <- answer{rs, err}
	}()
	select {
	case a := <-done:
		require.NoError(t, a.err, name)
		return a.rs
	case <-time.After(metadataDeadline):
		t.Fatalf("%s did not answer within %s: a metadata query is waiting on a connection another query holds", name, metadataDeadline)
		return nil
	}
}

func isStatement(table protocol.InformationSchemaTableType, schema, tableName string) protocol.Statement {
	return protocol.Statement{
		Type:        protocol.StatementInformationSchema,
		ISTableType: table,
		ISFilter:    protocol.InformationSchemaFilter{SchemaName: schema, TableName: tableName},
	}
}

// TestMetadataNeverWaitsOnItself pins the root cause of F-P1: every
// INFORMATION_SCHEMA table and SHOW command answers on a single connection,
// with and without filters, and keeps answering afterwards. Each answer's
// shape is the one the text and prepared paths both send.
//
// Mutations: in informationSchemaColumns, query each table's columns while
// the table list's rows are still open (the old all-tables loop); or in
// HandleShowIndexes, read index_info inside the index_list rows loop. The
// unfiltered COLUMNS row, or the STATISTICS and SHOW INDEX rows, then miss
// their deadline.
func TestMetadataNeverWaitsOnItself(t *testing.T) {
	h := newOnePoolHandler(t)

	tests := []struct {
		name     string
		call     func() (*protocol.ResultSet, error)
		wantCols int
		wantRows int
	}{
		{"TABLES of the session db", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableTables, "", ""))
		}, 21, 2},
		{"TABLES filtered by schema and table", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("", isStatement(protocol.ISTableTables, "testdb", "users"))
		}, 21, 1},
		{"TABLES with no database: every schema, same columns", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("", isStatement(protocol.ISTableTables, "", ""))
		}, 21, 2},
		{"TABLES of an unknown schema is empty", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableTables, "nosuch", ""))
		}, 21, 0},
		{"TABLES of the system database is empty", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableTables, "__marmot_system", ""))
		}, 21, 0},
		{"COLUMNS of every table", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableColumns, "", ""))
		}, 20, 5},
		{"COLUMNS of one table", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("", isStatement(protocol.ISTableColumns, "testdb", "users"))
		}, 20, 3},
		{"COLUMNS of an unknown table", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableColumns, "", "nosuch"))
		}, 20, 0},
		{"SCHEMATA", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("", isStatement(protocol.ISTableSchemata, "", ""))
		}, 5, 1},
		{"SCHEMATA filtered", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("", isStatement(protocol.ISTableSchemata, "nosuch", ""))
		}, 5, 0},
		{"STATISTICS of one table", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableStatistics, "", "users"))
		}, 8, 1},
		{"STATISTICS without a table", func() (*protocol.ResultSet, error) {
			return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableStatistics, "", ""))
		}, 8, 0},
		{"SHOW TABLES", func() (*protocol.ResultSet, error) { return h.HandleShowTables("testdb", "") }, 1, 2},
		{"SHOW TABLES LIKE", func() (*protocol.ResultSet, error) { return h.HandleShowTables("testdb", "u%") }, 1, 1},
		{"SHOW COLUMNS", func() (*protocol.ResultSet, error) { return h.HandleShowColumns("testdb", "users") }, 6, 3},
		{"SHOW INDEX", func() (*protocol.ResultSet, error) { return h.HandleShowIndexes("testdb", "users") }, 13, 1},
		{"SHOW TABLE STATUS", func() (*protocol.ResultSet, error) { return h.HandleShowTableStatus("testdb", "") }, 18, 2},
	}

	// Twice over: a wedged connection shows as the first answer passing and
	// every later one timing out.
	for round := 0; round < 2; round++ {
		for _, tt := range tests {
			rs := answerWithin(t, tt.name, tt.call)
			require.Len(t, rs.Columns, tt.wantCols, tt.name)
			require.Len(t, rs.Rows, tt.wantRows, tt.name)
			for _, row := range rs.Rows {
				require.Len(t, row, tt.wantCols, "%s: row width must match its columns", tt.name)
			}
		}
	}
}

// TestInformationSchemaRowValues pins the values a client reads from the
// rows the tests above count.
func TestInformationSchemaRowValues(t *testing.T) {
	h := newOnePoolHandler(t)

	cols := answerWithin(t, "COLUMNS users", func() (*protocol.ResultSet, error) {
		return h.HandleInformationSchema("", isStatement(protocol.ISTableColumns, "testdb", "users"))
	})
	require.Equal(t, []interface{}{"def", "testdb", "users", "id", 1, nil, "YES", "int", nil, nil, nil, nil, nil,
		"utf8mb4", "utf8mb4_general_ci", "int", "PRI", "", "select,insert,update,references", ""}, cols.Rows[0])
	require.Equal(t, "name", cols.Rows[1][3])
	require.Equal(t, "NO", cols.Rows[1][6])
	require.Equal(t, "'none'", cols.Rows[2][5])

	idx := answerWithin(t, "STATISTICS users", func() (*protocol.ResultSet, error) {
		return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableStatistics, "", "users"))
	})
	require.Equal(t, []interface{}{"def", "testdb", "users", 1, "testdb", "idx_users_email", 1, "email"}, idx.Rows[0])

	tables := answerWithin(t, "TABLES", func() (*protocol.ResultSet, error) {
		return h.HandleInformationSchema("testdb", isStatement(protocol.ISTableTables, "", ""))
	})
	require.Equal(t, "posts", tables.Rows[0][2])
	require.Equal(t, "users", tables.Rows[1][2])
	require.Equal(t, "BASE TABLE", tables.Rows[1][3])
}

// TestTableNamesAreBoundNotSpliced pins that a table name reaches SQLite as a
// bound value, never as SQL text: a filter value is client input, and every
// prepared INFORMATION_SCHEMA query now delivers one. A name SQLite cannot
// parse as SQL answers as the unknown table it is.
//
// Mutation: build "PRAGMA table_info(%s)" with fmt.Sprintf again; the
// statement fails to parse and the call errors.
func TestTableNamesAreBoundNotSpliced(t *testing.T) {
	h := newOnePoolHandler(t)
	for _, name := range []string{"o'brien", "users); DROP TABLE posts; --"} {
		cols := answerWithin(t, "SHOW COLUMNS "+name, func() (*protocol.ResultSet, error) { return h.HandleShowColumns("testdb", name) })
		require.Empty(t, cols.Rows, name)
		idx := answerWithin(t, "SHOW INDEX "+name, func() (*protocol.ResultSet, error) { return h.HandleShowIndexes("testdb", name) })
		require.Empty(t, idx.Rows, name)
	}
	tables := answerWithin(t, "SHOW TABLES", func() (*protocol.ResultSet, error) { return h.HandleShowTables("testdb", "") })
	require.Len(t, tables.Rows, 2)
}
