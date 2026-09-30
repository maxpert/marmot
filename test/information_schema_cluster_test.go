package test

// INFORMATION_SCHEMA over the real MySQL wire protocol, text and prepared,
// against a 3-node cluster. Every query carries its own timeout: the
// failure these tests exist for is a query that never answers.

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

// isAnswer is one INFORMATION_SCHEMA answer, normalised so the text
// protocol's strings and the binary protocol's typed values compare equal.
type isAnswer struct {
	columns []string
	rows    [][]string
}

// queryAnswer runs one query under clientTimeout. With args, the driver
// prepares it on the server (COM_STMT_PREPARE/EXECUTE); without, it is a text
// query.
func queryAnswer(conn *sql.DB, query string, args ...interface{}) (isAnswer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	defer clientCalls.Add(1)
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return isAnswer{}, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return isAnswer{}, err
	}
	answer := isAnswer{columns: columns}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return isAnswer{}, err
		}
		row := make([]string, len(values))
		for i, v := range values {
			switch x := v.(type) {
			case nil:
				row[i] = "NULL"
			case []byte:
				row[i] = string(x)
			case int64:
				row[i] = strconv.FormatInt(x, 10)
			default:
				row[i] = fmt.Sprint(x)
			}
		}
		answer.rows = append(answer.rows, row)
	}
	return answer, rows.Err()
}

// TestInformationSchemaAnswersOnEveryPath checks end to end that every
// INFORMATION_SCHEMA table Marmot serves answers,
// with and without filters, the same rows and columns to a prepared
// statement as to the text query, on every node; and DDL coordinated from
// the node afterwards completes. Before, a prepared COLUMNS or TABLES query
// never answered and took the database's only write connection with it, so
// the DDL hung; a prepared query that did answer came back as an empty OK.
// The existence probe migration tools send (table_schema = ? AND table_name
// = ?) is among them.
//
// Mutations: the metadata handlers read through the write handle with the
// nested table loop (the 6cf7e580 handlers) - the unfiltered COLUMNS query
// times out; a prepared statement answers a result set only for SELECT - the
// prepared answers come back with no columns.
func TestInformationSchemaAnswersOnEveryPath(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createDatabase(1, "isdb")
	for _, ddl := range []string{
		"CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) NOT NULL, email VARCHAR(128))",
		"CREATE INDEX idx_users_email ON users (email)",
		"CREATE TABLE posts (id BIGINT PRIMARY KEY, body TEXT)",
	} {
		c.mustExec(1, "isdb", ddl)
	}
	c.waitTable("isdb", "posts", 1, 2, 3)
	cases := []struct {
		name     string
		database string
		text     string
		prepared string
		args     []interface{}
		wantRows int
	}{
		{"COLUMNS of one table", "marmot",
			"SELECT * FROM information_schema.columns WHERE table_schema = 'isdb' AND table_name = 'users'",
			"SELECT * FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
			[]interface{}{"isdb", "users"}, 3},
		{"COLUMNS of every table", "marmot",
			"SELECT * FROM information_schema.columns WHERE table_schema = 'isdb'",
			"SELECT * FROM information_schema.columns WHERE table_schema = ?",
			[]interface{}{"isdb"}, 5},
		{"TABLES of a schema", "marmot",
			"SELECT * FROM information_schema.tables WHERE table_schema = 'isdb'",
			"SELECT * FROM information_schema.tables WHERE table_schema = ?",
			[]interface{}{"isdb"}, 2},
		{"TABLES existence probe", "marmot",
			"SELECT table_name FROM information_schema.tables WHERE table_schema = 'isdb' AND table_name = 'users'",
			"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			[]interface{}{"isdb", "users"}, 1},
		{"TABLES existence probe, absent table", "marmot",
			"SELECT table_name FROM information_schema.tables WHERE table_schema = 'isdb' AND table_name = 'nosuch'",
			"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			[]interface{}{"isdb", "nosuch"}, 0},
		{"TABLES of the session database", "isdb",
			"SELECT * FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
			"SELECT * FROM information_schema.tables WHERE table_type = ?",
			[]interface{}{"BASE TABLE"}, 2},
		{"SCHEMATA", "marmot",
			"SELECT * FROM information_schema.schemata WHERE schema_name = 'isdb'",
			"SELECT * FROM information_schema.schemata WHERE schema_name = ?",
			[]interface{}{"isdb"}, 1},
		{"STATISTICS", "marmot",
			"SELECT * FROM information_schema.statistics WHERE table_schema = 'isdb' AND table_name = 'users'",
			"SELECT * FROM information_schema.statistics WHERE table_schema = ? AND table_name = ?",
			[]interface{}{"isdb", "users"}, 1},
	}

	for _, nodeID := range []int{1, 2, 3} {
		// Prepared first: before the fix the first prepared query wedged the
		// node, so every later query on it would time out.
		for _, tc := range cases {
			conn := c.db(nodeID, tc.database)
			prepared, err := queryAnswer(conn, tc.prepared, tc.args...)
			if err != nil {
				t.Fatalf("node %d %s, prepared: %v", nodeID, tc.name, err)
			}
			text, err := queryAnswer(conn, tc.text)
			if err != nil {
				t.Fatalf("node %d %s, text: %v", nodeID, tc.name, err)
			}
			if len(text.rows) != tc.wantRows {
				t.Fatalf("node %d %s: text answered %d rows, want %d: %v", nodeID, tc.name, len(text.rows), tc.wantRows, text.rows)
			}
			if len(prepared.columns) == 0 {
				t.Fatalf("node %d %s: the prepared answer has no columns", nodeID, tc.name)
			}
			if !reflect.DeepEqual(prepared, text) {
				t.Fatalf("node %d %s: prepared and text answers differ\nprepared: %v %v\ntext:     %v %v",
					nodeID, tc.name, prepared.columns, prepared.rows, text.columns, text.rows)
			}
		}

		// DDL coordinated from the node that just answered them, on the
		// database they read, completes. An index on posts leaves every answer
		// above unchanged for the next node.
		ddl := fmt.Sprintf("CREATE INDEX after_is_%d ON posts (body)", nodeID)
		if _, err := c.exec(nodeID, "isdb", ddl); err != nil {
			t.Fatalf("node %d: %s after INFORMATION_SCHEMA queries: %v", nodeID, ddl, err)
		}
	}
}
