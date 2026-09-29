package test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestClusterReplication: CREATE TABLE, INSERTs, UPDATEs and DELETEs, each
// through a different node, leave exactly the same rows on every node.
func TestClusterReplication(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "users", "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, balance INT)")
	const q = "SELECT id, name, balance FROM users ORDER BY id"

	for i := 1; i <= 10; i++ {
		c.mustExec(1, "marmot", "INSERT INTO users (id, name, balance) VALUES (?, ?, ?)", i, fmt.Sprintf("user%d", i), i*100)
	}
	rows := func(balances map[int]int, ids ...int) []string {
		var want []string
		for _, id := range ids {
			want = append(want, fmt.Sprintf("%d|user%d|%d", id, id, balances[id]))
		}
		return want
	}
	balances := map[int]int{}
	for i := 1; i <= 10; i++ {
		balances[i] = i * 100
	}
	c.waitRows("marmot", q, rows(balances, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1, 2, 3)

	for i := 1; i <= 5; i++ {
		c.mustExec(2, "marmot", "UPDATE users SET balance = ? WHERE id = ?", i*200, i)
		balances[i] = i * 200
	}
	c.waitRows("marmot", q, rows(balances, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1, 2, 3)

	for i := 8; i <= 10; i++ {
		c.mustExec(3, "marmot", "DELETE FROM users WHERE id = ?", i)
	}
	c.waitRows("marmot", q, rows(balances, 1, 2, 3, 4, 5, 6, 7), 1, 2, 3)
}

// TestClusterLoadDataLocalReplication: rows a LOAD DATA LOCAL INFILE loads
// through one node reach every node exactly.
func TestClusterLoadDataLocalReplication(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "bulk_users", "CREATE TABLE bulk_users (id INT PRIMARY KEY, name TEXT, score INT)")

	csvPath := filepath.Join(testDir(t), "bulk_users.csv")
	csv := "id,name,score\n1,alice,100\n2,bob,200\n3,charlie,300\n4,dana,400\n5,eve,500\n"
	if err := os.WriteFile(csvPath, []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}
	loadConn, err := sql.Open("mysql", c.dsn(1, "marmot")+"&allowAllFiles=true")
	if err != nil {
		t.Fatal(err)
	}
	defer loadConn.Close()
	res, err := loadConn.Exec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE bulk_users FIELDS TERMINATED BY ',' "+
		"LINES TERMINATED BY '\\n' IGNORE 1 LINES (id,name,score)", csvPath))
	if err != nil {
		t.Fatalf("LOAD DATA LOCAL INFILE: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n <= 0 {
		t.Fatalf("LOAD DATA reported %d rows affected (err=%v), want some", n, err)
	}
	want := []string{"1|alice|100", "2|bob|200", "3|charlie|300", "4|dana|400", "5|eve|500"}
	c.waitRows("marmot", "SELECT id, name, score FROM bulk_users ORDER BY id", want, 1, 2, 3)
}

// TestClusterLoadDataLocalNarrowReportsEveryRow: a LOAD DATA LOCAL INFILE into
// a narrow AUTO_INCREMENT table runs as INSERTs through the coordinator
// (7a66596), which reports exactly the rows loaded, and every node ends with
// them under distinct generated ids.
func TestClusterLoadDataLocalNarrowReportsEveryRow(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.createTable(1, "marmot", "bulk_narrow", "CREATE TABLE bulk_narrow (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT, score INT)")
	csvPath := filepath.Join(testDir(t), "bulk_narrow.csv")
	if err := os.WriteFile(csvPath, []byte("name,score\nalice,100\nbob,200\ncharlie,300\ndana,400\neve,500\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	loadConn, err := sql.Open("mysql", c.dsn(1, "marmot")+"&allowAllFiles=true")
	if err != nil {
		t.Fatal(err)
	}
	defer loadConn.Close()
	res, err := loadConn.Exec(fmt.Sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE bulk_narrow FIELDS TERMINATED BY ',' "+
		"LINES TERMINATED BY '\\n' IGNORE 1 LINES (name,score)", csvPath))
	if err != nil {
		t.Fatalf("LOAD DATA LOCAL INFILE: %v", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 5 {
		t.Fatalf("LOAD DATA reported %d rows affected (err=%v), want 5", n, err)
	}
	want := []string{"alice|100", "bob|200", "charlie|300", "dana|400", "eve|500"}
	c.waitRows("marmot", "SELECT name, score FROM bulk_narrow ORDER BY name", want, 1, 2, 3)
	c.waitRows("marmot", "SELECT COUNT(DISTINCT id) FROM bulk_narrow", []string{"5"}, 1, 2, 3)
}
