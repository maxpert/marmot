package db

import (
	"testing"

	"github.com/maxpert/marmot/hlc"
)

// BenchmarkUserDatabaseWriteCommit measures one autocommit write on a user
// database's write pool: the path every SQLite commit takes, and so every
// call of the write gate's commit hook.
func BenchmarkUserDatabaseWriteCommit(b *testing.B) {
	dm, err := NewDatabaseManager(b.TempDir(), 1, hlc.NewClock(1))
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()
	if err := dm.CreateDatabase("app"); err != nil {
		b.Fatal(err)
	}
	app, err := dm.GetDatabase("app")
	if err != nil {
		b.Fatal(err)
	}
	writeDB := app.GetWriteDB()
	if _, err := writeDB.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeDB.Exec("INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)", i%1024, i); err != nil {
			b.Fatal(err)
		}
	}
}
