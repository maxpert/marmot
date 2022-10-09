package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/fsnotify/fsnotify"
	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

const snapshotTransactionMode = "exclusive"

var MarmotPrefix = "__marmot__"

type SqliteStreamDB struct {
	*goqu.Database
	OnChange      func(event *ChangeLogEvent) error
	rawConnection *sqlite3.SQLiteConn
	watcher       *fsnotify.Watcher
	publishLock   *sync.Mutex

	dbPath            string
	prefix            string
	watchTablesSchema map[string][]*ColumnInfo
}

type ColumnInfo struct {
	Name         string `db:"name"`
	Type         string `db:"type"`
	NotNull      bool   `db:"notnull"`
	DefaultValue any    `db:"dflt_value"`
	IsPrimaryKey bool   `db:"pk"`
}

func GetAllDBTables(path string) ([]string, error) {
	connectionStr := fmt.Sprintf("%s?_journal_mode=wal", path)
	conn, rawConn, err := OpenRaw(connectionStr)
	if err != nil {
		return nil, err
	}
	defer rawConn.Close()
	defer conn.Close()

	gSQL := goqu.New("sqlite", conn)
	names := make([]string, 0)
	err = gSQL.WithTx(func(tx *goqu.TxDatabase) error {
		return listDBTables(&names, tx)
	})

	if err != nil {
		return nil, err
	}

	return names, nil
}

func OpenStreamDB(path string) (*SqliteStreamDB, error) {
	ret := &SqliteStreamDB{
		Database:          nil,
		rawConnection:     nil,
		watcher:           nil,
		dbPath:            path,
		prefix:            MarmotPrefix,
		publishLock:       &sync.Mutex{},
		watchTablesSchema: map[string][]*ColumnInfo{},
	}

	err := ret.cycleDBConnection()
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func OpenRaw(dns string) (*sql.DB, *sqlite3.SQLiteConn, error) {
	var rawConn *sqlite3.SQLiteConn
	d := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			rawConn = conn
			return conn.RegisterFunc("marmot_version", func() string {
				return "0.1"
			}, true)
		},
	}

	conn := sql.OpenDB(SqliteDriverConnector{driver: d, dns: dns})
	err := conn.Ping()
	if err != nil {
		return nil, nil, err
	}

	return conn, rawConn, nil
}

func (conn *SqliteStreamDB) InstallCDC(tables []string) error {
	err := conn.WithTx(func(tx *goqu.TxDatabase) error {
		for _, n := range tables {
			colInfo, err := getTableInfo(tx, n)
			if err != nil {
				return err
			}

			conn.watchTablesSchema[n] = colInfo
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = conn.installChangeLogTriggers()
	if err != nil {
		return err
	}

	go conn.watchChanges(conn.dbPath)
	return nil
}

func (conn *SqliteStreamDB) installChangeLogTriggers() error {
	for tableName := range conn.watchTablesSchema {
		err := conn.initTriggers(tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *SqliteStreamDB) RemoveCDC(tables bool) error {
	log.Info().Msg("Uninstalling all CDC triggers...")
	err := removeMarmotTriggers(conn.Database, conn.prefix)
	if err != nil {
		return err
	}

	if tables {
		return removeMarmotTables(conn.Database, conn.prefix)
	}

	return nil
}

func (conn *SqliteStreamDB) Execute(query string) error {
	st, err := conn.Prepare(query)
	if err != nil {
		return err
	}

	stmt := &EnhancedStatement{st}
	defer stmt.Finalize()

	if _, err := stmt.Exec(); err != nil {
		return err
	}

	return nil
}

func getTableInfo(tx *goqu.TxDatabase, table string) ([]*ColumnInfo, error) {
	query := "SELECT name, type, `notnull`, dflt_value, pk FROM pragma_table_info(?)"
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(table)
	if err != nil {
		return nil, err
	}

	tableInfo := make([]*ColumnInfo, 0)
	hasPrimaryKey := false
	for rows.Next() {
		if rows.Err() != nil {
			return nil, rows.Err()
		}

		c := ColumnInfo{}
		err = rows.Scan(&c.Name, &c.Type, &c.NotNull, &c.DefaultValue, &c.IsPrimaryKey)
		if err != nil {
			return nil, err
		}

		if c.IsPrimaryKey {
			hasPrimaryKey = true
		}

		tableInfo = append(tableInfo, &c)
	}

	if !hasPrimaryKey {
		tableInfo = append(tableInfo, &ColumnInfo{
			Name:         "rowid",
			IsPrimaryKey: true,
			Type:         "INT",
			NotNull:      true,
			DefaultValue: nil,
		})
	}

	return tableInfo, nil
}

func (conn *SqliteStreamDB) BackupTo(bkFilePath string) error {
	sqlDB, src, err := OpenRaw(fmt.Sprintf("%s?mode=ro&_foreign_keys=false&_journal_mode=wal", conn.dbPath))
	if err != nil {
		return err
	}
	defer src.Close()

	_, err = src.Exec("VACUUM main INTO ?;", []driver.Value{bkFilePath})
	if err != nil {
		return err
	}

	err = src.Close()
	if err != nil {
		return err
	}

	sqlDB, src, err = OpenRaw(fmt.Sprintf("%s?_foreign_keys=false&_journal_mode=wal", bkFilePath))
	if err != nil {
		return err
	}

	gSQL := goqu.New("sqlite", sqlDB)
	err = removeMarmotTriggers(gSQL, conn.prefix)
	if err != nil {
		return err
	}

	err = removeMarmotTables(gSQL, conn.prefix)
	if err != nil {
		return err
	}

	return nil
}

func (conn *SqliteStreamDB) RestoreFrom(bkFilePath string) error {
	dnsTpl := "%s?_journal_mode=wal&_foreign_keys=false&_busy_timeout=30000&_txlock=%s"
	dns := fmt.Sprintf(dnsTpl, conn.dbPath, snapshotTransactionMode)
	destDB, dest, err := OpenRaw(dns)
	if err != nil {
		return err
	}
	defer dest.Close()

	dns = fmt.Sprintf(dnsTpl, bkFilePath, snapshotTransactionMode)
	srcDB, src, err := OpenRaw(dns)
	if err != nil {
		return err
	}
	defer src.Close()

	dgSQL := goqu.New("sqlite", destDB)
	sgSQL := goqu.New("sqlite", srcDB)

	// Source locking is required so that any lock related metadata is mirrored in destination
	// Transacting on both src and dest in immediate mode makes sure nobody
	// else is modifying or interacting with DB
	err = sgSQL.WithTx(func(dtx *goqu.TxDatabase) error {
		return dgSQL.WithTx(func(_ *goqu.TxDatabase) error {
			return copyFile(conn.dbPath, bkFilePath)
		})
	})

	if err != nil {
		return err
	}

	err = performCheckpoint(dgSQL)
	if err != nil {
		return err
	}

	return conn.cycleDBConnection()
}

func (conn *SqliteStreamDB) GetRawConnection() *sqlite3.SQLiteConn {
	return conn.rawConnection
}

func (conn *SqliteStreamDB) GetPath() string {
	return conn.dbPath
}

func (conn *SqliteStreamDB) cycleDBConnection() error {
	connectionStr := fmt.Sprintf("%s?_journal_mode=wal", conn.dbPath)
	sqlC, rawConn, err := OpenRaw(connectionStr)
	if err != nil {
		return err
	}

	sqlC.SetConnMaxLifetime(0)
	sqlC.SetConnMaxIdleTime(10 * time.Second)

	if conn.rawConnection != nil {
		err = conn.rawConnection.Close()
	}

	conn.Database = goqu.New("sql", sqlC)
	conn.rawConnection = rawConn

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	err = watcher.Add(conn.dbPath)
	if err != nil {
		return err
	}

	if conn.watcher != nil {
		conn.watcher.Close()
	}
	conn.watcher = watcher

	return performCheckpoint(conn.Database)
}

func copyFile(toPath, fromPath string) error {
	fi, err := os.OpenFile(fromPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer fi.Close()

	fo, err := os.OpenFile(toPath, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	defer fo.Close()

	bytesWritten, err := io.Copy(fo, fi)
	log.Debug().
		Int64("bytes", bytesWritten).
		Str("from", fromPath).
		Str("to", toPath).
		Msg("File copied...")
	return err
}

func listDBTables(names *[]string, gSQL *goqu.TxDatabase) error {
	err := gSQL.Select("name").From("sqlite_schema").Where(
		goqu.C("type").Eq("table"),
		goqu.C("name").NotLike("sqlite_%"),
		goqu.C("name").NotLike(MarmotPrefix+"%"),
	).ScanVals(names)

	if err != nil {
		return err
	}

	return nil
}

func performCheckpoint(gSQL *goqu.Database) error {
	rBusy, rLog, rCheckpoint := int64(1), int64(0), int64(0)
	for rBusy != 0 {
		row := gSQL.QueryRow("PRAGMA wal_checkpoint(truncate);")
		err := row.Scan(&rBusy, &rLog, &rCheckpoint)
		if err != nil {
			return err
		}

		if rBusy != 0 {
			log.Debug().
				Int64("busy", rBusy).
				Int64("log", rLog).
				Int64("checkpoint", rCheckpoint).
				Msg("Waiting checkpoint...")

			time.Sleep(500 * time.Millisecond)
		}
	}

	return nil
}
