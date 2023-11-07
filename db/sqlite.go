package db

import (
	"context"
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
	"github.com/maxpert/marmot/pool"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

const snapshotTransactionMode = "exclusive"

var PoolSize = 4
var MarmotPrefix = "__marmot__"

type statsSqliteStreamDB struct {
	published      telemetry.Counter
	pendingPublish telemetry.Gauge
	countChanges   telemetry.Histogram
	scanChanges    telemetry.Histogram
}

type SqliteStreamDB struct {
	OnChange      func(event *ChangeLogEvent) error
	pool          *pool.SQLitePool
	rawConnection *sqlite3.SQLiteConn
	publishLock   *sync.Mutex

	dbPath            string
	prefix            string
	watchTablesSchema map[string][]*ColumnInfo
	stats             *statsSqliteStreamDB
}

type ColumnInfo struct {
	Name            string `db:"name"`
	Type            string `db:"type"`
	NotNull         bool   `db:"notnull"`
	DefaultValue    any    `db:"dflt_value"`
	PrimaryKeyIndex int    `db:"pk"`
	IsPrimaryKey    bool
}

func RestoreFrom(destPath, bkFilePath string) error {
	dnsTpl := "%s?_journal_mode=WAL&_foreign_keys=false&_busy_timeout=30000&_sync=FULL&_txlock=%s"
	dns := fmt.Sprintf(dnsTpl, destPath, snapshotTransactionMode)
	destDB, dest, err := pool.OpenRaw(dns)
	if err != nil {
		return err
	}
	defer dest.Close()

	dns = fmt.Sprintf(dnsTpl, bkFilePath, snapshotTransactionMode)
	srcDB, src, err := pool.OpenRaw(dns)
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
			err = copyFile(destPath, bkFilePath)
			if err != nil {
				return err
			}

			err = copyFile(destPath+"-shm", bkFilePath+"-shm")
			if err != nil {
				return err
			}

			err = copyFile(destPath+"-wal", bkFilePath+"-wal")
			if err != nil {
				return err
			}

			return nil
		})
	})

	if err != nil {
		return err
	}

	err = performCheckpoint(dgSQL)
	if err != nil {
		return err
	}

	return nil
}

func GetAllDBTables(path string) ([]string, error) {
	connectionStr := fmt.Sprintf("%s?_journal_mode=WAL", path)
	conn, rawConn, err := pool.OpenRaw(connectionStr)
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
	dbPool, err := pool.NewSQLitePool(fmt.Sprintf("%s?_journal_mode=WAL", path), PoolSize, true)
	if err != nil {
		return nil, err
	}

	conn, err := dbPool.Borrow()
	if err != nil {
		return nil, err
	}
	defer conn.Return()

	err = performCheckpoint(conn.DB())
	if err != nil {
		return nil, err
	}

	ret := &SqliteStreamDB{
		pool:              dbPool,
		dbPath:            path,
		prefix:            MarmotPrefix,
		publishLock:       &sync.Mutex{},
		watchTablesSchema: map[string][]*ColumnInfo{},
		stats: &statsSqliteStreamDB{
			published:      telemetry.NewCounter("published", "number of rows published"),
			pendingPublish: telemetry.NewGauge("pending_publish", "rows pending publishing"),
			countChanges:   telemetry.NewHistogram("count_changes", "latency counting changes in microseconds"),
			scanChanges:    telemetry.NewHistogram("scan_changes", "latency scanning change rows in DB"),
		},
	}

	return ret, nil
}

func (conn *SqliteStreamDB) InstallCDC(tables []string) error {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return err
	}
	defer sqlConn.Return()

	err = sqlConn.DB().WithTx(func(tx *goqu.TxDatabase) error {
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

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	go conn.watchChanges(watcher, conn.dbPath)
	return nil
}

func (conn *SqliteStreamDB) RemoveCDC(tables bool) error {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return err
	}
	defer sqlConn.Return()

	log.Info().Msg("Uninstalling all CDC triggers...")
	err = removeMarmotTriggers(sqlConn.DB(), conn.prefix)
	if err != nil {
		return err
	}

	if tables {
		return removeMarmotTables(sqlConn.DB(), conn.prefix)
	}

	return nil
}

func (conn *SqliteStreamDB) installChangeLogTriggers() error {
	if err := conn.initGlobalChangeLog(); err != nil {
		return err
	}

	for tableName := range conn.watchTablesSchema {
		err := conn.initTriggers(tableName)
		if err != nil {
			return err
		}
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
		err = rows.Scan(&c.Name, &c.Type, &c.NotNull, &c.DefaultValue, &c.PrimaryKeyIndex)
		if err != nil {
			return nil, err
		}

		c.IsPrimaryKey = c.PrimaryKeyIndex > 0

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
	sqlDB, rawDB, err := pool.OpenRaw(fmt.Sprintf("%s?mode=ro&_foreign_keys=false&_journal_mode=WAL", conn.dbPath))
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	defer rawDB.Close()

	_, err = rawDB.Exec("VACUUM main INTO ?;", []driver.Value{bkFilePath})
	if err != nil {
		return err
	}

	err = rawDB.Close()
	if err != nil {
		return err
	}

	err = sqlDB.Close()
	if err != nil {
		return err
	}

	// Now since we have separate copy of DB we don't need to deal with WAL journals or foreign keys
	// We need to remove all the marmot specific tables, triggers, and vacuum out the junk.
	sqlDB, rawDB, err = pool.OpenRaw(fmt.Sprintf("%s?_foreign_keys=false&_journal_mode=TRUNCATE", bkFilePath))
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

	_, err = gSQL.Exec("VACUUM;")
	if err != nil {
		return err
	}

	return nil
}

func (conn *SqliteStreamDB) GetRawConnection() *sqlite3.SQLiteConn {
	return conn.rawConnection
}

func (conn *SqliteStreamDB) GetPath() string {
	return conn.dbPath
}

func (conn *SqliteStreamDB) WithReadTx(cb func(tx *sql.Tx) error) error {
	var tx *sql.Tx = nil
	db, _, err := pool.OpenRaw(fmt.Sprintf("%s?_journal_mode=WAL", conn.dbPath))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if r := recover(); r != nil {
			log.Error().Any("recover", r).Msg("Recovered read transaction")
		}

		if tx != nil {
			err = tx.Rollback()
			if err != nil {
				log.Error().Err(err).Msg("Error performing read transaction")
			}
		}

		db.Close()
		cancel()
	}()

	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	return cb(tx)
}

func copyFile(toPath, fromPath string) error {
	fi, err := os.OpenFile(fromPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer fi.Close()

	fo, err := os.OpenFile(toPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0)
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
	log.Debug().Msg("Forcing WAL checkpoint")

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

			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}
