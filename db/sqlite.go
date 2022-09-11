package db

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/fsnotify/fsnotify"
	"github.com/fxamacker/cbor/v2"
	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

const restoreBackupSQL = `PRAGMA wal_checkpoint(TRUNCATE);
INSERT OR REPLACE INTO main.%[1]s(%[2]s) 
SELECT %[2]s FROM backup.%[1]s 
LIMIT %[3]d OFFSET %[4]d;`

const batchSize = 1000

type SqliteStreamDB struct {
	*goqu.Database
	rawConnection     *sqlite3.SQLiteConn
	dbPath            string
	watcher           *fsnotify.Watcher
	prefix            string
	publishLock       *sync.Mutex
	watchTablesSchema map[string][]*ColumnInfo
	OnChange          func(event *ChangeLogEvent) error
}

type ChangeLogEvent struct {
	Id        int64
	Type      string
	TableName string
	Row       map[string]any
	tableInfo []*ColumnInfo `cbor:"-"`
}

type ColumnInfo struct {
	Name         string `db:"name"`
	Type         string `db:"type"`
	NotNull      bool   `db:"notnull"`
	DefaultValue any    `db:"dflt_value"`
	IsPrimaryKey bool   `db:"pk"`
}

func OpenStreamDB(path string, tables []string) (*SqliteStreamDB, error) {
	connectionStr := fmt.Sprintf("%s?_journal_mode=wal", path)
	conn, rawConn, err := OpenRaw(connectionStr)
	if err != nil {
		return nil, err
	}

	conn.SetConnMaxLifetime(0)
	conn.SetConnMaxIdleTime(10 * time.Second)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	err = watcher.Add(path)
	if err != nil {
		return nil, err
	}

	sqliteQu := goqu.Dialect("sqlite3")
	ret := &SqliteStreamDB{
		Database:          sqliteQu.DB(conn),
		rawConnection:     rawConn,
		watcher:           watcher,
		dbPath:            path,
		prefix:            "__marmot__",
		publishLock:       &sync.Mutex{},
		watchTablesSchema: map[string][]*ColumnInfo{},
	}

	for _, n := range tables {
		colInfo, err := ret.GetTableInfo(n)
		if err != nil {
			return nil, err
		}

		ret.watchTablesSchema[n] = colInfo
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

func (conn *SqliteStreamDB) StartWatching() error {
	for tableName := range conn.watchTablesSchema {
		err := conn.initTriggers(tableName)
		if err != nil {
			return err
		}
	}

	go conn.watchChanges(conn.dbPath)
	return nil
}

func (conn *SqliteStreamDB) RemoveCDC() error {
	log.Warn().Msg("Uninstalling all CDC hooks...")
	return conn.cleanAll()
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

func (conn *SqliteStreamDB) GetTableInfo(table string) ([]*ColumnInfo, error) {
	query := "SELECT name, type, `notnull`, dflt_value, pk FROM pragma_table_info(?)"
	stmt, err := conn.Prepare(query)
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
	var backup *sqlite3.SQLiteBackup = nil
	_, src, err := OpenRaw(fmt.Sprintf("%s?_foreign_keys=false", conn.dbPath))
	if err != nil {
		return err
	}

	_, dest, err := OpenRaw(fmt.Sprintf("%s?_foreign_keys=false", bkFilePath))
	if err != nil {
		return err
	}

	defer func() {
		var err error

		if backup != nil {
			err = backup.Finish()
		}
		if err != nil {
			log.Error().Err(err).Msg("Unable to finish backup DB")
		}

		err = dest.Close()
		if err != nil {
			log.Error().Err(err).Msg("Unable to close backup DB")
		}

		err = src.Close()
		if err != nil {
			log.Error().Err(err).Msg("Unable to close backup DB")
		}
	}()

	backup, err = dest.Backup("main", src, "main")
	if err != nil {
		return err
	}

	for {
		done, err := backup.Step(-1)
		if err != nil {
			return err
		}

		if done {
			break
		}

		time.Sleep(150 * time.Millisecond)
	}

	return nil
}

func (conn *SqliteStreamDB) RestoreFrom(bkFilePath string) error {
	dns := fmt.Sprintf("%s?_journal_mode=wal&_foreign_keys=false&_txlock=deferred",
		conn.dbPath)
	sqlDB, dest, err := OpenRaw(dns)
	if err != nil {
		return err
	}

	gSQL := goqu.New("sqlite", sqlDB)
	_, err = dest.Exec("ATTACH DATABASE ? AS backup", []driver.Value{bkFilePath})
	if err != nil {
		return err
	}

	defer func() {
		_ = dest.Close()
	}()

	for tableName, cols := range conn.watchTablesSchema {
		names := lo.Map(cols, func(c *ColumnInfo, i int) string {
			return c.Name
		})

		totalRows, err := gSQL.From(tableName).Count()
		if err != nil {
			return err
		}

		for i := int64(0); i < totalRows; i += batchSize {
			log.Debug().Int64("page", i).Msg("Restoring " + tableName)
			err = gSQL.WithTx(func(tx *goqu.TxDatabase) error {
				query := fmt.Sprintf(restoreBackupSQL, tableName, strings.Join(names, ", "), i+batchSize, i)
				_, err = tx.Exec(query)
				if err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				return err
			}
		}

		log.Debug().Msg("Restored " + tableName)
	}

	return nil
}

func (conn *SqliteStreamDB) GetRawConnection() *sqlite3.SQLiteConn {
	return conn.rawConnection
}

func (conn *SqliteStreamDB) GetPath() string {
	return conn.dbPath
}

func (e *ChangeLogEvent) Marshal() ([]byte, error) {
	return cbor.Marshal(e)
}

func (e *ChangeLogEvent) Unmarshal(data []byte) error {
	return cbor.Unmarshal(data, e)
}

func (e *ChangeLogEvent) Hash() (uint64, error) {
	hasher := fnv.New64()
	_, err := hasher.Write([]byte(e.TableName))
	if err != nil {
		return 0, err
	}

	pkColumns := make([]string, 0, len(e.tableInfo))
	for _, itm := range e.tableInfo {
		if itm.IsPrimaryKey {
			pkColumns = append(pkColumns, itm.Name)
		}
	}

	pkTuples := make([]any, len(pkColumns))
	sort.Strings(pkColumns)
	for i, pk := range pkColumns {
		pkTuples[i] = e.Row[pk]
	}

	bts, err := cbor.Marshal(pkTuples)
	if err != nil {
		return 0, err
	}

	_, err = hasher.Write(bts)
	if err != nil {
		return 0, err
	}

	return hasher.Sum64(), nil
}
