package db

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/doug-martin/goqu/v9"
	"github.com/fsnotify/fsnotify"
	"github.com/fxamacker/cbor/v2"
	"github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

type SqliteStreamDB struct {
	*goqu.Database
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
}

type ColumnInfo struct {
	Name         string `db:"name"`
	Type         string `db:"type"`
	NotNull      bool   `db:"notnull"`
	DefaultValue any    `db:"dflt_value"`
	IsPrimaryKey bool   `db:"pk"`
}

func OpenSqlite(path string) (*SqliteStreamDB, error) {
	driver := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return conn.RegisterFunc("marmot_version", func() string {
				return "0.1"
			}, true)
		},
	}
	sql.Register("sqlite-marmot", driver)

	connectionStr := fmt.Sprintf("%s?_journal_mode=wal&mode=memory", path)
	conn, err := sql.Open("sqlite-marmot", connectionStr)
	if err != nil {
		return nil, err
	}

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
		watcher:           watcher,
		dbPath:            path,
		prefix:            "__marmot__",
		publishLock:       &sync.Mutex{},
		watchTablesSchema: map[string][]*ColumnInfo{},
	}

	return ret, nil
}

func (conn *SqliteStreamDB) InstallCDC(tables []string) error {

	for _, n := range tables {
		colInfo, err := conn.GetTableInfo(n)
		if err != nil {
			return err
		}
		conn.watchTablesSchema[n] = colInfo
	}

	err := conn.initTriggers(tables)
	if err != nil {
		return err
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

	stmt := &enhancedStatement{st}
	defer stmt.Finalize()

	if _, err := stmt.Exec(); err != nil {
		return err
	}

	return nil
}

func (conn *SqliteStreamDB) metaTable(tableName string, name string) string {
	return conn.prefix + tableName + "_" + name
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

	// Hash primary keys (TODO)
	return hasher.Sum64(), nil
}
