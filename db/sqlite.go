package db

import (
    "database/sql"
    "encoding/binary"
    "fmt"
    "hash/fnv"
    "time"

    "github.com/bep/debounce"
    "github.com/doug-martin/goqu/v9"
    "github.com/fsnotify/fsnotify"
    "github.com/fxamacker/cbor/v2"
    "github.com/rs/zerolog/log"
)

type SqliteStreamDB struct {
    *goqu.Database
    OnChange  func(event *ChangeLogEvent) error
    watcher   *fsnotify.Watcher
    debounced func(f func())
    prefix    string
}

type ChangeLogEvent struct {
    Id          int64
    ChangeRowId int64
    Type        string
    TableName   string
    Row         map[string]any
}

type ColumnInfo struct {
    Name         string `db:"name"`
    Type         string `db:"type"`
    NotNull      bool   `db:"notnull"`
    DefaultValue any    `db:"dflt_value"`
    IsPrimaryKey bool   `db:"pk"`
}

func OpenSqlite(path string) (*SqliteStreamDB, error) {
    connectionStr := fmt.Sprintf("%s?_journal_mode=wal&mode=memory", path)
    conn, err := sql.Open("sqlite3", connectionStr)
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
        Database:  sqliteQu.DB(conn),
        watcher:   watcher,
        prefix:    "__marmot__",
        debounced: debounce.New(50 * time.Millisecond),
    }

    go ret.watchChanges(path)
    return ret, nil
}

func (conn *SqliteStreamDB) InstallCDC(tables []string) error {
    log.Debug().Msg("Creating log table...")
    createChangeLogQuery := fmt.Sprintf(
        logTableCreateStatement,
        conn.metaTable(changeLogName),
        conn.metaTable(replicaInName),
    )
    if _, err := conn.Exec(createChangeLogQuery); err != nil {
        return err
    }

    log.Debug().Msg("Creating replica table...")
    return conn.initTriggers(tables)
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

func (conn *SqliteStreamDB) metaTable(name string) string {
    return conn.prefix + name
}

func (conn *SqliteStreamDB) GetTableInfo(table string) (map[string]*ColumnInfo, error) {
    query := "SELECT name, type, `notnull`, dflt_value, pk FROM pragma_table_info(?)"
    stmt, err := conn.Prepare(query)
    if err != nil {
        return nil, err
    }

    rows, err := stmt.Query(table)
    if err != nil {
        return nil, err
    }

    tableInfo := make(map[string]*ColumnInfo)
    for rows.Next() {
        if rows.Err() != nil {
            return nil, rows.Err()
        }

        c := ColumnInfo{}
        err = rows.Scan(&c.Name, &c.Type, &c.NotNull, &c.DefaultValue, &c.IsPrimaryKey)
        if err != nil {
            return nil, err
        }

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

    b := make([]byte, 8)
    binary.LittleEndian.PutUint64(b, uint64(e.ChangeRowId))
    _, err = hasher.Write(b)
    if err != nil {
        return 0, err
    }

    return hasher.Sum64(), nil
}
