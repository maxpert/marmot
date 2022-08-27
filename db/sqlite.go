package db

import (
    "database/sql"
    "fmt"
    "time"

    "github.com/bep/debounce"
    "github.com/fsnotify/fsnotify"
    "github.com/rs/zerolog/log"
)

type SqliteStreamDB struct {
    *sql.DB
    watcher   *fsnotify.Watcher
    debounced func(f func())
    OnChange  func(event *ChangeLogEvent)
}

type ChangeLogEvent struct {
    Id          int64
    ChangeRowId int64
    Type        string
    Table       string
    Row         map[string]any
}

type changeLogEntry struct {
    Id          int64
    TableName   string
    Type        string
    ChangeRowId int64
    JsonPayload string
}

func OpenSqlite(path string) (*SqliteStreamDB, error) {
    connectionStr := fmt.Sprintf("file://%s?_journal_mode=wal", path)
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

    ret := &SqliteStreamDB{
        DB:        conn,
        watcher:   watcher,
        debounced: debounce.New(50 * time.Millisecond),
    }

    go ret.watchChanges(path)
    return ret, nil
}

func (conn *SqliteStreamDB) InstallCDC() error {
    log.Info().Msg("Creating log table...")

    if _, err := conn.Exec(logTableCreateStatement); err != nil {
        return err
    }
    if _, err := conn.Exec(replicaTableCreateStatement); err != nil {
        return err
    }

    return conn.initTriggers()
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

func (conn *SqliteStreamDB) RunQuery(query string, args ...any) ([]map[string]any, error) {
    st, err := conn.Prepare(query)
    if err != nil {
        return nil, err
    }

    stmt := &enhancedStatement{st}
    defer stmt.Finalize()

    rws, err := stmt.Query(args...)
    if err != nil {
        return nil, err
    }

    rs := &enhancedResultSet{rws}
    defer rs.Finalize()

    resultSet := make([]map[string]any, 0)
    for {
        step := rs.Next()
        if !step {
            break
        }

        row, err := rs.fetchRow()
        if err != nil {
            return nil, err
        }

        resultSet = append(resultSet, row)
    }

    if rs.Err() != nil {
        return nil, rs.Err()
    }

    return resultSet, nil
}

func (conn *SqliteStreamDB) RunTransactionQuery(txn *sql.Tx, query string, args ...any) ([]map[string]any, error) {
    st, err := txn.Prepare(query)
    if err != nil {
        return nil, err
    }

    stmt := &enhancedStatement{st}
    defer stmt.Finalize()

    rws, err := stmt.Query(args...)
    if err != nil {
        return nil, err
    }

    rs := &enhancedResultSet{rws}
    defer rs.Finalize()

    resultSet := make([]map[string]any, 0)
    for {
        step := rs.Next()
        if !step {
            break
        }

        row, err := rs.fetchRow()
        if err != nil {
            return nil, err
        }

        resultSet = append(resultSet, row)
    }

    if rs.Err() != nil {
        return nil, rs.Err()
    }

    return resultSet, nil
}
