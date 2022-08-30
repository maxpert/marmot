package db

import (
    "fmt"
    "strings"
    "time"

    "github.com/doug-martin/goqu/v9"
    "github.com/fsnotify/fsnotify"
    "github.com/rs/zerolog/log"
    "github.com/samber/lo"
)

const changeLogName = "change__log"

const logTableCreateStatement = `CREATE TABLE IF NOT EXISTS %s(
    id              integer primary key,
    table_name      text,
    type            text,
    change_row_id   integer,
    row_map_serial  blob
);`

type changeLogEntry struct {
    Id            int64  `db:"id"`
    TableName     string `db:"table_name"`
    Type          string `db:"type"`
    ChangeRowId   int64  `db:"change_row_id"`
    SerializedRow []byte `db:"row_map_serial"`
}

func createTriggerSQL(prefix, table, event, logTable, newOld string) string {
    // 1: prefix, 2: table, 3: event 4: log_table 5: NEW/OLD
    const createTriggerStatement = `CREATE TRIGGER IF NOT EXISTS %[1]son_%[2]s_%[3]s
    AFTER %[3]s ON %[2]s
    BEGIN
        INSERT INTO %[4]s(
            id,
            table_name, 
            type, 
            change_row_id
        ) VALUES(
            abs(random()),
            '%[2]s', 
            '%[3]s', 
            %[5]s.rowid
        );
    END;`
    return fmt.Sprintf(createTriggerStatement, prefix, table, event, logTable, newOld)
}

func dropTriggerSQL(prefix, table, event string) string {
    // 1: prefix, 2: table, 3: event
    const dropTriggerStatement = `DROP TRIGGER IF EXISTS %[1]son_%[2]s_%[3]s;`
    return fmt.Sprintf(dropTriggerStatement, prefix, table, event)
}

func (conn *SqliteStreamDB) initTriggers() error {
    log.Info().Msg("Listing tables...")
    tableNames := make([]string, 0)
    err := conn.Select("name").
        From("sqlite_master").
        Where(
            goqu.And(
                goqu.Ex{"type": "table"},
                goqu.Ex{"name": goqu.Op{"notLike": "sqlite_%"}},
                goqu.Ex{"name": goqu.Op{"notLike": conn.prefix + "%"}},
            ),
        ).
        Prepared(true).
        Executor().
        ScanVals(&tableNames)

    if err != nil {
        return err
    }

    for _, name := range tableNames {
        log.Info().Msg(fmt.Sprintf("Creating trigger for %v", name))
        logTableName := conn.metaTable(changeLogName)

        query := dropTriggerSQL(conn.prefix, name, "insert")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = createTriggerSQL(conn.prefix, name, "insert", logTableName, "NEW")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = dropTriggerSQL(conn.prefix, name, "update")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = createTriggerSQL(conn.prefix, name, "update", logTableName, "NEW")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = dropTriggerSQL(conn.prefix, name, "update")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = createTriggerSQL(conn.prefix, name, "delete", logTableName, "OLD")
        if err := conn.Execute(query); err != nil {
            return err
        }
    }

    return nil
}

func (conn *SqliteStreamDB) watchChanges(path string) {
    shmPath := path + "-shm"
    walPath := path + "-wal"
    watcher := conn.watcher

    errShm := watcher.Add(shmPath)
    errWal := watcher.Add(walPath)

    for {
        select {
        case ev, ok := <-conn.watcher.Events:
            if !ok {
                return
            }

            if ev.Op != fsnotify.Chmod {
                conn.debounced(conn.publishChangeLog)
            }
        case <-time.After(time.Second * 1):
            conn.debounced(conn.publishChangeLog)
            if errShm != nil {
                errShm = watcher.Add(shmPath)
            }

            if errWal != nil {
                errWal = watcher.Add(walPath)
            }
        }
    }
}

func (conn *SqliteStreamDB) publishChangeLog() {
    var changes []*changeLogEntry
    err := conn.WithTx(func(tx *goqu.TxDatabase) error {
        return tx.From(conn.metaTable(changeLogName)).Prepared(true).ScanStructs(&changes)
    })

    if err != nil {
        log.Error().Err(err).Msg("Error scanning last row ID")
        return
    }

    if len(changes) == 0 {
        return
    }

    err = conn.consumeChangeLogs(changes)
    if err != nil {
        log.Error().Err(err).Msg("Unable to consume changes")
    }
}

func (conn *SqliteStreamDB) consumeChangeLogs(changes []*changeLogEntry) error {
    perTableChanges := lo.GroupBy[*changeLogEntry, string](changes, func(e *changeLogEntry) string {
        return e.TableName
    })

    for tableName, tableChanges := range perTableChanges {
        // Consume each batch in transaction
        deletes, upserts := conn.splitDeleteAndUpserts(tableChanges)

        err := conn.consumeDeletes(deletes, tableName)
        if err != nil {
            return err
        }

        err = conn.consumeUpserts(upserts, tableName)
        if err != nil {
            return err
        }

        return nil
    }

    return nil
}

func (conn *SqliteStreamDB) consumeUpserts(upserts []*changeLogEntry, tableName string) error {
    changeRowIds := lo.Map[*changeLogEntry, any](upserts, func(e *changeLogEntry, _ int) any {
        return e.ChangeRowId
    })

    query, params, err := conn.From(tableName).
        Select("rowid", "*").
        Where(goqu.C("rowid").In(changeRowIds)).
        Prepared(true).
        ToSQL()
    if err != nil {
        return err
    }

    rawRows, err := conn.Query(query, params...)
    if err != nil {
        return nil
    }

    rows := &enhancedRows{rawRows}
    upsertMap := lo.Associate[*changeLogEntry, int64, *changeLogEntry](
        upserts,
        func(l *changeLogEntry) (int64, *changeLogEntry) {
            return l.ChangeRowId, l
        },
    )

    for rows.Next() {
        row, err := rows.fetchRow()
        if err != nil {
            return err
        }

        changeRowID := row["rowid"].(int64)
        changeRow := upsertMap[changeRowID]
        if conn.OnChange != nil {
            err = conn.OnChange(&ChangeLogEvent{
                Id:          changeRow.Id,
                Type:        changeRow.Type,
                TableName:   changeRow.TableName,
                ChangeRowId: changeRowID,
                Row:         row,
            })

            if err != nil {
                return err
            }
        }

        _, err = conn.
            Delete(conn.metaTable(changeLogName)).
            Where(goqu.Ex{"rowid": changeRow.Id}).
            Executor().
            Exec()
        if err != nil {
            return err
        }
    }

    return nil
}

func (conn *SqliteStreamDB) consumeDeletes(deletes []*changeLogEntry, tableName string) error {
    for _, d := range deletes {
        if conn.OnChange != nil {
            err := conn.OnChange(&ChangeLogEvent{
                Id:          d.Id,
                Row:         nil,
                Type:        "delete",
                TableName:   tableName,
                ChangeRowId: d.ChangeRowId,
            })

            if err != nil {
                return err
            }

            _, err = conn.
                Delete(conn.metaTable(changeLogName)).
                Where(goqu.Ex{"rowid": d.Id}).
                Executor().
                Exec()
            if err != nil {
                return err
            }
        }
    }

    return nil
}

func (conn *SqliteStreamDB) splitDeleteAndUpserts(tableChanges []*changeLogEntry) ([]*changeLogEntry, []*changeLogEntry) {
    deletes := make([]*changeLogEntry, 0)
    upserts := make([]*changeLogEntry, 0)
    for _, tableChange := range tableChanges {
        if strings.ToLower(tableChange.Type) == "delete" {
            deletes = append(deletes, tableChange)
        } else {
            upserts = append(upserts, tableChange)
        }
    }
    return deletes, upserts
}
