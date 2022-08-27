package db

import (
    "context"
    "database/sql"
    "fmt"
    "strings"
    "time"

    "github.com/fsnotify/fsnotify"
    "github.com/rs/zerolog/log"
    "github.com/samber/lo"
)

const logTableCreateStatement = `CREATE TABLE IF NOT EXISTS __marmot__change__log(
    id              integer primary key,
    table_name      text,
    type            text,
    change_row_id   integer,
    row_map_serial  blob
);`

const listTableQuery = `SELECT name 
FROM sqlite_master 
WHERE type ='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__marmot__%'`

const selectChangesId = "SELECT id, table_name, type, change_row_id FROM __marmot__change__log"
const dropTriggerStatement = `DROP TRIGGER IF EXISTS __marmot__on_%[1]s_%[2]s;`
const createTriggerStatement = `CREATE TRIGGER IF NOT EXISTS __marmot__on_%[1]s_%[2]s
AFTER %[2]s ON %[1]s 
WHEN
    (SELECT COUNT(*) 
    FROM __marmot__replica__log as r 
    WHERE r.change_row_id = %[3]s.rowid AND r.table_name = '%[1]s' AND r.type = '%[2]s') = 0
BEGIN
    INSERT INTO __marmot__change__log(
        id,
        table_name, 
        type, 
        change_row_id
    ) VALUES(
        abs(random()),
        '%[1]s', 
        '%[2]s', 
        %[3]s.rowid
    );
END;`

func (conn *SqliteStreamDB) initTriggers() error {
    liftName := func(tpl map[string]any, _ int) string {
        return tpl["name"].(string)
    }

    log.Info().Msg("Listing tables...")
    rs, err := conn.RunQuery(listTableQuery)
    if err != nil {
        return err
    }

    tableNames := lo.Map[map[string]any, string](rs, liftName)

    for _, name := range tableNames {
        log.Info().Msg(fmt.Sprintf("Creating trigger for %v", name))

        query := fmt.Sprintf(dropTriggerStatement, name, "insert")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = fmt.Sprintf(createTriggerStatement, name, "insert", "NEW")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = fmt.Sprintf(dropTriggerStatement, name, "update")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = fmt.Sprintf(createTriggerStatement, name, "update", "NEW")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = fmt.Sprintf(dropTriggerStatement, name, "delete")
        if err := conn.Execute(query); err != nil {
            return err
        }

        query = fmt.Sprintf(createTriggerStatement, name, "delete", "OLD")
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
                conn.debounced(conn.scanChangeLog)
            }
        case <-time.After(time.Second * 1):
            conn.debounced(conn.scanChangeLog)
            if errShm != nil {
                errShm = watcher.Add(shmPath)
            }

            if errWal != nil {
                errWal = watcher.Add(walPath)
            }
        }
    }
}

func (conn *SqliteStreamDB) scanChangeLog() {
    tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
        Isolation: sql.LevelLinearizable,
        ReadOnly:  true,
    })
    if err != nil {
        log.Error().Err(err).Msg("Unable to start read transaction")
        return
    }
    defer func(tx *sql.Tx) {
        _ = tx.Rollback()
    }(tx)

    rs, err := conn.RunTransactionQuery(tx, selectChangesId)
    if err != nil {
        log.Error().Err(err).Msg("Error scanning last row ID")
        return
    }

    if err := tx.Commit(); err != nil {
        log.Error().Err(err).Msg("Unable to commit transaction")
    }

    if len(rs) == 0 {
        return
    }

    log.Debug().Int("total", len(rs)).Msg("New changes...")
    changes := make([]*changeLogEntry, 0, len(rs))

    for _, r := range rs {
        currId, ok := r["id"].(int64)
        if ok {
            log.Debug().Msg(fmt.Sprintf("Row updated %v", r))
            changes = append(changes, &changeLogEntry{
                ChangeRowId: r["change_row_id"].(int64),
                Type:        r["type"].(string),
                TableName:   r["table_name"].(string),
                Id:          currId,
            })
        }
    }

    err = conn.consumeChanges(changes)
    if err != nil {
        log.Error().Err(err).Msg("Unable to consume changes")
    }
}

func (conn *SqliteStreamDB) consumeChanges(changes []*changeLogEntry) error {
    perTableChanges := lo.GroupBy[*changeLogEntry, string](changes, func(e *changeLogEntry) string {
        return e.TableName
    })

    for tableName, tableChanges := range perTableChanges {
        deletes, upserts := conn.splitDeleteAndUpserts(tableChanges)

        tx, err := conn.Begin()
        if err != nil {
            return err
        }

        err = conn.consumeDeletes(tx, deletes, tableName)
        if err != nil {
            _ = tx.Rollback()
            return err
        }

        err = conn.consumeUpserts(tx, upserts, tableName)
        if err != nil {
            _ = tx.Rollback()
            return err
        }

        err = tx.Commit()
        if err != nil {
            return err
        }
    }

    return nil
}

func (conn *SqliteStreamDB) consumeUpserts(tx *sql.Tx, upserts []*changeLogEntry, tableName string) error {
    placeHolders := strings.Split(strings.Repeat("?", len(upserts)), "")
    placeHoldersStr := strings.Join(placeHolders, ", ")
    conditionalPostfix := fmt.Sprintf("FROM %s WHERE rowid IN(%s)", tableName, placeHoldersStr)
    changeRowIds := lo.Map[*changeLogEntry, any](upserts, func(e *changeLogEntry, _ int) any {
        return e.ChangeRowId
    })

    if err := conn.selectAndPublishRows(tx, upserts, conditionalPostfix, changeRowIds); err != nil {
        return err
    }

    rowIds := lo.Map[*changeLogEntry, any](upserts, func(e *changeLogEntry, _ int) any {
        return e.Id
    })

    if err := conn.deleteChangeLogRows(tx, rowIds); err != nil {
        return err
    }

    return nil
}

func (conn *SqliteStreamDB) consumeDeletes(tx *sql.Tx, deletes []*changeLogEntry, tableName string) error {
    for _, d := range deletes {
        if conn.OnChange != nil {
            conn.OnChange(&ChangeLogEvent{
                Id:          d.Id,
                Row:         nil,
                Type:        "delete",
                Table:       tableName,
                ChangeRowId: d.ChangeRowId,
            })
        }
    }

    rowIds := lo.Map[*changeLogEntry, any](deletes, func(e *changeLogEntry, _ int) any {
        return e.Id
    })

    return conn.deleteChangeLogRows(tx, rowIds)
}

func (conn *SqliteStreamDB) deleteChangeLogRows(tx *sql.Tx, rowIds []any) error {
    placeHolders := strings.Split(strings.Repeat("?", len(rowIds)), "")
    placeHoldersStr := strings.Join(placeHolders, ", ")
    del := fmt.Sprintf("DELETE FROM __marmot__change__log WHERE rowid IN(%[1]s)", placeHoldersStr)
    delSt, err := tx.Prepare(del)
    if err != nil {
        return err
    }

    defer (&enhancedStatement{delSt}).Finalize()
    _, err = delSt.Exec(rowIds...)
    if err != nil {
        return err
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

func (conn *SqliteStreamDB) selectAndPublishRows(tx *sql.Tx, upserts []*changeLogEntry, conditionalPostfix string, rowIds []any) error {
    sel := fmt.Sprintf("SELECT rowid, * %s", conditionalPostfix)
    selSt, err := tx.Prepare(sel)
    if err != nil {
        return err
    }

    selStmt := &enhancedStatement{selSt}
    defer selStmt.Finalize()

    changedRows, err := selStmt.Query(rowIds...)
    if err != nil {
        return err
    }

    upsertMap := make(map[int64]*changeLogEntry)
    for _, e := range upserts {
        upsertMap[e.ChangeRowId] = e
    }

    rs := &enhancedResultSet{changedRows}
    for {
        step := rs.Next()
        if !step {
            break
        }

        row, err := rs.fetchRow()
        if err != nil {
            return err
        }

        changeRowID := row["rowid"].(int64)
        if conn.OnChange != nil {
            conn.OnChange(&ChangeLogEvent{
                Id:          upsertMap[changeRowID].Id,
                Type:        upsertMap[changeRowID].Type,
                Table:       upsertMap[changeRowID].TableName,
                ChangeRowId: changeRowID,
                Row:         row,
            })
        }
    }

    return nil
}
