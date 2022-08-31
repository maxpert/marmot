package db

import (
    "errors"
    "fmt"
    "strings"

    "github.com/doug-martin/goqu/v9"
)

const upsertQuery = `INSERT OR REPLACE INTO %s(%s) VALUES (%s)`

func (conn *SqliteStreamDB) Replicate(event *ChangeLogEvent) error {
    if err := conn.consumeReplicationEvent(event); err != nil {
        return err
    }
    return nil
}

func (conn *SqliteStreamDB) consumeReplicationEvent(event *ChangeLogEvent) error {
    return conn.WithTx(func(tnx *goqu.TxDatabase) error {
        _, err := tnx.Insert(conn.metaTable(replicaInName)).
            Rows(goqu.Record{
                "id":            event.Id,
                "type":          event.Type,
                "table_name":    event.TableName,
                "change_row_id": event.ChangeRowId,
            }).
            Prepared(true).
            Executor().
            Exec()

        if err != nil {
            return err
        }

        err = replicateRow(tnx, event)
        if err != nil {
            return err
        }

        _, err = tnx.Delete(conn.metaTable(replicaInName)).Where(goqu.Ex{
            "id": event.Id,
        }).Prepared(true).Executor().Exec()

        return err
    })
}

func replicateRow(tx *goqu.TxDatabase, event *ChangeLogEvent) error {
    if event.Type == "insert" || event.Type == "update" {
        return replicateUpsert(tx, event)
    }

    if event.Type == "delete" {
        return replicateDelete(tx, event)
    }

    return errors.New(fmt.Sprintf("invalid operation type %s", event.Type))
}

func replicateUpsert(tx *goqu.TxDatabase, event *ChangeLogEvent) error {
    columnNames := make([]string, 0, len(event.Row))
    columnValues := make([]any, 0, len(event.Row))

    columnNames = append(columnNames, "rowid")
    columnValues = append(columnValues, event.ChangeRowId)

    for k, v := range event.Row {
        columnNames = append(columnNames, k)
        columnValues = append(columnValues, v)
    }

    query := fmt.Sprintf(
        upsertQuery,
        event.TableName,
        strings.Join(columnNames, ", "),
        strings.Join(strings.Split(strings.Repeat("?", len(columnNames)), ""), ", "),
    )

    stmt, err := tx.Prepare(query)
    if err != nil {
        return err
    }

    _, err = stmt.Exec(columnValues...)
    return err
}

func replicateDelete(tx *goqu.TxDatabase, event *ChangeLogEvent) error {
    _, err := tx.Delete(event.TableName).
        Where(goqu.Ex{"rowid": event.ChangeRowId}).
        Prepared(true).
        Executor().
        Exec()

    return err
}
