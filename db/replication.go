package db

import (
    "errors"
    "fmt"

    "github.com/doug-martin/goqu/v9"
    "github.com/doug-martin/goqu/v9/exp"
)

func (conn *SqliteStreamDB) Replicate(event *ChangeLogEvent) error {
    if err := conn.consumeReplicationEvent(event); err != nil {
        return err
    }
    return nil
}

func (conn *SqliteStreamDB) consumeReplicationEvent(event *ChangeLogEvent) error {
    return conn.WithTx(func(tnx *goqu.TxDatabase) error {
        err := replicateRow(tnx, event)
        if err != nil {

            return err
        }

        return nil
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
    _, err := tx.Insert(event.TableName).
        OnConflict(exp.NewDoUpdateConflictExpression("", event.Row)).
        Rows(event.Row).
        Executor().
        Exec()

    return err
}

func replicateDelete(tx *goqu.TxDatabase, event *ChangeLogEvent) error {
    _, err := tx.Delete(event.TableName).
        Where(goqu.Ex{"rowid": event.ChangeRowId}).
        Executor().
        Exec()

    return err
}
