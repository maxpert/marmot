package db

import (
    "database/sql"
    "errors"
    "fmt"
    "strings"
    "time"

    "github.com/fxamacker/cbor/v2"
    "github.com/rs/zerolog/log"
)

const replicaTableCreateStatement = `CREATE TABLE IF NOT EXISTS __marmot__replica__log(
    id              integer primary key,
    table_name      text,
    type            text,
    change_row_id   integer,
    row_map_serial  blob,
    create_on       timestamp
);
CREATE INDEX IF NOT EXISTS __marmot__replica_change_tuples ON __marmot__replica__log(change_row_id, table_name);`

const insertReplicaLog = `INSERT INTO __marmot__replica__log(
     id, table_name, type, change_row_id, row_map_serial, create_on
 ) VALUES(
     ?, ?, ?, ?, ?, ?
 );`

func (conn *SqliteStreamDB) Replicate(event *ChangeLogEvent) error {
    msg := fmt.Sprintf("Replicating %d. %s on %s for %d", event.Id, event.Type, event.Table, event.ChangeRowId)
    log.Info().Msg(msg)

    // Commit log entry accepting proposal
    serializedRow, err := cbor.Marshal(event.Row)
    if err != nil {
        return err
    }

    _, err = conn.Exec(
        insertReplicaLog,
        event.Id, event.Table, event.Type, event.ChangeRowId, serializedRow, time.Now().Unix(),
    )

    if err != nil {
        return err
    }

    if err := conn.consumeReplicationEvent(event); err != nil {
        return err
    }

    return nil
}

func (conn *SqliteStreamDB) consumeReplicationEvent(event *ChangeLogEvent) error {
    tnx, err := conn.Begin()
    defer func(tnx *sql.Tx) {
        _ = tnx.Rollback()
    }(tnx)

    if err != nil {
        return err
    }

    err = replicateRow(tnx, event)
    if err != nil {

        return err
    }

    err = deleteReplicationEvent(tnx, event)
    if err != nil {
        return err
    }

    return tnx.Commit()
}

func deleteReplicationEvent(tx *sql.Tx, event *ChangeLogEvent) error {
    _, err := tx.Exec("DELETE FROM __marmot__replica__log WHERE id = ?", event.Id)
    return err
}

func replicateRow(tx *sql.Tx, event *ChangeLogEvent) error {
    if event.Type == "insert" || event.Type == "update" {
        _, err := replicateUpsert(tx, event)
        return err
    }

    if event.Type == "delete" {
        _, err := replicateDelete(tx, event)
        return err
    }

    return errors.New(fmt.Sprintf("invalid operation type %s", event.Type))
}

func replicateUpsert(tx *sql.Tx, event *ChangeLogEvent) (sql.Result, error) {
    cols, vals := getColRowTuples(event)
    placeHolders := strings.Split(strings.Repeat("?", len(vals)), "")
    colsStr := strings.Join(cols, ", ")
    placeHolderStr := strings.Join(placeHolders, ", ")
    query := fmt.Sprintf(
        "INSERT OR REPLACE INTO %s(%s) VALUES (%s)",
        event.Table,
        colsStr,
        placeHolderStr,
    )
    return tx.Exec(query, vals...)
}

func replicateDelete(tx *sql.Tx, event *ChangeLogEvent) (sql.Result, error) {
    query := fmt.Sprintf("DELETE FROM %s WHERE rowid = ?", event.Table)
    return tx.Exec(query, event.ChangeRowId)
}

func getColRowTuples(event *ChangeLogEvent) ([]string, []any) {
    rowMap := event.Row
    cols := make([]string, 0, len(rowMap)+1)
    vals := make([]any, 0, len(rowMap)+1)

    cols = append(cols, "rowid")
    vals = append(vals, event.ChangeRowId)
    for colName, colVal := range rowMap {
        cols = append(cols, colName)
        vals = append(vals, colVal)
    }

    return cols, vals
}
