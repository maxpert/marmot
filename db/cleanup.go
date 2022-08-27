package db

import (
    "fmt"

    "github.com/rs/zerolog/log"
)

const selectAllTriggers = `SELECT name FROM sqlite_master WHERE type ='trigger' AND NAME LIKE '__marmot%'`
const deleteTriggerQuery = `DROP TRIGGER IF EXISTS %s`
const deleteMarmotTables = `
DROP TABLE IF EXISTS __marmot__change__log;
DROP TABLE IF EXISTS __marmot__replica__log;
`

func (conn *SqliteStreamDB) cleanAll() error {
    triggers, err := conn.RunQuery(selectAllTriggers)
    if err != nil {
        return err
    }

    for _, t := range triggers {
        name := t["name"].(string)
        query := fmt.Sprintf(deleteTriggerQuery, name)
        _, err = conn.Exec(query)
        if err != nil {
            log.Error().Err(err).Str("name", name).Msg("Unable to delete trigger")
        }
    }

    _, err = conn.Exec(deleteMarmotTables)
    if err != nil {
        log.Error().Err(err).Msg("Unable to delete marmot tables")
    }

    return nil
}
