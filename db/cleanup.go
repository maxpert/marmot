package db

import (
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/zerolog/log"
)

const deleteTriggerQuery = `DROP TRIGGER IF EXISTS %s`
const deleteMarmotTables = `DROP TABLE IF EXISTS %s;`

func cleanMarmotTriggers(conn *goqu.Database, prefix string) error {
	triggers := make([]string, 0)
	err := conn.
		Select("name").
		From("sqlite_master").
		Where(goqu.And(goqu.Ex{"type": "trigger"}, goqu.C("name").Like(prefix+"%"))).
		Prepared(true).
		ScanVals(&triggers)
	if err != nil {
		return err
	}

	for _, name := range triggers {
		query := fmt.Sprintf(deleteTriggerQuery, name)
		_, err = conn.Exec(query)
		if err != nil {
			log.Error().Err(err).Str("name", name).Msg("Unable to delete trigger")
			return err
		}
	}

	return nil
}

func clearMarmotTables(conn *goqu.Database, prefix string) error {
	tables := make([]string, 0)
	err := conn.
		Select("name").
		From("sqlite_master").
		Where(goqu.And(goqu.Ex{"type": "table"}, goqu.C("name").Like(prefix+"%"))).
		Prepared(true).
		ScanVals(&tables)
	if err != nil {
		return err
	}

	for _, name := range tables {
		query := fmt.Sprintf(deleteMarmotTables, name)
		_, err = conn.Exec(query)
		if err != nil {
			log.Error().Err(err).Msg("Unable to delete marmot tables")
			return err
		}
	}

	return nil
}
