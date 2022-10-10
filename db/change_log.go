package db

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"

	_ "embed"

	"github.com/doug-martin/goqu/v9"
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

// ScanLimit is number of change log rows processed at a time, to limit memory usage
const ScanLimit = uint(128)

var ErrNoTableMapping = errors.New("no table mapping found")
var ErrLogNotReadyToPublish = errors.New("not ready to publish changes")

//go:embed table_change_log_script.tmpl
var tableChangeLogScriptTemplate string
var tableChangeLogTpl *template.Template

var spaceStripper = regexp.MustCompile(`\n\s+`)

type ChangeLogState = int16

const (
	Pending   ChangeLogState = 0
	Published ChangeLogState = 1
	Failed    ChangeLogState = -1
)
const changeLogName = "change_log"
const upsertQuery = `INSERT OR REPLACE INTO %s(%s) VALUES (%s)`

type triggerTemplateData struct {
	Prefix    string
	TableName string
	Columns   []*ColumnInfo
	Triggers  map[string]string
}

type globalChangeLogEntry struct {
	Id            int64  `db:"id"`
	ChangeTableId int64  `db:"change_table_id"`
	TableName     string `db:"table_name"`
}

type changeLogEntry struct {
	Id    int64  `db:"id"`
	Type  string `db:"type"`
	State string `db:"state"`
}

func init() {
	tableChangeLogTpl = template.Must(
		template.New("tableChangeLogScriptTemplate").Parse(tableChangeLogScriptTemplate),
	)
}

func (conn *SqliteStreamDB) Replicate(event *ChangeLogEvent) error {
	if err := conn.consumeReplicationEvent(event); err != nil {
		return err
	}
	return nil
}

func (conn *SqliteStreamDB) CleanupChangeLogs(beforeTime time.Time) (int64, error) {
	total := int64(0)
	for name := range conn.watchTablesSchema {
		metaTableName := conn.metaTable(name, changeLogName)
		rs, err := conn.Delete(metaTableName).
			Where(
				goqu.C("state").Eq(Published),
				goqu.C("created_at").Lte(beforeTime.UnixMilli()),
			).
			Prepared(true).
			Executor().
			Exec()

		if err != nil {
			return 0, err
		}

		count, err := rs.RowsAffected()
		if err != nil {
			return 0, err
		}

		total += count
	}

	return total, nil
}

func (conn *SqliteStreamDB) metaTable(tableName string, name string) string {
	return conn.prefix + tableName + "_" + name
}

func (conn *SqliteStreamDB) globalMetaTable() string {
	return conn.prefix + "_change_log_global"
}

func (conn *SqliteStreamDB) tableCDCScriptFor(tableName string) (string, error) {
	columns, ok := conn.watchTablesSchema[tableName]
	if !ok {
		return "", errors.New("table info not found")
	}

	buf := new(bytes.Buffer)
	err := tableChangeLogTpl.Execute(buf, &triggerTemplateData{
		Prefix:    conn.prefix,
		Triggers:  map[string]string{"insert": "NEW", "update": "NEW", "delete": "OLD"},
		Columns:   columns,
		TableName: tableName,
	})

	if err != nil {
		return "", err
	}

	return spaceStripper.ReplaceAllString(buf.String(), "\n    "), nil
}

func (conn *SqliteStreamDB) consumeReplicationEvent(event *ChangeLogEvent) error {
	return conn.WithTx(func(tnx *goqu.TxDatabase) error {
		primaryKeyMap := conn.getPrimaryKeyMap(event)
		if primaryKeyMap == nil {
			return ErrNoTableMapping
		}

		log.Debug().
			Str("table", event.TableName).
			Str("type", event.Type).
			Int64("event_id", event.Id).
			Msg(fmt.Sprintf("Replicating %v", primaryKeyMap))
		return replicateRow(tnx, event, primaryKeyMap)
	})
}

func (conn *SqliteStreamDB) getPrimaryKeyMap(event *ChangeLogEvent) map[string]any {
	ret := make(map[string]any)
	tableColsSchema, ok := conn.watchTablesSchema[event.TableName]
	if !ok {
		return nil
	}

	for _, col := range tableColsSchema {
		if col.IsPrimaryKey {
			ret[col.Name] = event.Row[col.Name]
		}
	}

	return ret
}

func (conn *SqliteStreamDB) initTriggers(tableName string) error {
	name := strings.TrimSpace(tableName)
	if strings.HasPrefix(name, "sqlite_") || strings.HasPrefix(name, conn.prefix) {
		return fmt.Errorf("invalid table to watch %s", tableName)
	}

	script, err := conn.tableCDCScriptFor(name)
	if err != nil {
		log.Error().Err(err).Msg("Failed to prepare CDC statement")
		return err
	}

	log.Info().Msg(fmt.Sprintf("Creating trigger for %v", name))
	_, err = conn.Exec(script)
	if err != nil {
		return err
	}

	return nil
}

func (conn *SqliteStreamDB) watchChanges(path string) {
	shmPath := path + "-shm"
	walPath := path + "-wal"
	watcher := conn.watcher

	errShm := watcher.Add(shmPath)
	errWal := watcher.Add(walPath)
	changeLogTicker := time.NewTicker(time.Millisecond * 500)
	updateTime := time.Now()

	for {
		select {
		case ev, ok := <-conn.watcher.Events:
			if !ok {
				return
			}

			if ev.Op != fsnotify.Chmod {
				conn.publishChangeLog()
			}
		case t := <-changeLogTicker.C:
			if t.After(updateTime) {
				conn.publishChangeLog()
			}

			if errShm != nil {
				errShm = watcher.Add(shmPath)
			}

			if errWal != nil {
				errWal = watcher.Add(walPath)
			}
		}

		updateTime = time.Now()
	}
}

func (conn *SqliteStreamDB) getGlobalChanges(limit uint) ([]globalChangeLogEntry, error) {
	var entries []globalChangeLogEntry
	err := conn.
		From(conn.globalMetaTable()).
		Limit(limit).
		ScanStructs(&entries)

	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (conn *SqliteStreamDB) publishChangeLog() {
	conn.publishLock.Lock()
	defer conn.publishLock.Unlock()

	changes, err := conn.getGlobalChanges(ScanLimit)
	if err != nil {
		log.Error().Err(err).Msg("Unable to scan global changes")
		return
	}

	if len(changes) < 0 {
		return
	}

	for _, change := range changes {
		logEntries := changeLogEntry{}

		found, err := conn.Select("id", "type", "state").
			From(conn.metaTable(change.TableName, changeLogName)).
			Where(
				goqu.C("state").Eq(Pending),
				goqu.C("id").Eq(change.ChangeTableId),
			).
			Prepared(true).
			ScanStruct(&logEntries)

		if err != nil {
			log.Error().Err(err).Msg("Error scanning last row ID")
			return
		}

		if !found {
			log.Panic().
				Str("table", change.TableName).
				Int64("id", change.ChangeTableId).
				Msg("Global change log row not found in corresponding table")
			return
		}

		err = conn.consumeChangeLogs(change.TableName, []*changeLogEntry{&logEntries})
		if err != nil {
			if err == ErrLogNotReadyToPublish {
				break
			}

			log.Error().Err(err).Msg("Unable to consume changes")
		}

		err = conn.WithTx(func(txDatabase *goqu.TxDatabase) error {
			_, err = conn.Update(conn.metaTable(change.TableName, changeLogName)).
				Set(goqu.Record{"state": Published}).
				Where(goqu.Ex{"id": change.ChangeTableId}).
				Prepared(true).
				Executor().
				Exec()

			if err != nil {
				return err
			}

			_, err = conn.Delete(conn.globalMetaTable()).
				Where(goqu.C("id").Eq(change.Id)).
				Prepared(true).
				Executor().
				Exec()

			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			log.Error().Err(err).Msg("Unable to cleanup change log")
		}
	}
}

func (conn *SqliteStreamDB) consumeChangeLogs(tableName string, changes []*changeLogEntry) error {
	rowIds := lo.Map(changes, func(e *changeLogEntry, i int) int64 {
		return e.Id
	})

	changeMap := lo.Associate(
		changes,
		func(l *changeLogEntry) (int64, *changeLogEntry) {
			return l.Id, l
		},
	)

	idColumnName := conn.prefix + "change_log_id"
	rawRows, err := conn.fetchChangeRows(tableName, idColumnName, rowIds)
	if err != nil {
		return err
	}

	rows := &EnhancedRows{rawRows}
	defer rows.Finalize()

	for rows.Next() {
		row, err := rows.fetchRow()
		if err != nil {
			return err
		}

		changeRowID := row[idColumnName].(int64)
		changeRow := changeMap[changeRowID]
		delete(row, idColumnName)

		logger := log.With().
			Int64("rowid", changeRowID).
			Str("table", tableName).
			Str("type", changeRow.Type).
			Logger()

		if conn.OnChange != nil {
			err = conn.OnChange(&ChangeLogEvent{
				Id:        changeRowID,
				Type:      changeRow.Type,
				TableName: tableName,
				Row:       row,
				tableInfo: conn.watchTablesSchema[tableName],
			})

			if err != nil {
				if err == ErrLogNotReadyToPublish {
					return err
				}

				logger.Error().Err(err).Msg("Failed to publish for table " + tableName)
				return err
			}
		}
	}

	return nil
}

func (conn *SqliteStreamDB) fetchChangeRows(
	tableName string,
	idColumnName string,
	rowIds []int64,
) (*sql.Rows, error) {
	columnNames := make([]any, 0)
	tableCols := conn.watchTablesSchema[tableName]
	columnNames = append(columnNames, goqu.C("id").As(idColumnName))
	for _, col := range tableCols {
		columnNames = append(columnNames, goqu.C("val_"+col.Name).As(col.Name))
	}

	query, params, err := conn.From(conn.metaTable(tableName, changeLogName)).
		Select(columnNames...).
		Where(goqu.C("id").In(rowIds)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	rawRows, err := conn.Query(query, params...)
	if err != nil {
		return nil, err
	}

	return rawRows, nil
}

func replicateRow(tx *goqu.TxDatabase, event *ChangeLogEvent, pkMap map[string]any) error {
	if event.Type == "insert" || event.Type == "update" {
		return replicateUpsert(tx, event, pkMap)
	}

	if event.Type == "delete" {
		return replicateDelete(tx, event, pkMap)
	}

	return fmt.Errorf("invalid operation type %s", event.Type)
}

func replicateUpsert(tx *goqu.TxDatabase, event *ChangeLogEvent, _ map[string]any) error {
	columnNames := make([]string, 0, len(event.Row))
	columnValues := make([]any, 0, len(event.Row))
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

func replicateDelete(tx *goqu.TxDatabase, event *ChangeLogEvent, pkMap map[string]any) error {
	_, err := tx.Delete(event.TableName).
		Where(goqu.Ex(pkMap)).
		Prepared(true).
		Executor().
		Exec()

	return err
}
