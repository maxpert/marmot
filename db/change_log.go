package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/utils"

	_ "embed"

	"github.com/doug-martin/goqu/v9"
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

var ErrNoTableMapping = errors.New("no table mapping found")
var ErrLogNotReadyToPublish = errors.New("not ready to publish changes")
var ErrEndOfWatch = errors.New("watching event finished")

//go:embed table_change_log_script.tmpl
var tableChangeLogScriptTemplate string

//go:embed global_change_log_script.tmpl
var globalChangeLogScriptTemplate string
var tableChangeLogTpl *template.Template
var globalChangeLogTpl *template.Template

var spaceStripper = regexp.MustCompile(`\n\s+`)

type ChangeLogState = int16

const (
	Pending   ChangeLogState = 0
	Published ChangeLogState = 1
	Failed    ChangeLogState = -1
)
const changeLogName = "change_log"
const upsertQuery = `INSERT OR REPLACE INTO %s(%s) VALUES (%s)`

type globalChangeLogTemplateData struct {
	Prefix string
}

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

	globalChangeLogTpl = template.Must(
		template.New("globalChangeLogScriptTemplate").Parse(globalChangeLogScriptTemplate),
	)
}

func (conn *SqliteStreamDB) Replicate(event *ChangeLogEvent) error {
	if err := conn.consumeReplicationEvent(event); err != nil {
		return err
	}
	return nil
}

func (conn *SqliteStreamDB) CleanupChangeLogs(beforeTime time.Time) (int64, error) {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return 0, err
	}
	defer sqlConn.Return()

	total := int64(0)
	for name := range conn.watchTablesSchema {
		metaTableName := conn.metaTable(name, changeLogName)
		rs, err := sqlConn.DB().Delete(metaTableName).
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

func (conn *SqliteStreamDB) globalCDCScript() (string, error) {
	buf := new(bytes.Buffer)
	err := globalChangeLogTpl.Execute(buf, &globalChangeLogTemplateData{
		Prefix: conn.prefix,
	})

	if err != nil {
		return "", err
	}

	return spaceStripper.ReplaceAllString(buf.String(), "\n    "), nil
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
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return err
	}
	defer sqlConn.Return()

	return sqlConn.DB().WithTx(func(tnx *goqu.TxDatabase) error {
		primaryKeyMap := conn.getPrimaryKeyMap(event)
		if primaryKeyMap == nil {
			return ErrNoTableMapping
		}

		logEv := log.Debug().
			Int64("event_id", event.Id).
			Str("type", event.Type)

		for k, v := range primaryKeyMap {
			logEv = logEv.Str(event.TableName+"."+k, fmt.Sprintf("%v", v))
		}

		logEv.Send()

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

func (conn *SqliteStreamDB) initGlobalChangeLog() error {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return err
	}
	defer sqlConn.Return()

	script, err := conn.globalCDCScript()
	if err != nil {
		return err
	}

	log.Info().Msg("Creating global change log table")
	_, err = sqlConn.DB().Exec(script)
	if err != nil {
		return err
	}

	return nil
}

func (conn *SqliteStreamDB) initTriggers(tableName string) error {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return err
	}
	defer sqlConn.Return()

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
	_, err = sqlConn.DB().Exec(script)
	if err != nil {
		return err
	}

	return nil
}

func (conn *SqliteStreamDB) filterChangesTo(changed chan fsnotify.Event, watcher *fsnotify.Watcher) {
	for {
		select {
		case ev, ok := <-watcher.Events:
			if !ok {
				close(changed)
				return
			}

			if ev.Op == fsnotify.Chmod {
				continue
			}

			changed <- ev
		}
	}
}

func (conn *SqliteStreamDB) watchChanges(watcher *fsnotify.Watcher, path string) {
	shmPath := path + "-shm"
	walPath := path + "-wal"

	errDB := watcher.Add(path)
	errShm := watcher.Add(shmPath)
	errWal := watcher.Add(walPath)
	dbChanged := make(chan fsnotify.Event)

	tickerDur := time.Duration(cfg.Config.PollingInterval) * time.Millisecond
	changeLogTicker := utils.NewTimeoutPublisher(tickerDur)

	// Publish change logs for any residual change logs before starting watcher
	conn.publishChangeLog()
	go conn.filterChangesTo(dbChanged, watcher)

	for {
		changeLogTicker.Reset()

		err := conn.WithReadTx(func(_tx *sql.Tx) error {
			select {
			case ev, ok := <-dbChanged:
				if !ok {
					return ErrEndOfWatch
				}

				log.Debug().Int("change", int(ev.Op)).Msg("Change detected")
				conn.publishChangeLog()
			case <-changeLogTicker.Channel():
				log.Debug().Dur("timeout", tickerDur).Msg("Change polling timeout")
				conn.publishChangeLog()
			}

			return nil
		})

		if err != nil {
			log.Warn().Err(err).Msg("Error watching changes; trying to resubscribe...")
			errDB = watcher.Add(path)
			errShm = watcher.Add(shmPath)
			errWal = watcher.Add(walPath)
		}

		if errDB != nil {
			errDB = watcher.Add(path)
		}

		if errShm != nil {
			errShm = watcher.Add(shmPath)
		}

		if errWal != nil {
			errWal = watcher.Add(walPath)
		}
	}
}

func (conn *SqliteStreamDB) getGlobalChanges(limit uint32) ([]globalChangeLogEntry, error) {
	sw := utils.NewStopWatch("scan_changes")
	defer sw.Log(log.Debug(), conn.stats.scanChanges)

	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return nil, err
	}
	defer sqlConn.Return()

	var entries []globalChangeLogEntry
	err = sqlConn.DB().
		From(conn.globalMetaTable()).
		Order(goqu.I("id").Asc()).
		Limit(uint(limit)).
		ScanStructs(&entries)

	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (conn *SqliteStreamDB) countChanges() (int64, error) {
	sw := utils.NewStopWatch("count_changes")
	defer sw.Log(log.Debug(), conn.stats.countChanges)

	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return -1, err
	}
	defer sqlConn.Return()

	return sqlConn.DB().
		From(conn.globalMetaTable()).
		Count()
}

func (conn *SqliteStreamDB) publishChangeLog() {
	if !conn.publishLock.TryLock() {
		log.Warn().Msg("Publish in progress skipping...")
		return
	}
	defer conn.publishLock.Unlock()

	cnt, err := conn.countChanges()
	if err != nil {
		log.Error().Err(err).Msg("Unable to count global changes")
		return
	}

	conn.stats.pendingPublish.Set(float64(cnt))
	if cnt <= 0 {
		log.Debug().Msg("no new rows")
		return
	}

	changes, err := conn.getGlobalChanges(cfg.Config.ScanMaxChanges)
	if err != nil {
		log.Error().Err(err).Msg("Unable to scan global changes")
		return
	}

	if len(changes) < 0 {
		return
	}

	for _, change := range changes {
		logEntry := changeLogEntry{}
		found := false
		found, err = conn.getChangeEntry(&logEntry, change)

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

		err = conn.consumeChangeLogs(change.TableName, []*changeLogEntry{&logEntry})
		if err != nil {
			if errors.Is(err, ErrLogNotReadyToPublish) || errors.Is(err, context.Canceled) {
				break
			}

			log.Error().Err(err).Msg("Unable to consume changes")
		}

		err = conn.markChangePublished(change)
		if err != nil {
			log.Error().Err(err).Msg("Unable to cleanup change log")
		}

		conn.stats.published.Inc()
	}
}

func (conn *SqliteStreamDB) markChangePublished(change globalChangeLogEntry) error {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return err
	}
	defer sqlConn.Return()

	return sqlConn.DB().WithTx(func(tx *goqu.TxDatabase) error {
		_, err = tx.Update(conn.metaTable(change.TableName, changeLogName)).
			Set(goqu.Record{"state": Published}).
			Where(goqu.Ex{"id": change.ChangeTableId}).
			Prepared(true).
			Executor().
			Exec()

		if err != nil {
			return err
		}

		_, err = tx.Delete(conn.globalMetaTable()).
			Where(goqu.C("id").Eq(change.Id)).
			Prepared(true).
			Executor().
			Exec()

		if err != nil {
			return err
		}

		return nil
	})
}

func (conn *SqliteStreamDB) getChangeEntry(entry *changeLogEntry, change globalChangeLogEntry) (bool, error) {
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return false, err
	}
	defer sqlConn.Return()

	return sqlConn.DB().Select("id", "type", "state").
		From(conn.metaTable(change.TableName, changeLogName)).
		Where(
			goqu.C("state").Eq(Pending),
			goqu.C("id").Eq(change.ChangeTableId),
		).
		Prepared(true).
		ScanStruct(entry)
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
				if err == ErrLogNotReadyToPublish || err == context.Canceled {
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
	sqlConn, err := conn.pool.Borrow()
	if err != nil {
		return nil, err
	}
	defer sqlConn.Return()

	columnNames := make([]any, 0)
	tableCols := conn.watchTablesSchema[tableName]
	columnNames = append(columnNames, goqu.C("id").As(idColumnName))
	for _, col := range tableCols {
		columnNames = append(columnNames, goqu.C("val_"+col.Name).As(col.Name))
	}

	query, params, err := sqlConn.DB().From(conn.metaTable(tableName, changeLogName)).
		Select(columnNames...).
		Where(goqu.C("id").In(rowIds)).
		Prepared(true).
		ToSQL()
	if err != nil {
		return nil, err
	}

	rawRows, err := sqlConn.DB().Query(query, params...)
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
