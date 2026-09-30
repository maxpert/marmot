package db

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
)

const (
	// loadBatchSavepoint scopes one batch of loaded rows, undone whole if
	// any of its rows loses its version stamp.
	loadBatchSavepoint = "marmot_load_batch"
	// loadRowSavepoint scopes one loaded row, so a row that loses its
	// version stamp is undone alone.
	loadRowSavepoint = "marmot_load_row"
	// loadBatchMaxRows and loadBatchMaxParams bound one batch's statement.
	loadBatchMaxRows   = 500
	loadBatchMaxParams = 30_000
)

// tablePrimaryKeyColumns reads table's primary-key columns in key order
// through tx, which sees any DDL earlier in the same transaction; a table
// without a declared key is keyed by its rowid, named as the CDC hook
// names it.
func tablePrimaryKeyColumns(tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.Query("SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk", table)
	if err != nil {
		return nil, fmt.Errorf("read primary key of %s: %w", table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("read primary key of %s: %w", table, err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read primary key of %s: %w", table, err)
	}
	if len(cols) == 0 {
		cols = []string{rowidColumnKey}
	}
	return cols, nil
}

// loadApply is one LOAD DATA payload being applied by a versionedApplier.
type loadApply struct {
	*versionedApplier
	load        *protocol.LoadDataRows
	primaryKeys []string
	version     hlc.Timestamp
}

// applyLoadData applies a LOAD DATA LOCAL payload inside the applier's tx,
// each row versioned at version like a replicated INSERT: a row is kept
// only if its stored key's version stamp wins, so a replayed load never
// overwrites a newer row and an older change replayed later never removes
// a loaded row. A row landing on the key of an older row replaces it, as
// ApplyCDCInsert does: the origin inserted it into a key that was free in
// commit order. Rows go in batches under a savepoint; a batch in which any
// row loses is undone and applied again one row at a time.
func (a *versionedApplier) applyLoadData(loadSQL string, data []byte, version hlc.Timestamp) error {
	load, err := protocol.ParseLoadDataRows(loadSQL, data)
	if err != nil || len(load.Rows) == 0 {
		return err
	}
	primaryKeys, err := tablePrimaryKeyColumns(a.tx, load.Table)
	if err != nil {
		return err
	}
	l := &loadApply{versionedApplier: a, load: load, primaryKeys: primaryKeys, version: version}
	for start := 0; start < len(load.Rows); {
		end := loadBatchEnd(load.Rows, start)
		if err := l.applyBatch(load.Rows[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// loadBatchEnd returns the end of the batch starting at start: rows of the
// same field count, within loadBatchMaxRows and loadBatchMaxParams.
func loadBatchEnd(rows [][]string, start int) int {
	fields := len(rows[start])
	limit := loadBatchMaxRows
	if fields > 0 && loadBatchMaxParams/fields < limit {
		limit = loadBatchMaxParams / fields
	}
	end := start + 1
	for end < len(rows) && end-start < limit && len(rows[end]) == fields {
		end++
	}
	return end
}

// applyBatch writes rows under loadBatchSavepoint and stamps every key they
// were stored under. If any stamp loses, the batch is undone and its rows
// applied one at a time instead.
func (l *loadApply) applyBatch(rows [][]string) error {
	if err := l.savepoint(loadBatchSavepoint); err != nil {
		return err
	}
	keys, err := l.insertReturningKeys(rows)
	allWon := false
	if err == nil {
		allWon, err = l.stampAll(keys)
	}
	if err == nil && !allWon {
		err = l.rollbackTo(loadBatchSavepoint)
	}
	if err == nil {
		err = l.release(loadBatchSavepoint)
	}
	if err != nil || allWon {
		return err
	}
	for _, row := range rows {
		if err := l.applyRow(row); err != nil {
			return err
		}
	}
	return nil
}

// applyRow writes one row under loadRowSavepoint and keeps it only if its
// key's version stamp wins.
func (l *loadApply) applyRow(row []string) error {
	if err := l.savepoint(loadRowSavepoint); err != nil {
		return err
	}
	keys, err := l.insertReturningKeys([][]string{row})
	won := false
	if err == nil {
		won, err = l.stampAll(keys)
	}
	if err == nil && !won {
		err = l.rollbackTo(loadRowSavepoint)
	}
	if err != nil {
		return err
	}
	return l.release(loadRowSavepoint)
}

func (l *loadApply) savepoint(name string) error {
	return l.execLoad("SAVEPOINT " + name)
}

func (l *loadApply) rollbackTo(name string) error {
	return l.execLoad("ROLLBACK TO " + name)
}

func (l *loadApply) release(name string) error {
	return l.execLoad("RELEASE " + name)
}

func (l *loadApply) execLoad(stmt string) error {
	if _, err := l.tx.Exec(stmt); err != nil {
		return fmt.Errorf("LOAD DATA into %s: %s: %w", l.load.Table, stmt, err)
	}
	return nil
}

// insertReturningKeys writes rows with one INSERT OR REPLACE and returns the
// intent key of each row as stored, after SQLite's type affinity, encoded
// as the CDC hook encodes it.
func (l *loadApply) insertReturningKeys(rows [][]string) ([][]byte, error) {
	var cols string
	if len(l.load.Columns) > 0 {
		cols = " (" + strings.Join(quoteSQLiteIdentList(l.load.Columns), ", ") + ")"
	}
	tuple := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(rows[0])), ", ") + ")"
	stmt := fmt.Sprintf("INSERT OR REPLACE INTO %s%s VALUES %s RETURNING %s", quoteSQLiteIdent(l.load.Table), cols,
		strings.TrimSuffix(strings.Repeat(tuple+", ", len(rows)), ", "), strings.Join(quoteSQLiteIdentList(l.primaryKeys), ", "))
	args := make([]interface{}, 0, len(rows)*len(rows[0]))
	for _, row := range rows {
		for _, field := range row {
			args = append(args, field)
		}
	}
	result, err := l.tx.Query(stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("LOAD DATA into %s: %w", l.load.Table, err)
	}
	defer result.Close()
	keys := make([][]byte, 0, len(rows))
	pk := make([]interface{}, len(l.primaryKeys))
	ptrs := make([]interface{}, len(pk))
	for i := range pk {
		ptrs[i] = &pk[i]
	}
	pkValues := make([]filter.TypedPKValue, len(pk))
	for result.Next() {
		if err := result.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("LOAD DATA key of %s: %w", l.load.Table, err)
		}
		for i, v := range pk {
			pkValues[i] = capturedPKValue(v)
		}
		keys = append(keys, filter.EncodeIntentKey(l.load.Table, pkValues))
	}
	return keys, result.Err()
}

// stampAll stamps every key at the load's version with one upsert and
// reports whether every stamp won: the upsert changes one row per key it
// wrote, and none for a key whose stored version is newer. A key repeated
// in keys (a payload loading one key twice) conflicts with its own earlier
// row at an equal version, which wins, so it still counts once per row.
func (l *loadApply) stampAll(keys [][]byte) (bool, error) {
	if len(keys) == 0 {
		return true, nil
	}
	args := make([]interface{}, 0, len(keys)*6)
	for _, key := range keys {
		args = append(args, l.load.Table, key, l.version.WallTime, l.version.Logical, l.version.NodeID, false)
	}
	res, err := l.tx.Exec(stampRowVersionsSQL(len(keys)), args...)
	if err != nil {
		return false, fmt.Errorf("stamp row versions for %s: %w", l.load.Table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("stamp row versions for %s: %w", l.load.Table, err)
	}
	return n == int64(len(keys)), nil
}
