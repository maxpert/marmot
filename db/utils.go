package db

import (
	"database/sql"

	"github.com/rs/zerolog/log"
)

type EnhancedStatement struct {
	*sql.Stmt
}

func (stmt *EnhancedStatement) Finalize() {
	err := stmt.Close()
	if err != nil {
		log.Error().Err(err).Msg("Unable to close statement")
	}
}

type EnhancedRows struct {
	*sql.Rows
}

func (rs *EnhancedRows) fetchRow() (map[string]any, error) {
	columns, err := rs.Columns()
	if err != nil {
		return nil, err
	}

	scanRow := make([]any, len(columns))
	rowPointers := make([]any, len(columns))
	for i := range scanRow {
		rowPointers[i] = &scanRow[i]
	}

	if err := rs.Scan(rowPointers...); err != nil {
		return nil, err
	}

	row := make(map[string]any)
	for i, column := range columns {
		row[column] = scanRow[i]
	}

	return row, nil
}

func (rs *EnhancedRows) Finalize() {
	err := rs.Close()
	if err != nil {
		log.Error().Err(err).Msg("Unable to close result set")
	}
}
