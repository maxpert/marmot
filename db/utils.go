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

func (rs *EnhancedRows) Finalize() {
	err := rs.Close()
	if err != nil {
		log.Error().Err(err).Msg("Unable to close result set")
	}
}
