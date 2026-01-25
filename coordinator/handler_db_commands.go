package coordinator

import (
	"errors"

	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// SystemDatabaseName must match db.SystemDatabaseName (avoid import cycle)
const SystemDatabaseName = "__marmot_system"

var ErrSystemDatabaseProtected = errors.New("cannot drop system database")

// handleShowDatabases returns list of all databases
func (h *CoordinatorHandler) handleShowDatabases() (*protocol.ResultSet, error) {
	if h.dbManager == nil {
		// Fallback if dbManager not set
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{{Name: "Database", Type: 0xFD}},
			Rows:    [][]interface{}{{"marmot"}},
		}, nil
	}

	return h.metadata.HandleShowDatabases()
}

// handleUseDatabase changes the current database for the session
func (h *CoordinatorHandler) handleUseDatabase(session *protocol.ConnectionSession, dbName string) (*protocol.ResultSet, error) {
	if h.dbManager != nil && !h.dbManager.DatabaseExists(dbName) {
		return nil, errors.New("database does not exist: " + dbName)
	}

	session.CurrentDatabase = dbName
	log.Debug().Str("database", dbName).Uint64("conn_id", session.ConnID).Msg("Switched database")

	return nil, nil
}
