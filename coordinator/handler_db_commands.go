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

// handleShowEngines returns fake MySQL engine list for compatibility
func (h *CoordinatorHandler) handleShowEngines() (*protocol.ResultSet, error) {
	return &protocol.ResultSet{
		Columns: []protocol.ColumnDef{
			{Name: "Engine", Type: 0xFD},
			{Name: "Support", Type: 0xFD},
			{Name: "Comment", Type: 0xFD},
			{Name: "Transactions", Type: 0xFD},
			{Name: "XA", Type: 0xFD},
			{Name: "Savepoints", Type: 0xFD},
		},
		Rows: [][]interface{}{
			{"InnoDB", "DEFAULT", "Marmot SQLite engine with InnoDB compatibility", "YES", "NO", "YES"},
		},
	}, nil
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
