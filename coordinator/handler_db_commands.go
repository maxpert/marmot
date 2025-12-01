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

// handleCreateDatabase creates a new database
func (h *CoordinatorHandler) handleCreateDatabase(dbName string) (*protocol.ResultSet, error) {
	log.Info().Str("database", dbName).Msg("Creating database")

	err := h.dbManager.CreateDatabase(dbName)
	if err != nil {
		log.Error().Err(err).Str("database", dbName).Msg("Failed to create database")
		return nil, err
	}

	log.Info().Str("database", dbName).Msg("Database created successfully")

	// Return nil for OK response (no result set)
	return nil, nil
}

// handleDropDatabase drops a database
func (h *CoordinatorHandler) handleDropDatabase(dbName string) (*protocol.ResultSet, error) {
	log.Info().Str("database", dbName).Msg("Dropping database")

	// Prevent dropping system database
	if dbName == SystemDatabaseName {
		log.Error().Str("database", dbName).Msg("Cannot drop system database")
		return nil, ErrSystemDatabaseProtected
	}

	err := h.dbManager.DropDatabase(dbName)
	if err != nil {
		log.Error().Err(err).Str("database", dbName).Msg("Failed to drop database")
		return nil, err
	}

	log.Info().Str("database", dbName).Msg("Database dropped successfully")

	// Return nil for OK response (no result set)
	return nil, nil
}
