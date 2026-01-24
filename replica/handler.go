package replica

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/handlers"

	"github.com/rs/zerolog/log"
)

// ReadOnlyHandler implements protocol.ConnectionHandler for read-only replicas
// It rejects all mutations and executes reads locally
type ReadOnlyHandler struct {
	dbManager *db.DatabaseManager
	clock     *hlc.Clock
	replica   *Replica
	metadata  *handlers.MetadataHandler
}

// NewReadOnlyHandler creates a new read-only handler
func NewReadOnlyHandler(dbManager *db.DatabaseManager, clock *hlc.Clock, replica *Replica) *ReadOnlyHandler {
	return &ReadOnlyHandler{
		dbManager: dbManager,
		clock:     clock,
		replica:   replica,
		metadata:  handlers.NewMetadataHandler(dbManager, db.SystemDatabaseName),
	}
}

// HandleQuery processes a SQL query (read-only)
func (h *ReadOnlyHandler) HandleQuery(session *protocol.ConnectionSession, sql string, params []interface{}) (*protocol.ResultSet, error) {
	log.Debug().
		Uint64("conn_id", session.ConnID).
		Str("database", session.CurrentDatabase).
		Str("query", sql).
		Msg("Handling query (read-only)")

	// Handle Marmot session commands first (SET marmot_*, LOAD EXTENSION)
	// These are handled before parsing since they control parsing behavior
	if protocol.IsMarmotSessionCommand(sql) {
		return h.handleMarmotCommand(session, sql)
	}

	// Parse with options based on session state
	stmt := protocol.ParseStatementWithOptions(sql, protocol.ParseOptions{
		SkipTranspilation: !session.TranspilationEnabled,
	})

	// Handle system variable queries (@@version, DATABASE(), etc.)
	if stmt.Type == protocol.StatementSystemVariable {
		return h.handleSystemQuery(session, stmt)
	}

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Int("stmt_type", int(stmt.Type)).
		Str("table", stmt.TableName).
		Str("database", stmt.Database).
		Bool("transpilation", session.TranspilationEnabled).
		Msg("Parsed statement")

	// Reject all mutations with MySQL read-only error
	if protocol.IsMutation(stmt) {
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Int("stmt_type", int(stmt.Type)).
			Msg("Rejecting mutation on read-only replica")
		return nil, protocol.ErrReadOnly()
	}

	// Reject transaction control for writes (BEGIN is ok for reads, but COMMIT on writes is not)
	// Allow read-only transactions for compatibility
	if protocol.IsTransactionControl(stmt) {
		// Allow BEGIN/START TRANSACTION (for read-only transactions)
		// Allow COMMIT/ROLLBACK (no-op for read-only)
		switch stmt.Type {
		case protocol.StatementBegin:
			// For read-only replica, we don't need real transaction tracking
			// Just set a flag for client compatibility
			return nil, nil
		case protocol.StatementCommit, protocol.StatementRollback:
			return nil, nil
		default:
			// Savepoints, XA transactions - reject
			return nil, protocol.ErrReadOnly()
		}
	}

	// Handle SET commands as no-op (return OK)
	if stmt.Type == protocol.StatementSet {
		return nil, nil
	}

	// Handle database management commands (read-only)
	switch stmt.Type {
	case protocol.StatementShowDatabases:
		return h.metadata.HandleShowDatabases()
	case protocol.StatementUseDatabase:
		if err := h.metadata.HandleUseDatabase(stmt.Database); err != nil {
			return nil, err
		}
		session.CurrentDatabase = stmt.Database
		return nil, nil
	}

	// Handle metadata queries
	switch stmt.Type {
	case protocol.StatementShowTables:
		dbName := stmt.Database
		if dbName == "" {
			dbName = session.CurrentDatabase
		}
		return h.metadata.HandleShowTables(dbName)
	case protocol.StatementShowColumns:
		dbName := stmt.Database
		if dbName == "" {
			dbName = session.CurrentDatabase
		}
		return h.metadata.HandleShowColumns(dbName, stmt.TableName)
	case protocol.StatementShowCreateTable:
		dbName := stmt.Database
		if dbName == "" {
			dbName = session.CurrentDatabase
		}
		return h.metadata.HandleShowCreateTable(dbName, stmt.TableName)
	case protocol.StatementShowIndexes:
		dbName := stmt.Database
		if dbName == "" {
			dbName = session.CurrentDatabase
		}
		return h.metadata.HandleShowIndexes(dbName, stmt.TableName)
	case protocol.StatementShowTableStatus:
		dbName := stmt.Database
		if dbName == "" {
			dbName = session.CurrentDatabase
		}
		return h.metadata.HandleShowTableStatus(dbName, stmt.TableName)
	case protocol.StatementInformationSchema:
		return h.metadata.HandleInformationSchema(session.CurrentDatabase, stmt)
	}

	// Set database context from session if not specified in statement
	if stmt.Database == "" {
		stmt.Database = session.CurrentDatabase
	}

	// Execute read locally
	return h.executeLocalRead(stmt, params)
}

// handleSystemQuery handles MySQL system variable queries
func (h *ReadOnlyHandler) handleSystemQuery(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	config := handlers.SystemVarConfig{
		ReadOnly:       true,
		VersionComment: "Marmot Read-Only Replica",
		ConnID:         session.ConnID,
		CurrentDB:      session.CurrentDatabase,
		FoundRowsCount: session.FoundRowsCount.Load(),
		LastInsertId:   session.LastInsertId.Load(),
	}
	return handlers.HandleSystemVariableQuery(stmt, config)
}

// executeLocalRead executes a read query locally
func (h *ReadOnlyHandler) executeLocalRead(stmt protocol.Statement, params []interface{}) (*protocol.ResultSet, error) {
	if stmt.Database == "" {
		return nil, fmt.Errorf("ERROR 1046 (3D000): No database selected")
	}

	sqlDB, err := h.dbManager.GetDatabaseConnection(stmt.Database)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := sqlDB.QueryContext(ctx, stmt.SQL, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := &protocol.ResultSet{
		Columns: make([]protocol.ColumnDef, len(columns)),
		Rows:    make([][]interface{}, 0),
	}

	for i, col := range columns {
		result.Columns[i] = protocol.ColumnDef{Name: col}
	}

	// Scan rows
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Convert sql.RawBytes to string for proper MySQL encoding
		row := make([]interface{}, len(columns))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}

		result.Rows = append(result.Rows, row)
	}

	return result, rows.Err()
}

// handleMarmotCommand handles Marmot session commands (SET marmot_*, LOAD EXTENSION).
func (h *ReadOnlyHandler) handleMarmotCommand(session *protocol.ConnectionSession, sql string) (*protocol.ResultSet, error) {
	// Handle SET marmot_transpilation
	if varType, value, matched := protocol.ParseMarmotSetCommand(sql); matched {
		if varType == "transpilation" {
			session.TranspilationEnabled = (value == "ON")
			log.Debug().
				Uint64("conn_id", session.ConnID).
				Bool("transpilation", session.TranspilationEnabled).
				Msg("Session transpilation updated")
			return nil, nil
		}
	}

	// Handle LOAD EXTENSION
	if extName, matched := protocol.ParseLoadExtensionCommand(sql); matched {
		extLoader := protocol.GetExtensionLoader()
		if extLoader == nil {
			return nil, fmt.Errorf("extension loading not configured (no extensions.directory set)")
		}

		if err := extLoader.LoadExtensionGlobally(extName); err != nil {
			return nil, err
		}

		log.Info().
			Uint64("conn_id", session.ConnID).
			Str("extension", extName).
			Msg("Extension loaded globally")
		return nil, nil
	}

	return nil, fmt.Errorf("unrecognized marmot command: %s", sql)
}
