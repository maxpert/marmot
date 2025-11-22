package coordinator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// DatabaseManager interface to avoid import cycles
type DatabaseManager interface {
	ListDatabases() []string
	DatabaseExists(name string) bool
	CreateDatabase(name string) error
	DropDatabase(name string) error
}

// SchemaVersionManager interface to avoid import cycles
type SchemaVersionManager interface {
	GetSchemaVersion(database string) (uint64, error)
	IncrementSchemaVersion(database string, ddlSQL string, txnID uint64) (uint64, error)
	GetAllSchemaVersions() (map[string]uint64, error)
}

// NodeRegistry interface to avoid import cycles
type NodeRegistry interface {
	UpdateSchemaVersions(versions map[string]uint64)
}

// CoordinatorHandler implements protocol.ConnectionHandler
// It routes queries to the appropriate coordinator (Read or Write)
type CoordinatorHandler struct {
	nodeID           uint64
	writeCoord       *WriteCoordinator
	readCoord        *ReadCoordinator
	clock            *hlc.Clock
	dbManager        DatabaseManager
	ddlLockMgr       *DDLLockManager
	schemaVersionMgr SchemaVersionManager
	nodeRegistry     NodeRegistry
}

// NewCoordinatorHandler creates a new handler
func NewCoordinatorHandler(nodeID uint64, writeCoord *WriteCoordinator, readCoord *ReadCoordinator, clock *hlc.Clock, dbManager DatabaseManager, ddlLockMgr *DDLLockManager, schemaVersionMgr SchemaVersionManager, nodeRegistry NodeRegistry) *CoordinatorHandler {
	return &CoordinatorHandler{
		nodeID:           nodeID,
		writeCoord:       writeCoord,
		readCoord:        readCoord,
		clock:            clock,
		dbManager:        dbManager,
		ddlLockMgr:       ddlLockMgr,
		schemaVersionMgr: schemaVersionMgr,
		nodeRegistry:     nodeRegistry,
	}
}

// HandleQuery processes a SQL query
func (h *CoordinatorHandler) HandleQuery(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	log.Debug().
		Uint64("conn_id", session.ConnID).
		Str("database", session.CurrentDatabase).
		Str("query", query).
		Msg("Handling query")

	// Intercept MySQL system variable queries
	if strings.Contains(query, "@@") || strings.Contains(strings.ToUpper(query), "DATABASE()") {
		return h.handleSystemQuery(session, query)
	}

	stmt := protocol.ParseStatement(query)

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Int("stmt_type", int(stmt.Type)).
		Str("table", stmt.TableName).
		Str("database", stmt.Database).
		Msg("Parsed statement")

	// Handle SET commands as no-op (return OK)
	if stmt.Type == protocol.StatementSet {
		return nil, nil
	}

	// Handle database management commands
	switch stmt.Type {
	case protocol.StatementShowDatabases:
		return h.handleShowDatabases()
	case protocol.StatementUseDatabase:
		return h.handleUseDatabase(session, stmt.Database)
	case protocol.StatementCreateDatabase:
		return h.handleCreateDatabase(stmt.Database)
	case protocol.StatementDropDatabase:
		return h.handleDropDatabase(stmt.Database)
	}

	// Handle metadata queries (for DBeaver compatibility)
	switch stmt.Type {
	case protocol.StatementShowTables:
		return h.handleShowTables(session, stmt)
	case protocol.StatementShowColumns:
		return h.handleShowColumns(session, stmt)
	case protocol.StatementShowCreateTable:
		return h.handleShowCreateTable(session, stmt)
	case protocol.StatementShowIndexes:
		return h.handleShowIndexes(session, stmt)
	case protocol.StatementShowTableStatus:
		return h.handleShowTableStatus(session, stmt)
	case protocol.StatementInformationSchema:
		return h.handleInformationSchema(session, query)
	}

	// Set database context from session if not specified in statement
	if stmt.Database == "" {
		stmt.Database = session.CurrentDatabase
	}

	// Check for consistency hint
	consistency, _ := protocol.ExtractConsistencyHint(query)

	if protocol.IsMutation(stmt) {
		return h.handleMutation(stmt, consistency)
	}

	return h.handleRead(stmt, consistency)
}

func (h *CoordinatorHandler) handleMutation(stmt protocol.Statement, consistency protocol.ConsistencyLevel) (*protocol.ResultSet, error) {
	// Create transaction
	txnID := h.clock.Now().WallTime // Simple ID generation for now
	startTS := h.clock.Now()

	// Detect DDL and handle differently
	isDDL := stmt.Type == protocol.StatementDDL

	// Rewrite DDL for idempotency (safe to replay)
	if isDDL {
		originalSQL := stmt.SQL
		stmt.SQL = protocol.RewriteDDLForIdempotency(originalSQL)
		if stmt.SQL != originalSQL {
			log.Debug().
				Str("original", originalSQL).
				Str("rewritten", stmt.SQL).
				Msg("Rewrote DDL for idempotency")
		}
	}

	if isDDL && h.ddlLockMgr != nil {
		// Acquire cluster-wide DDL lock for this database
		_, err := h.ddlLockMgr.AcquireLock(stmt.Database, h.nodeID, uint64(txnID), startTS)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire DDL lock: %w", err)
		}
		// Release lock when done (even if transaction fails)
		defer func() {
			if releaseErr := h.ddlLockMgr.ReleaseLock(stmt.Database, uint64(txnID)); releaseErr != nil {
				log.Error().Err(releaseErr).Str("database", stmt.Database).Msg("Failed to release DDL lock")
			}
		}()
	}

	// Get current schema version for this database
	var schemaVersion uint64
	if h.schemaVersionMgr != nil {
		var err error
		schemaVersion, err = h.schemaVersionMgr.GetSchemaVersion(stmt.Database)
		if err != nil {
			log.Warn().Err(err).Str("database", stmt.Database).Msg("Failed to get schema version, using 0")
			schemaVersion = 0
		}
	}

	txn := &Transaction{
		ID:                    uint64(txnID),
		NodeID:                h.nodeID,
		Statements:            []protocol.Statement{stmt},
		StartTS:               startTS,
		WriteConsistency:      consistency,
		Database:              stmt.Database,
		RequiredSchemaVersion: schemaVersion, // Current version required
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.writeCoord.WriteTransaction(ctx, txn); err != nil {
		return nil, err
	}

	// If DDL succeeded, increment schema version
	if isDDL && h.schemaVersionMgr != nil {
		newVersion, err := h.schemaVersionMgr.IncrementSchemaVersion(stmt.Database, stmt.SQL, uint64(txnID))
		if err != nil {
			log.Error().Err(err).Str("database", stmt.Database).Msg("Failed to increment schema version")
		} else {
			log.Info().
				Str("database", stmt.Database).
				Uint64("new_version", newVersion).
				Uint64("txn_id", uint64(txnID)).
				Msg("Schema version incremented after DDL")

			// Update gossip with new schema versions
			if h.nodeRegistry != nil {
				allVersions, err := h.schemaVersionMgr.GetAllSchemaVersions()
				if err != nil {
					log.Error().Err(err).Msg("Failed to get schema versions for gossip")
				} else {
					h.nodeRegistry.UpdateSchemaVersions(allVersions)
				}
			}
		}
	}

	// Return OK result
	return nil, nil
}

func (h *CoordinatorHandler) handleRead(stmt protocol.Statement, consistency protocol.ConsistencyLevel) (*protocol.ResultSet, error) {
	req := &ReadRequest{
		Query:       stmt.SQL,
		SnapshotTS:  h.clock.Now(),
		Consistency: consistency,
		TableName:   stmt.TableName,
		Database:    stmt.Database,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := h.readCoord.ReadTransaction(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	// Convert to protocol.ResultSet
	rs := &protocol.ResultSet{
		Columns: make([]protocol.ColumnDef, 0),
		Rows:    make([][]interface{}, 0),
	}

	if len(resp.Rows) > 0 || len(resp.Columns) > 0 {
		// Use columns from response if available (preserves order)
		if len(resp.Columns) > 0 {
			for _, colName := range resp.Columns {
				rs.Columns = append(rs.Columns, protocol.ColumnDef{
					Name: colName,
					Type: 0xFD, // VAR_STRING
				})
			}
		} else if len(resp.Rows) > 0 {
			// Fallback: Infer columns from first row (random order)
			firstRow := resp.Rows[0]
			for colName := range firstRow {
				rs.Columns = append(rs.Columns, protocol.ColumnDef{
					Name: colName,
					Type: 0xFD, // VAR_STRING
				})
			}
		}

		for _, rowMap := range resp.Rows {
			row := make([]interface{}, len(rs.Columns))
			for i, col := range rs.Columns {
				row[i] = rowMap[col.Name]
			}
			rs.Rows = append(rs.Rows, row)
		}
	}

	return rs, nil
}

func (h *CoordinatorHandler) handleSystemQuery(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	// Parse column names from SELECT statement
	// Example: SELECT @@version AS version, @@sql_mode AS sql_mode

	// Extract the SELECT portion (remove comments and LIMIT)
	queryUpper := strings.ToUpper(query)
	selectIdx := strings.Index(queryUpper, "SELECT")
	if selectIdx == -1 {
		return &protocol.ResultSet{
			Columns: []protocol.ColumnDef{{Name: "Value", Type: 0xFD}},
			Rows:    [][]interface{}{{""}},
		}, nil
	}

	// Find FROM or LIMIT or end of string
	fromIdx := strings.Index(queryUpper[selectIdx:], "FROM")
	limitIdx := strings.Index(queryUpper[selectIdx:], "LIMIT")
	endIdx := len(query)

	if fromIdx != -1 {
		endIdx = selectIdx + fromIdx
	} else if limitIdx != -1 {
		endIdx = selectIdx + limitIdx
	}

	// Extract column list
	columnsPart := strings.TrimSpace(query[selectIdx+6 : endIdx])

	// Split by comma (simple split, doesn't handle nested functions perfectly)
	parts := strings.Split(columnsPart, ",")

	var columns []protocol.ColumnDef
	var values []interface{}

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Extract alias using AS or just the last word
		var colName string
		if strings.Contains(strings.ToUpper(part), " AS ") {
			asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
			colName = strings.TrimSpace(part[asIdx+4:])
		} else {
			// Use the variable name itself
			words := strings.Fields(part)
			if len(words) > 0 {
				colName = words[len(words)-1]
			} else {
				colName = "value"
			}
		}

		// Clean up column name (remove backticks, quotes)
		colName = strings.Trim(colName, "`'\"")

		columns = append(columns, protocol.ColumnDef{Name: colName, Type: 0xFD})

		// Return appropriate values for known variables
		value := h.getSystemVariable(part, session)
		values = append(values, value)
	}

	if len(columns) == 0 {
		columns = []protocol.ColumnDef{{Name: "Value", Type: 0xFD}}
		values = []interface{}{""}
	}

	return &protocol.ResultSet{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

func (h *CoordinatorHandler) getSystemVariable(varExpr string, session *protocol.ConnectionSession) interface{} {
	varExpr = strings.ToLower(varExpr)

	// Common system variables
	if strings.Contains(varExpr, "version") {
		return "8.0.0-marmot"
	}
	if strings.Contains(varExpr, "database()") {
		return session.CurrentDatabase
	}
	if strings.Contains(varExpr, "autocommit") {
		return 1
	}
	if strings.Contains(varExpr, "auto_increment") {
		return 1
	}
	if strings.Contains(varExpr, "sql_mode") {
		return "STRICT_TRANS_TABLES"
	}
	if strings.Contains(varExpr, "character_set") || strings.Contains(varExpr, "charset") {
		return "utf8mb4"
	}
	if strings.Contains(varExpr, "collation") {
		return "utf8mb4_general_ci"
	}
	if strings.Contains(varExpr, "system_time_zone") {
		return "UTC"
	}
	if strings.Contains(varExpr, "time_zone") {
		return "SYSTEM"
	}
	if strings.Contains(varExpr, "interactive_timeout") {
		return 28800
	}
	if strings.Contains(varExpr, "wait_timeout") {
		return 28800
	}
	if strings.Contains(varExpr, "net_write_timeout") {
		return 60
	}
	if strings.Contains(varExpr, "timeout") {
		return 28800
	}
	if strings.Contains(varExpr, "max_allowed_packet") {
		return 67108864
	}
	if strings.Contains(varExpr, "lower_case_table_names") {
		return 0
	}
	if strings.Contains(varExpr, "transaction_isolation") || strings.Contains(varExpr, "tx_isolation") {
		return "REPEATABLE-READ"
	}
	if strings.Contains(varExpr, "tx_read_only") || strings.Contains(varExpr, "read_only") {
		return 0
	}
	if strings.Contains(varExpr, "performance_schema") {
		return 0
	}
	if strings.Contains(varExpr, "query_cache") {
		return 0
	}
	if strings.Contains(varExpr, "license") {
		return "Apache-2.0"
	}
	if strings.Contains(varExpr, "init_connect") {
		return ""
	}

	// Default: return empty string for unknown variables
	// Note: This may cause issues with JDBC if it expects a numeric value
	return ""
}
