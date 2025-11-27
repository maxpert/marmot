package coordinator

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/handlers"
	"github.com/rs/zerolog/log"
)

// DatabaseManager interface to avoid import cycles
type DatabaseManager interface {
	ListDatabases() []string
	DatabaseExists(name string) bool
	CreateDatabase(name string) error
	DropDatabase(name string) error
	GetDatabaseConnection(name string) (*sql.DB, error)
	// GetMVCCDatabase returns the MVCCDatabase for executing with hooks
	GetMVCCDatabase(name string) (MVCCDatabaseProvider, error)
}

// MVCCDatabaseProvider provides access to MVCC database operations
type MVCCDatabaseProvider interface {
	ExecuteLocalWithHooks(ctx context.Context, txnID uint64, statements []protocol.Statement) (PendingExecution, error)
}

// PendingExecution represents a locally executed transaction waiting for quorum
type PendingExecution interface {
	GetRowCounts() map[string]int64
	GetTotalRowCount() int64
	BuildFilters() map[string][]byte
	Commit() error
	Rollback() error
	// FlushIntentLog is a no-op with SQLite-backed intent storage (WAL handles durability).
	// Kept for API compatibility.
	FlushIntentLog() error
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
	CountAlive() int
	GetAll() []any // Returns slice of node states (avoids import cycle)
}

// NodeState represents cluster node state
type NodeState struct {
	NodeId      uint64
	Address     string
	Status      NodeStatus
	Incarnation uint64
}

// NodeStatus enum
type NodeStatus int32

const (
	NodeStatus_JOINING NodeStatus = 0
	NodeStatus_ALIVE   NodeStatus = 1
	NodeStatus_SUSPECT NodeStatus = 2
	NodeStatus_DEAD    NodeStatus = 3
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatus_JOINING:
		return "JOINING"
	case NodeStatus_ALIVE:
		return "ALIVE"
	case NodeStatus_SUSPECT:
		return "SUSPECT"
	case NodeStatus_DEAD:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
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
	metadata         *handlers.MetadataHandler
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
		metadata:         handlers.NewMetadataHandler(dbManager, SystemDatabaseName),
	}
}

// HandleQuery processes a SQL query
func (h *CoordinatorHandler) HandleQuery(session *protocol.ConnectionSession, query string) (*protocol.ResultSet, error) {
	log.Trace().
		Uint64("conn_id", session.ConnID).
		Str("database", session.CurrentDatabase).
		Str("query", query).
		Msg("Handling query")

	// Parse first - all routing decisions based on parsed Statement
	stmt := protocol.ParseStatement(query)

	// Handle system variable queries (@@version, DATABASE(), etc.)
	if stmt.Type == protocol.StatementSystemVariable {
		return h.handleSystemQuery(session, stmt)
	}

	// Handle virtual table queries (MARMOT_CLUSTER_NODES, etc.)
	if stmt.Type == protocol.StatementVirtualTable {
		return h.handleVirtualTableQuery(session, stmt)
	}

	log.Trace().
		Uint64("conn_id", session.ConnID).
		Int("stmt_type", int(stmt.Type)).
		Str("table", stmt.TableName).
		Msg("Parsed statement")

	// Handle SET commands as no-op (return OK)
	if stmt.Type == protocol.StatementSet {
		return nil, nil
	}

	// Handle transaction control statements (BEGIN/COMMIT/ROLLBACK)
	if protocol.IsTransactionControl(stmt) {
		return h.handleTransactionControl(session, stmt)
	}

	// Handle database management commands
	switch stmt.Type {
	case protocol.StatementShowDatabases:
		return h.handleShowDatabases()
	case protocol.StatementUseDatabase:
		return h.handleUseDatabase(session, stmt.Database)
	}

	// Handle metadata queries (for DBeaver compatibility)
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

	// Generate row key for DML statements with CDC data
	// This must happen here (coordinator layer) because:
	// 1. CDC extraction has already populated NewValues/OldValues
	// 2. We have database context available
	// 3. Row key is needed for MVCC write intents (distributed locking)
	if protocol.IsMutation(stmt) && stmt.RowKey == "" {
		// Only generate for DML operations that have CDC data
		isDML := stmt.Type == protocol.StatementInsert ||
			stmt.Type == protocol.StatementUpdate ||
			stmt.Type == protocol.StatementDelete ||
			stmt.Type == protocol.StatementReplace

		if isDML && (len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0) {
			// Get database connection to access schema
			sqlDB, err := h.dbManager.GetDatabaseConnection(stmt.Database)
			if err != nil {
				return nil, fmt.Errorf("failed to get database for row key generation: %w", err)
			}

			// Create schema provider and get table schema
			schemaProvider := protocol.NewSchemaProvider(sqlDB)
			schema, err := schemaProvider.GetTableSchema(stmt.TableName)
			if err != nil {
				return nil, fmt.Errorf("failed to get schema for table %s: %w", stmt.TableName, err)
			}

			// Generate row key from CDC values
			// For INSERT/UPDATE: use NewValues
			// For DELETE: use OldValues
			var rowKey string
			if len(stmt.NewValues) > 0 {
				rowKey, err = protocol.GenerateRowKey(schema, stmt.NewValues)
			} else {
				rowKey, err = protocol.GenerateRowKey(schema, stmt.OldValues)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to generate row key for %s.%s: %w", stmt.Database, stmt.TableName, err)
			}

			stmt.RowKey = rowKey
		}
	}

	// Check for consistency hint
	consistency, _ := protocol.ExtractConsistencyHint(query)

	isMutation := protocol.IsMutation(stmt)
	inTransaction := session.InTransaction()

	log.Trace().
		Int("stmt_type", int(stmt.Type)).
		Bool("is_mutation", isMutation).
		Str("table", stmt.TableName).
		Msg("Routing query")

	// If in explicit transaction, buffer mutations instead of immediate 2PC
	if inTransaction && isMutation {
		return h.bufferStatement(session, stmt)
	}

	// Normal path - immediate execution (for YCSB and auto-commit clients)
	if isMutation {
		return h.handleMutation(stmt, consistency)
	}

	return h.handleRead(stmt, consistency)
}

func (h *CoordinatorHandler) handleMutation(stmt protocol.Statement, consistency protocol.ConsistencyLevel) (*protocol.ResultSet, error) {
	txnID := h.clock.Now().WallTime
	startTS := h.clock.Now()

	// Detect DDL and handle differently
	isDDL := stmt.Type == protocol.StatementDDL ||
		stmt.Type == protocol.StatementCreateDatabase ||
		stmt.Type == protocol.StatementDropDatabase

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

	// Expand multi-row INSERTs into multiple statements
	// Each row gets its own statement with its own CDC data
	statements := h.expandMultiRowInsert(stmt)

	// For DML operations, try to execute locally with hooks to capture affected rows
	// This enables MutationGuard-based conflict detection for multi-row operations
	var pendingExec PendingExecution
	var rowsAffected int64 = 1

	isDML := stmt.Type == protocol.StatementInsert ||
		stmt.Type == protocol.StatementUpdate ||
		stmt.Type == protocol.StatementDelete ||
		stmt.Type == protocol.StatementReplace

	// cancelHookCtx is called after pendingExec.Commit/Rollback to release the context
	// CRITICAL: Do NOT call cancel() before Commit/Rollback - Go's database/sql.Tx
	// watches the context and auto-rolls back if cancelled before explicit commit
	var cancelHookCtx context.CancelFunc

	if isDML && stmt.Database != "" {
		mvccDB, err := h.dbManager.GetMVCCDatabase(stmt.Database)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			cancelHookCtx = cancel // Store for later - DO NOT call yet
			pendingExec, err = mvccDB.ExecuteLocalWithHooks(ctx, uint64(txnID), statements)

			if err != nil {
				cancel() // Only cancel on error
				log.Warn().Err(err).Msg("Failed to execute with hooks, falling back to statement-based")
				pendingExec = nil
				cancelHookCtx = nil
			} else {
				rowsAffected = pendingExec.GetTotalRowCount()
			}
		}
	}

	// Build transaction for replication
	txn := &Transaction{
		ID:                    uint64(txnID),
		NodeID:                h.nodeID,
		Statements:            statements,
		StartTS:               startTS,
		WriteConsistency:      consistency,
		Database:              stmt.Database,
		RequiredSchemaVersion: schemaVersion,
		LocalExecutionDone:    pendingExec != nil, // Skip local replication if already executed
	}

	// If we have pending execution with multiple rows, build MutationGuards
	if pendingExec != nil && rowsAffected > 1 {
		// FlushIntentLog is a no-op with SQLite-backed storage (WAL handles durability)
		// Kept for backwards compatibility
		_ = pendingExec.FlushIntentLog()
		filters := pendingExec.BuildFilters()
		rowCounts := pendingExec.GetRowCounts()

		if len(filters) > 0 {
			txn.MutationGuards = make(map[string]*MutationGuard)
			for table, filterBytes := range filters {
				txn.MutationGuards[table] = &MutationGuard{
					Filter:           filterBytes,
					ExpectedRowCount: rowCounts[table],
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := h.writeCoord.WriteTransaction(ctx, txn)

	// Handle pending execution commit/rollback based on quorum result
	if pendingExec != nil {
		if err != nil {
			// Quorum failed - rollback local execution
			if rollbackErr := pendingExec.Rollback(); rollbackErr != nil {
				log.Error().Err(rollbackErr).Msg("Failed to rollback local execution after quorum failure")
			}
			if cancelHookCtx != nil {
				cancelHookCtx()
			}
			return nil, err
		}
		// Quorum succeeded - commit local execution
		if commitErr := pendingExec.Commit(); commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit local execution after quorum success")
			// Note: Quorum already succeeded, so other nodes have the data
			// This is an inconsistency that should be handled by anti-entropy
		}
		if cancelHookCtx != nil {
			cancelHookCtx()
		}
	} else if err != nil {
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

	return &protocol.ResultSet{
		RowsAffected: rowsAffected,
	}, nil
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
		log.Warn().
			Err(err).
			Str("database", stmt.Database).
			Str("table", stmt.TableName).
			Str("sql", stmt.SQL).
			Msg("Returning error to client: read transaction failed")
		return nil, err
	}

	if !resp.Success {
		log.Warn().
			Str("error", resp.Error).
			Str("database", stmt.Database).
			Str("table", stmt.TableName).
			Str("sql", stmt.SQL).
			Msg("Returning error to client: read transaction unsuccessful")
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

func (h *CoordinatorHandler) handleSystemQuery(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	config := handlers.SystemVarConfig{
		ReadOnly:       false,
		VersionComment: "Marmot",
		ConnID:         session.ConnID,
		CurrentDB:      session.CurrentDatabase,
	}
	return handlers.HandleSystemVariableQuery(stmt, config)
}

// handleVirtualTableQuery handles queries to Marmot virtual tables (MARMOT_CLUSTER_NODES, etc.)
func (h *CoordinatorHandler) handleVirtualTableQuery(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	switch stmt.VirtualTableType {
	case protocol.VirtualTableClusterNodes:
		return h.handleClusterNodesQuery()
	default:
		return nil, fmt.Errorf("unknown virtual table type: %d", stmt.VirtualTableType)
	}
}

// handleClusterNodesQuery returns current cluster membership state
func (h *CoordinatorHandler) handleClusterNodesQuery() (*protocol.ResultSet, error) {
	// Get cluster state from node registry
	rawNodes := h.nodeRegistry.GetAll()

	// Extract fields using reflection/type assertion
	nodes := make([]*NodeState, 0, len(rawNodes))
	for _, n := range rawNodes {
		// Type assert to access fields
		// The underlying type is *grpc.NodeState but we avoid import cycle
		// So we use reflection-like field access via any
		nMap := make(map[string]interface{})

		// Use reflection to extract fields
		nVal := reflect.ValueOf(n)
		if nVal.Kind() == reflect.Ptr {
			nVal = nVal.Elem()
		}

		nMap["NodeId"] = nVal.FieldByName("NodeId").Uint()
		nMap["Address"] = nVal.FieldByName("Address").String()
		nMap["Incarnation"] = nVal.FieldByName("Incarnation").Uint()

		statusVal := nVal.FieldByName("Status")
		statusInt := int32(statusVal.Int())

		nodes = append(nodes, &NodeState{
			NodeId:      nMap["NodeId"].(uint64),
			Address:     nMap["Address"].(string),
			Status:      NodeStatus(statusInt),
			Incarnation: nMap["Incarnation"].(uint64),
		})
	}

	// Build result set
	columns := []protocol.ColumnDef{
		{Name: "node_id", Type: 0x08},     // MYSQL_TYPE_LONGLONG
		{Name: "address", Type: 0xFD},     // MYSQL_TYPE_VAR_STRING
		{Name: "status", Type: 0xFD},      // MYSQL_TYPE_VAR_STRING
		{Name: "incarnation", Type: 0x08}, // MYSQL_TYPE_LONGLONG
	}

	rows := make([][]interface{}, 0, len(nodes))
	for _, node := range nodes {
		row := []interface{}{
			node.NodeId,
			node.Address,
			node.Status.String(),
			node.Incarnation,
		}
		rows = append(rows, row)
	}

	return &protocol.ResultSet{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// handleTransactionControl handles BEGIN, COMMIT, and ROLLBACK statements
func (h *CoordinatorHandler) handleTransactionControl(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	switch stmt.Type {
	case protocol.StatementBegin:
		return h.handleBegin(session)
	case protocol.StatementCommit:
		return h.handleCommit(session)
	case protocol.StatementRollback:
		return h.handleRollback(session)
	default:
		// Savepoint and other transaction control - just return OK for now
		log.Debug().
			Int("stmt_type", int(stmt.Type)).
			Msg("Unsupported transaction control statement, returning OK")
		return nil, nil
	}
}

// handleBegin starts a new explicit transaction
func (h *CoordinatorHandler) handleBegin(session *protocol.ConnectionSession) (*protocol.ResultSet, error) {
	if session.InTransaction() {
		// MySQL allows nested BEGIN but it implicitly commits the previous transaction
		// For simplicity, we'll just ignore nested BEGIN (like MySQL with autocommit)
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Msg("BEGIN while already in transaction - ignoring")
		return nil, nil
	}

	txnID := uint64(h.clock.Now().WallTime)
	startTS := h.clock.Now()

	session.BeginTransaction(txnID, startTS, session.CurrentDatabase)

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Uint64("txn_id", txnID).
		Str("database", session.CurrentDatabase).
		Msg("BEGIN: Started explicit transaction")

	return nil, nil // OK response
}

// handleCommit commits the accumulated statements via 2PC
func (h *CoordinatorHandler) handleCommit(session *protocol.ConnectionSession) (*protocol.ResultSet, error) {
	if !session.InTransaction() {
		// No active transaction - just return OK (MySQL behavior)
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Msg("COMMIT without active transaction - ignoring")
		return nil, nil
	}

	txnState := session.GetTransaction()
	if txnState == nil || len(txnState.Statements) == 0 {
		// Empty transaction - just clear and return OK
		session.EndTransaction()
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Msg("COMMIT: Empty transaction")
		return nil, nil
	}

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Uint64("txn_id", txnState.TxnID).
		Int("stmt_count", len(txnState.Statements)).
		Msg("COMMIT: Executing batched transaction via 2PC")

	// Create 2PC transaction with ALL accumulated statements
	txn := &Transaction{
		ID:                    txnState.TxnID,
		NodeID:                h.nodeID,
		Statements:            txnState.Statements,
		StartTS:               txnState.StartTS,
		WriteConsistency:      protocol.ConsistencyQuorum, // Default to quorum for explicit transactions
		Database:              txnState.Database,
		RequiredSchemaVersion: 0, // Will be set by handleMutation if needed
	}

	// Get schema version if schema manager is available
	if h.schemaVersionMgr != nil {
		schemaVersion, err := h.schemaVersionMgr.GetSchemaVersion(txnState.Database)
		if err != nil {
			log.Warn().Err(err).Str("database", txnState.Database).Msg("Failed to get schema version for transaction")
		} else {
			txn.RequiredSchemaVersion = schemaVersion
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Longer timeout for batched transactions
	defer cancel()

	err := h.writeCoord.WriteTransaction(ctx, txn)

	// Clear transaction state regardless of outcome
	session.EndTransaction()

	if err != nil {
		log.Error().
			Err(err).
			Uint64("conn_id", session.ConnID).
			Uint64("txn_id", txnState.TxnID).
			Int("stmt_count", len(txnState.Statements)).
			Msg("COMMIT: 2PC failed")
		return nil, err
	}

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Uint64("txn_id", txnState.TxnID).
		Int("stmt_count", len(txnState.Statements)).
		Msg("COMMIT: Transaction committed successfully")

	return &protocol.ResultSet{
		RowsAffected: int64(len(txnState.Statements)),
	}, nil
}

// handleRollback discards the accumulated statements
func (h *CoordinatorHandler) handleRollback(session *protocol.ConnectionSession) (*protocol.ResultSet, error) {
	if !session.InTransaction() {
		// No active transaction - just return OK
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Msg("ROLLBACK without active transaction - ignoring")
		return nil, nil
	}

	txnState := session.GetTransaction()
	stmtCount := 0
	if txnState != nil {
		stmtCount = len(txnState.Statements)
	}

	// Just discard the buffer - no network activity needed
	session.EndTransaction()

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Int("discarded_stmts", stmtCount).
		Msg("ROLLBACK: Discarded buffered statements")

	return nil, nil // OK response
}

// expandMultiRowInsert expands a multi-row INSERT statement into multiple single-row statements
// Each row gets its own statement with its own CDC data for proper replication
// For non-INSERT statements or single-row INSERTs, returns the original statement
func (h *CoordinatorHandler) expandMultiRowInsert(stmt protocol.Statement) []protocol.Statement {
	// Only expand for INSERT/REPLACE statements with multiple CDC rows
	if (stmt.Type != protocol.StatementInsert && stmt.Type != protocol.StatementReplace) ||
		len(stmt.CDCRows) <= 1 {
		// Single row or non-INSERT - return as-is
		return []protocol.Statement{stmt}
	}

	// Get schema for row key generation
	var schemaProvider *protocol.SchemaProvider
	if sqlDB, err := h.dbManager.GetDatabaseConnection(stmt.Database); err == nil {
		schemaProvider = protocol.NewSchemaProvider(sqlDB)
	}

	var schema *protocol.TableSchema
	if schemaProvider != nil {
		schema, _ = schemaProvider.GetTableSchema(stmt.TableName)
	}

	// Expand into multiple statements
	statements := make([]protocol.Statement, len(stmt.CDCRows))
	for i, cdcRow := range stmt.CDCRows {
		// Create a new statement for each row with its own CDC data
		expandedStmt := protocol.Statement{
			SQL:              stmt.SQL, // Same SQL (will be ignored on replicas, CDC data used instead)
			Type:             stmt.Type,
			TableName:        stmt.TableName,
			Database:         stmt.Database,
			Error:            stmt.Error,
			OldValues:        cdcRow.OldValues,
			NewValues:        cdcRow.NewValues,
			CDCRows:          nil, // Single row doesn't need CDCRows
			ISFilter:         stmt.ISFilter,
			ISTableType:      stmt.ISTableType,
			VirtualTableType: stmt.VirtualTableType,
			SystemVarNames:   stmt.SystemVarNames,
		}

		// Generate row key for this statement
		if schema != nil && len(cdcRow.NewValues) > 0 {
			if rowKey, err := protocol.GenerateRowKey(schema, cdcRow.NewValues); err == nil {
				expandedStmt.RowKey = rowKey
			}
		}

		statements[i] = expandedStmt
	}

	log.Debug().
		Int("original_rows", len(stmt.CDCRows)).
		Int("expanded_stmts", len(statements)).
		Str("table", stmt.TableName).
		Msg("Expanded multi-row INSERT into multiple statements")

	return statements
}

// bufferStatement adds a mutation to the active transaction buffer
func (h *CoordinatorHandler) bufferStatement(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	session.AddStatement(stmt)

	txnState := session.GetTransaction()
	stmtCount := 0
	if txnState != nil {
		stmtCount = len(txnState.Statements)
	}

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Str("table", stmt.TableName).
		Int("stmt_type", int(stmt.Type)).
		Int("buffered_count", stmtCount).
		Msg("Buffered statement in transaction")

	// Return OK immediately (optimistic - actual execution on COMMIT)
	return &protocol.ResultSet{
		RowsAffected: 1,
	}, nil
}
