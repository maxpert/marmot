package coordinator

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/handlers"
	"github.com/maxpert/marmot/telemetry"
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

// CDCEntry holds CDC data captured by preupdate hooks
type CDCEntry struct {
	RowKey    string
	OldValues map[string][]byte
	NewValues map[string][]byte
}

// PendingExecution represents a locally executed transaction waiting for quorum
type PendingExecution interface {
	GetRowCounts() map[string]int64
	GetTotalRowCount() int64
	GetKeyHashes(maxRows int) map[string][]uint64
	GetCDCEntries() []CDCEntry
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

// getWriteTimeout returns the configured write timeout or the default (300s)
func getWriteTimeout() time.Duration {
	if cfg.Config != nil && cfg.Config.Replication.WriteTimeoutMS > 0 {
		return time.Duration(cfg.Config.Replication.WriteTimeoutMS) * time.Millisecond
	}
	return 300 * time.Second
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
	recentTxnIDs     sync.Map // txn_id -> conn_id for duplicate detection
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

	// Build schema lookup function for auto-increment ID injection
	var schemaLookup protocol.SchemaLookupFunc
	if session.CurrentDatabase != "" {
		db, err := h.dbManager.GetDatabaseConnection(session.CurrentDatabase)
		if err == nil && db != nil {
			sp := protocol.NewSchemaProvider(db)
			schemaLookup = func(table string) string {
				col, _ := sp.GetAutoIncrementColumn(table)
				return col
			}
		}
	}

	// Parse first - all routing decisions based on parsed Statement
	stmt := protocol.ParseStatementWithSchema(query, schemaLookup)

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

	// Check for consistency hint
	consistency, found := protocol.ExtractConsistencyHint(query)

	isMutation := protocol.IsMutation(stmt)
	inTransaction := session.InTransaction()

	// Use configured default if no hint specified (different defaults for reads vs writes)
	if !found {
		if isMutation {
			consistency, _ = protocol.ParseConsistencyLevel(cfg.Config.Replication.DefaultWriteConsist)
		} else {
			consistency, _ = protocol.ParseConsistencyLevel(cfg.Config.Replication.DefaultReadConsist)
		}
	}

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
	queryStart := time.Now()

	// Generate txnID using Percolator/TiDB pattern: (physical_ms << 18) | logical
	// This guarantees uniqueness by keeping physical and logical in separate bit ranges
	startTS := h.clock.Now()
	txnID := startTS.ToTxnID()

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
			telemetry.QueriesTotal.With("ddl", "failed").Inc()
			telemetry.QueryDurationSeconds.With("ddl").Observe(time.Since(queryStart).Seconds())
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

	// Single statement array for now - hooks will populate CDC data
	statements := []protocol.Statement{stmt}

	// For DML operations, try to execute locally with hooks to capture affected rows
	// This captures CDC data for intent-based conflict detection via write intents
	var pendingExec PendingExecution
	var rowsAffected int64 = 1

	// cancelHookCtx is called after pendingExec.Commit/Rollback to release the context
	// CRITICAL: Do NOT call cancel() before Commit/Rollback - Go's database/sql.Tx
	// watches the context and auto-rolls back if cancelled before explicit commit
	var cancelHookCtx context.CancelFunc

	if protocol.IsDML(stmt) && stmt.Database != "" {
		mvccDB, err := h.dbManager.GetMVCCDatabase(stmt.Database)
		if err == nil {
			// Use configured lock wait timeout for hook execution (default 50s like MySQL innodb_lock_wait_timeout)
			hookTimeout := 50 * time.Second
			if cfg.Config != nil && cfg.Config.MVCC.LockWaitTimeoutSeconds > 0 {
				hookTimeout = time.Duration(cfg.Config.MVCC.LockWaitTimeoutSeconds) * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), hookTimeout)
			cancelHookCtx = cancel // Store for later - DO NOT call yet
			pendingExec, err = mvccDB.ExecuteLocalWithHooks(ctx, uint64(txnID), statements)

			if err != nil {
				cancel() // Only cancel on error
				// DML requires CDC hooks for idempotent replication (INSERT OR REPLACE)
				// Statement-based fallback uses raw SQL which fails on duplicate keys
				// Return error to client so they can retry
				log.Warn().Err(err).Msg("DML execution with CDC hooks failed - client should retry")
				telemetry.QueriesTotal.With("dml", "failed").Inc()
				telemetry.QueryDurationSeconds.With("dml").Observe(time.Since(queryStart).Seconds())
				return nil, fmt.Errorf("DML execution failed: %w", err)
			}
			rowsAffected = pendingExec.GetTotalRowCount()

			// Transfer hook-captured CDC data to statements for replication
			// Hooks capture actual row data; AST parsing can't handle parameterized queries
			cdcEntries := pendingExec.GetCDCEntries()
			if len(statements) == 1 && len(cdcEntries) > 0 {
				// For UPSERT (INSERT OR REPLACE) on existing rows, SQLite fires 2 hooks:
				// - DELETE (OldValues filled, NewValues empty)
				// - INSERT (OldValues empty, NewValues filled)
				// Merge all entries to get both OldValues and NewValues
				mergedOldValues := make(map[string][]byte)
				mergedNewValues := make(map[string][]byte)
				var rowKey string

				for _, e := range cdcEntries {
					if rowKey == "" {
						rowKey = e.RowKey
					}
					for k, v := range e.OldValues {
						mergedOldValues[k] = v
					}
					for k, v := range e.NewValues {
						mergedNewValues[k] = v
					}
				}

				statements[0].RowKey = rowKey
				statements[0].OldValues = mergedOldValues
				statements[0].NewValues = mergedNewValues
			}
		}
	}

	// Build transaction for replication
	// LocalExecutionDone is always false - coordinator commits via CDC replay like remotes
	// This is the new unified commit path that avoids hookDB/writeDB deadlock
	txn := &Transaction{
		ID:                    uint64(txnID),
		NodeID:                h.nodeID,
		Statements:            statements,
		StartTS:               startTS,
		WriteConsistency:      consistency,
		Database:              stmt.Database,
		RequiredSchemaVersion: schemaVersion,
		LocalExecutionDone:    false, // Coordinator commits via CDC replay, same as remotes
	}

	ctx, cancel := context.WithTimeout(context.Background(), getWriteTimeout())
	defer cancel()

	// Cancel hook context when done (safe even if nil)
	defer func() {
		if cancelHookCtx != nil {
			cancelHookCtx()
		}
	}()

	err := h.writeCoord.WriteTransaction(ctx, txn)
	if err != nil {
		queryType := "dml"
		if isDDL {
			queryType = "ddl"
		}
		telemetry.QueriesTotal.With(queryType, "failed").Inc()
		telemetry.QueryDurationSeconds.With(queryType).Observe(time.Since(queryStart).Seconds())
		return nil, err
	}
	// Success - coordinator committed via CDC replay in WriteTransaction
	// No pendingExec.Commit() needed - hookDB was already rolled back

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

	// Record success metrics
	queryType := "dml"
	if isDDL {
		queryType = "ddl"
	}
	telemetry.QueriesTotal.With(queryType, "success").Inc()
	telemetry.QueryDurationSeconds.With(queryType).Observe(time.Since(queryStart).Seconds())
	telemetry.RowsAffected.Observe(float64(rowsAffected))

	return &protocol.ResultSet{
		RowsAffected: rowsAffected,
	}, nil
}

func (h *CoordinatorHandler) handleRead(stmt protocol.Statement, consistency protocol.ConsistencyLevel) (*protocol.ResultSet, error) {
	queryStart := time.Now()

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
		telemetry.QueriesTotal.With("select", "failed").Inc()
		telemetry.QueryDurationSeconds.With("select").Observe(time.Since(queryStart).Seconds())
		return nil, err
	}

	if !resp.Success {
		log.Warn().
			Str("error", resp.Error).
			Str("database", stmt.Database).
			Str("table", stmt.TableName).
			Str("sql", stmt.SQL).
			Msg("Returning error to client: read transaction unsuccessful")
		telemetry.QueriesTotal.With("select", "failed").Inc()
		telemetry.QueryDurationSeconds.With("select").Observe(time.Since(queryStart).Seconds())
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
			// Fallback: Infer columns from first row (sorted for consistent order)
			firstRow := resp.Rows[0]
			colNames := make([]string, 0, len(firstRow))
			for colName := range firstRow {
				colNames = append(colNames, colName)
			}
			sort.Strings(colNames)
			for _, colName := range colNames {
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

	// Record success metrics
	telemetry.QueriesTotal.With("select", "success").Inc()
	telemetry.QueryDurationSeconds.With("select").Observe(time.Since(queryStart).Seconds())
	telemetry.RowsReturned.Observe(float64(len(rs.Rows)))

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

	// Generate txnID using Percolator/TiDB pattern: (physical_ms << 18) | logical
	// This guarantees uniqueness by keeping physical and logical in separate bit ranges
	// Retry up to 3 times if we somehow generate a duplicate (safety net)
	var txnID uint64
	var startTS hlc.Timestamp
	for attempt := 0; attempt < 3; attempt++ {
		startTS = h.clock.Now()
		txnID = startTS.ToTxnID()

		// Check for duplicate (this should never happen with proper HLC)
		if existing, loaded := h.recentTxnIDs.LoadOrStore(txnID, session.ConnID); loaded {
			log.Error().
				Uint64("conn_id", session.ConnID).
				Uint64("txn_id", txnID).
				Uint64("existing_conn_id", existing.(uint64)).
				Int("attempt", attempt).
				Int64("wall_time", startTS.WallTime).
				Int32("logical", startTS.Logical).
				Msg("CRITICAL: Duplicate txn_id detected in handleBegin!")
			continue // retry with new timestamp
		}
		break // unique txnID
	}

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

	// Execute DML statements with hooks to capture CDC data
	// This is required for 2PC replication - without CDC data, intents cannot be created
	enrichedStatements := make([]protocol.Statement, 0, len(txnState.Statements))
	var totalRowsAffected int64

	hookTimeout := 50 * time.Second
	if cfg.Config != nil && cfg.Config.MVCC.LockWaitTimeoutSeconds > 0 {
		hookTimeout = time.Duration(cfg.Config.MVCC.LockWaitTimeoutSeconds) * time.Second
	}

	for _, stmt := range txnState.Statements {
		if protocol.IsDML(stmt) && stmt.Database != "" {
			mvccDB, err := h.dbManager.GetMVCCDatabase(stmt.Database)
			if err != nil {
				session.EndTransaction()
				h.recentTxnIDs.Delete(txnState.TxnID)
				return nil, fmt.Errorf("failed to get database %s: %w", stmt.Database, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), hookTimeout)
			pendingExec, err := mvccDB.ExecuteLocalWithHooks(ctx, txnState.TxnID, []protocol.Statement{stmt})
			if err != nil {
				cancel()
				session.EndTransaction()
				h.recentTxnIDs.Delete(txnState.TxnID)
				return nil, fmt.Errorf("DML execution failed: %w", err)
			}
			cancel()

			totalRowsAffected += pendingExec.GetTotalRowCount()

			// Extract CDC data and merge into statement
			cdcEntries := pendingExec.GetCDCEntries()
			if len(cdcEntries) > 0 {
				// For UPSERT (INSERT OR REPLACE) on existing rows, SQLite fires 2 hooks:
				// - DELETE (OldValues filled, NewValues empty)
				// - INSERT (OldValues empty, NewValues filled)
				// Merge all entries to get both OldValues and NewValues
				mergedOldValues := make(map[string][]byte)
				mergedNewValues := make(map[string][]byte)
				var rowKey string

				for _, e := range cdcEntries {
					if rowKey == "" {
						rowKey = e.RowKey
					}
					for k, v := range e.OldValues {
						mergedOldValues[k] = v
					}
					for k, v := range e.NewValues {
						mergedNewValues[k] = v
					}
				}

				stmt.RowKey = rowKey
				stmt.OldValues = mergedOldValues
				stmt.NewValues = mergedNewValues
			}
		}
		enrichedStatements = append(enrichedStatements, stmt)
	}

	// Create 2PC transaction with CDC-enriched statements
	txn := &Transaction{
		ID:                    txnState.TxnID,
		NodeID:                h.nodeID,
		Statements:            enrichedStatements,
		StartTS:               txnState.StartTS,
		WriteConsistency:      protocol.ConsistencyQuorum, // Default to quorum for explicit transactions
		Database:              txnState.Database,
		RequiredSchemaVersion: 0,
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

	ctx, cancel := context.WithTimeout(context.Background(), getWriteTimeout())
	defer cancel()

	err := h.writeCoord.WriteTransaction(ctx, txn)

	// Clear transaction state regardless of outcome
	session.EndTransaction()
	// Cleanup from recentTxnIDs to prevent memory growth
	h.recentTxnIDs.Delete(txnState.TxnID)

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
		Int("stmt_count", len(enrichedStatements)).
		Int64("rows_affected", totalRowsAffected).
		Msg("COMMIT: Transaction committed successfully")

	return &protocol.ResultSet{
		RowsAffected: totalRowsAffected,
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
	var txnID uint64
	if txnState != nil {
		stmtCount = len(txnState.Statements)
		txnID = txnState.TxnID
	}

	// Just discard the buffer - no network activity needed
	session.EndTransaction()
	// Cleanup from recentTxnIDs to prevent memory growth
	if txnID != 0 {
		h.recentTxnIDs.Delete(txnID)
	}

	log.Debug().
		Uint64("conn_id", session.ConnID).
		Int("discarded_stmts", stmtCount).
		Msg("ROLLBACK: Discarded buffered statements")

	return nil, nil // OK response
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
