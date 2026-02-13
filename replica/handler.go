package replica

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/handlers"
	"github.com/maxpert/marmot/protocol/query/transform"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReadOnlyHandler implements protocol.ConnectionHandler for read-only replicas
// It rejects all mutations and executes reads locally
type ReadOnlyHandler struct {
	dbManager      *db.DatabaseManager
	clock          *hlc.Clock
	replica        *Replica
	metadata       *handlers.MetadataHandler
	forwardWrites  bool
	forwardTimeout time.Duration
}

// NewReadOnlyHandler creates a new read-only handler
func NewReadOnlyHandler(dbManager *db.DatabaseManager, clock *hlc.Clock, replica *Replica) *ReadOnlyHandler {
	forwardTimeout := 30 * time.Second
	if cfg.Config.Replica.ForwardWriteTimeoutSec > 0 {
		forwardTimeout = time.Duration(cfg.Config.Replica.ForwardWriteTimeoutSec) * time.Second
	}
	return &ReadOnlyHandler{
		dbManager:      dbManager,
		clock:          clock,
		replica:        replica,
		metadata:       handlers.NewMetadataHandler(dbManager, db.SystemDatabaseName),
		forwardWrites:  cfg.Config.Replica.ForwardWrites,
		forwardTimeout: forwardTimeout,
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
	schemaProvider := func(database, table string) *transform.SchemaInfo {
		if table == "" || h.dbManager == nil {
			return nil
		}
		dbName := database
		if dbName == "" {
			dbName = session.CurrentDatabase
		}
		if dbName == "" {
			return nil
		}

		schemaInfo, err := h.dbManager.GetTranspilerSchema(dbName, table)
		if err != nil {
			return nil
		}
		return schemaInfo
	}

	stmt := protocol.ParseStatementWithOptions(sql, protocol.ParseOptions{
		SkipTranspilation: !session.TranspilationEnabled,
		SchemaProvider:    schemaProvider,
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
		if h.forwardWrites {
			return h.forwardMutation(session, stmt, params)
		}
		log.Debug().
			Uint64("conn_id", session.ConnID).
			Int("stmt_type", int(stmt.Type)).
			Msg("Rejecting mutation on read-only replica")
		return nil, protocol.ErrReadOnly()
	}

	// Reject transaction control for writes (BEGIN is ok for reads, but COMMIT on writes is not)
	// Allow read-only transactions for compatibility
	if protocol.IsTransactionControl(stmt) {
		if h.forwardWrites {
			return h.forwardTxnControl(session, stmt)
		}
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
		return h.metadata.HandleShowTables(dbName, stmt.ShowFilter)
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

// HandleLoadData forwards LOAD DATA LOCAL INFILE as a single operation to the leader.
func (h *ReadOnlyHandler) HandleLoadData(session *protocol.ConnectionSession, sql string, data []byte) (*protocol.ResultSet, error) {
	stmt := protocol.ParseStatement(sql)
	if stmt.Type != protocol.StatementLoadData {
		return nil, fmt.Errorf("ERROR 1105 (HY000): statement is not LOAD DATA")
	}
	if !h.forwardWrites {
		return nil, protocol.ErrReadOnly()
	}

	client := h.replica.streamClient.GetClient()
	if client == nil {
		return nil, fmt.Errorf("ERROR 2003 (HY000): Not connected to leader")
	}

	requestID := session.NextForwardRequestID()
	resp, err := h.forwardLoadDataWithRetry(client, &marmotgrpc.ForwardLoadDataRequest{
		ReplicaNodeId:      h.replica.streamClient.GetNodeID(),
		SessionId:          session.ConnID,
		RequestId:          requestID,
		Database:           session.CurrentDatabase,
		Sql:                sql,
		Data:               data,
		WaitForReplication: session.WaitForReplication,
		TimeoutMs:          uint32(h.forwardTimeout.Milliseconds()),
	})
	if err != nil {
		return nil, fmt.Errorf("ERROR 2013 (HY000): Lost connection to leader during query: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("ERROR 1105 (HY000): %s", resp.ErrorMessage)
	}

	session.LastInsertId.Store(resp.LastInsertId)
	session.ForwardedTxnActive = resp.InTransaction

	if session.WaitForReplication && resp.CommittedTxnId > 0 {
		if err := h.waitForReplication(session.CurrentDatabase, resp.CommittedTxnId); err != nil {
			log.Warn().Err(err).Uint64("txn_id", resp.CommittedTxnId).Msg("Timeout waiting for replication")
		}
	}

	return &protocol.ResultSet{
		RowsAffected:   resp.RowsAffected,
		LastInsertId:   resp.LastInsertId,
		CommittedTxnId: resp.CommittedTxnId,
	}, nil
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
		switch varType {
		case "transpilation":
			session.TranspilationEnabled = (value == "ON")
			log.Debug().
				Uint64("conn_id", session.ConnID).
				Bool("transpilation", session.TranspilationEnabled).
				Msg("Session transpilation updated")
			return nil, nil
		case "wait_for_replication":
			session.WaitForReplication = (value == "ON")
			log.Debug().
				Uint64("conn_id", session.ConnID).
				Bool("wait_for_replication", session.WaitForReplication).
				Msg("Session wait_for_replication updated")
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

// forwardMutation forwards a mutation to the leader
func (h *ReadOnlyHandler) forwardMutation(session *protocol.ConnectionSession, stmt protocol.Statement, params []interface{}) (*protocol.ResultSet, error) {
	client := h.replica.streamClient.GetClient()
	if client == nil {
		return nil, fmt.Errorf("ERROR 2003 (HY000): Not connected to leader")
	}

	// Serialize params using msgpack
	serializedParams, err := marmotgrpc.SerializeParams(params)
	if err != nil {
		return nil, fmt.Errorf("ERROR 1105 (HY000): Failed to serialize params: %v", err)
	}

	requestID := session.NextForwardRequestID()
	resp, err := h.forwardQueryWithRetry(client, &marmotgrpc.ForwardQueryRequest{
		ReplicaNodeId:      h.replica.streamClient.GetNodeID(),
		SessionId:          session.ConnID,
		RequestId:          requestID,
		Database:           session.CurrentDatabase,
		Sql:                stmt.SQL,
		TxnControl:         marmotgrpc.ForwardTxnControl_FWD_TXN_NONE,
		WaitForReplication: session.WaitForReplication,
		Params:             serializedParams,
		TimeoutMs:          uint32(h.forwardTimeout.Milliseconds()),
	})

	if err != nil {
		return nil, fmt.Errorf("ERROR 2013 (HY000): Lost connection to leader during query: %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("ERROR 1105 (HY000): %s", resp.ErrorMessage)
	}

	// Update session state
	session.LastInsertId.Store(resp.LastInsertId)

	// Track forwarded transaction state if within a transaction
	session.ForwardedTxnActive = resp.InTransaction

	// Handle database operations locally after leader confirms success
	// These don't replicate via change stream - replica must create/drop locally
	if stmt.Type == protocol.StatementCreateDatabase && stmt.Database != "" {
		if err := h.dbManager.CreateDatabase(stmt.Database); err != nil {
			log.Warn().Err(err).Str("database", stmt.Database).Msg("Failed to create database locally after forward")
			// Don't fail - leader succeeded, local create might fail if already exists
		} else {
			log.Info().Str("database", stmt.Database).Msg("Created database locally after forward to leader")
		}
	} else if stmt.Type == protocol.StatementDropDatabase && stmt.Database != "" {
		if err := h.dbManager.DropDatabase(stmt.Database); err != nil {
			log.Warn().Err(err).Str("database", stmt.Database).Msg("Failed to drop database locally after forward")
			// Don't fail - leader succeeded
		} else {
			log.Info().Str("database", stmt.Database).Msg("Dropped database locally after forward to leader")
		}
	}

	// Wait for replication if requested OR if this is a DDL statement
	// DDL always waits for replication to ensure schema is visible for subsequent reads
	shouldWait := (session.WaitForReplication || protocol.IsDDL(stmt)) && resp.CommittedTxnId > 0
	if shouldWait {
		if err := h.waitForReplication(session.CurrentDatabase, resp.CommittedTxnId); err != nil {
			log.Warn().Err(err).Uint64("txn_id", resp.CommittedTxnId).Bool("is_ddl", protocol.IsDDL(stmt)).Msg("Timeout waiting for replication")
			// Don't return error - write succeeded, just visibility delay
		}
	}

	return &protocol.ResultSet{
		RowsAffected: resp.RowsAffected,
		LastInsertId: resp.LastInsertId,
	}, nil
}

// forwardTxnControl forwards transaction control to the leader
func (h *ReadOnlyHandler) forwardTxnControl(session *protocol.ConnectionSession, stmt protocol.Statement) (*protocol.ResultSet, error) {
	client := h.replica.streamClient.GetClient()
	if client == nil {
		return nil, fmt.Errorf("ERROR 2003 (HY000): Not connected to leader")
	}

	var txnControl marmotgrpc.ForwardTxnControl
	switch stmt.Type {
	case protocol.StatementBegin:
		txnControl = marmotgrpc.ForwardTxnControl_FWD_TXN_BEGIN
	case protocol.StatementCommit:
		txnControl = marmotgrpc.ForwardTxnControl_FWD_TXN_COMMIT
	case protocol.StatementRollback:
		txnControl = marmotgrpc.ForwardTxnControl_FWD_TXN_ROLLBACK
	default:
		return nil, protocol.ErrReadOnly()
	}

	requestID := session.NextForwardRequestID()
	resp, err := h.forwardQueryWithRetry(client, &marmotgrpc.ForwardQueryRequest{
		ReplicaNodeId:      h.replica.streamClient.GetNodeID(),
		SessionId:          session.ConnID,
		RequestId:          requestID,
		Database:           session.CurrentDatabase,
		TxnControl:         txnControl,
		WaitForReplication: session.WaitForReplication,
		TimeoutMs:          uint32(h.forwardTimeout.Milliseconds()),
	})

	if err != nil {
		return nil, fmt.Errorf("ERROR 2013 (HY000): %v", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("ERROR 1105 (HY000): %s", resp.ErrorMessage)
	}

	// Track forwarded transaction state based on response
	session.ForwardedTxnActive = resp.InTransaction

	// Clear state on COMMIT/ROLLBACK success
	if txnControl == marmotgrpc.ForwardTxnControl_FWD_TXN_COMMIT ||
		txnControl == marmotgrpc.ForwardTxnControl_FWD_TXN_ROLLBACK {
		session.ForwardedTxnActive = false
	}

	// Optional read-your-own-writes guarantee for forwarded explicit COMMIT.
	if txnControl == marmotgrpc.ForwardTxnControl_FWD_TXN_COMMIT &&
		session.WaitForReplication && resp.CommittedTxnId > 0 {
		if err := h.waitForReplication(session.CurrentDatabase, resp.CommittedTxnId); err != nil {
			log.Warn().
				Err(err).
				Uint64("conn_id", session.ConnID).
				Uint64("txn_id", resp.CommittedTxnId).
				Msg("Timeout waiting for forwarded COMMIT replication")
		}
	}

	return nil, nil
}

func (h *ReadOnlyHandler) forwardQueryWithRetry(
	client marmotgrpc.MarmotServiceClient,
	req *marmotgrpc.ForwardQueryRequest,
) (*marmotgrpc.ForwardQueryResponse, error) {
	resp, err := h.forwardQueryOnce(client, req)
	if err == nil {
		return resp, nil
	}

	if !isRetriableForwardError(err) {
		return nil, err
	}

	log.Warn().
		Err(err).
		Uint64("session_id", req.SessionId).
		Uint64("request_id", req.RequestId).
		Msg("Forward query failed with retriable error, retrying once with same request_id")

	return h.forwardQueryOnce(client, req)
}

func (h *ReadOnlyHandler) forwardLoadDataWithRetry(
	client marmotgrpc.MarmotServiceClient,
	req *marmotgrpc.ForwardLoadDataRequest,
) (*marmotgrpc.ForwardQueryResponse, error) {
	resp, err := h.forwardLoadDataOnce(client, req)
	if err == nil {
		return resp, nil
	}

	if !isRetriableForwardError(err) {
		return nil, err
	}

	log.Warn().
		Err(err).
		Uint64("session_id", req.SessionId).
		Uint64("request_id", req.RequestId).
		Msg("Forward load data failed with retriable error, retrying once with same request_id")

	return h.forwardLoadDataOnce(client, req)
}

func (h *ReadOnlyHandler) forwardQueryOnce(
	client marmotgrpc.MarmotServiceClient,
	req *marmotgrpc.ForwardQueryRequest,
) (*marmotgrpc.ForwardQueryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), h.forwardTimeout)
	defer cancel()
	return client.ForwardQuery(ctx, req)
}

func (h *ReadOnlyHandler) forwardLoadDataOnce(
	client marmotgrpc.MarmotServiceClient,
	req *marmotgrpc.ForwardLoadDataRequest,
) (*marmotgrpc.ForwardQueryResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), h.forwardTimeout)
	defer cancel()
	return client.ForwardLoadData(ctx, req)
}

func isRetriableForwardError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.DeadlineExceeded, codes.Unavailable, codes.Canceled, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// waitForReplication waits for the specified transaction to be replicated
func (h *ReadOnlyHandler) waitForReplication(database string, txnID uint64) error {
	if txnID == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.forwardTimeout)
	defer cancel()

	return h.replica.streamClient.WaitForTxn(ctx, database, txnID)
}
