package grpc

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/encoding"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/telemetry"
	"github.com/rs/zerolog/log"
)

// ReplicationHandler handles transaction replication with MVCC
type ReplicationHandler struct {
	nodeID           uint64
	dbMgr            *db.DatabaseManager
	clock            *hlc.Clock
	schemaVersionMgr *db.SchemaVersionManager
	engine           *db.ReplicationEngine
}

// NewReplicationHandler creates a new replication handler
func NewReplicationHandler(nodeID uint64, dbMgr *db.DatabaseManager, clock *hlc.Clock, schemaVersionMgr *db.SchemaVersionManager) *ReplicationHandler {
	return &ReplicationHandler{
		nodeID:           nodeID,
		dbMgr:            dbMgr,
		clock:            clock,
		schemaVersionMgr: schemaVersionMgr,
		engine:           db.NewReplicationEngine(nodeID, dbMgr, clock),
	}
}

// HandleReplicateTransaction handles incoming transaction replication requests.
// Entry point for all remote 2PC phases (PREPARE, COMMIT, ABORT).
func (rh *ReplicationHandler) HandleReplicateTransaction(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Update local clock with incoming timestamp
	incomingTS := hlc.Timestamp{
		WallTime: req.Timestamp.WallTime,
		Logical:  req.Timestamp.Logical,
		NodeID:   req.Timestamp.NodeId,
	}
	rh.clock.Update(incomingTS)

	switch req.Phase {
	case TransactionPhase_PREPARE:
		return rh.handlePrepare(ctx, req)
	case TransactionPhase_COMMIT:
		return rh.handleCommit(ctx, req)
	case TransactionPhase_ABORT:
		return rh.handleAbort(ctx, req)
	case TransactionPhase_REPLAY:
		return rh.handleReplay(ctx, req)
	default:
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("unknown transaction phase: %v", req.Phase),
		}, nil
	}
}

// handlePrepare processes Phase 1 of 2PC: Create write intents
func (rh *ReplicationHandler) handlePrepare(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Schema version validation - MUST happen before engine call
	if rh.schemaVersionMgr != nil && req.RequiredSchemaVersion > 0 {
		dbName := req.Database
		if dbName != "" && dbName != db.SystemDatabaseName {
			localVersion, err := rh.schemaVersionMgr.GetSchemaVersion(dbName)
			if err != nil {
				log.Warn().Err(err).Str("database", dbName).Msg("Failed to get local schema version during prepare")
			} else if localVersion < req.RequiredSchemaVersion {
				log.Error().
					Str("database", dbName).
					Uint64("local_version", localVersion).
					Uint64("required_version", req.RequiredSchemaVersion).
					Uint64("txn_id", req.TxnId).
					Msg("Schema version mismatch: local version is behind required version")
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("schema version mismatch: local version %d < required version %d", localVersion, req.RequiredSchemaVersion),
				}, nil
			}
		}
	}

	// Convert proto statements to internal format
	statements := make([]protocol.Statement, 0, len(req.Statements))
	for _, stmt := range req.Statements {
		internalStmt := protocol.Statement{
			SQL:       stmt.GetSQL(),
			Type:      common.MustFromWireType(stmt.Type),
			TableName: stmt.TableName,
			Database:  stmt.Database,
			IntentKey: stmt.GetIntentKey(),
		}
		if rowChange := stmt.GetRowChange(); rowChange != nil {
			internalStmt.OldValues = rowChange.OldValues
			internalStmt.NewValues = rowChange.NewValues
		}
		statements = append(statements, internalStmt)
	}

	// Build engine request
	startTS := hlc.Timestamp{
		WallTime: req.Timestamp.WallTime,
		Logical:  req.Timestamp.Logical,
		NodeID:   req.Timestamp.NodeId,
	}
	engineReq := &db.PrepareRequest{
		TxnID:      req.TxnId,
		NodeID:     req.SourceNodeId,
		StartTS:    startTS,
		Database:   req.Database,
		Statements: statements,
	}

	// Call engine
	result := rh.engine.Prepare(ctx, engineReq)

	// Convert to gRPC response
	resp := &TransactionResponse{
		Success:          result.Success,
		ErrorMessage:     result.Error,
		ConflictDetected: result.ConflictDetected,
		ConflictDetails:  result.ConflictDetails,
	}
	if result.Success {
		resp.AppliedAt = &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		}
		telemetry.ReplicationRequestsTotal.With("prepare", "success").Inc()
	}
	return resp, nil
}

// handleCommit processes Phase 2 of 2PC: Commit transaction
func (rh *ReplicationHandler) handleCommit(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	// Check if this transaction contains DDL statements BEFORE committing
	// (transaction is removed from memory after commit, so we can't check afterwards)
	hasDDL := false
	var ddlSQL string
	if rh.schemaVersionMgr != nil && req.Database != "" && len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			stmtType := common.MustFromWireType(stmt.Type)
			if stmtType == protocol.StatementDDL {
				hasDDL = true
				if ddl := stmt.GetDdlChange(); ddl != nil {
					ddlSQL = ddl.Sql
				}
				break
			}
		}
	}

	engineReq := &db.CommitRequest{
		TxnID:    req.TxnId,
		Database: req.Database,
	}

	result := rh.engine.Commit(ctx, engineReq)

	if !result.Success {
		telemetry.ReplicationRequestsTotal.With("commit", "failed").Inc()
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: result.Error,
		}, nil
	}

	// Schema version increment for DDL transactions (MUST happen after successful commit)
	if hasDDL && ddlSQL != "" {
		_, verErr := rh.schemaVersionMgr.IncrementSchemaVersion(req.Database, ddlSQL, req.TxnId)
		if verErr != nil {
			log.Error().Err(verErr).Str("database", req.Database).Uint64("txn_id", req.TxnId).Msg("Failed to increment schema version after DDL replication")
		}
	}

	telemetry.ReplicationRequestsTotal.With("commit", "success").Inc()
	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// handleAbort processes abort: Rollback transaction
func (rh *ReplicationHandler) handleAbort(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	engineReq := &db.AbortRequest{
		TxnID:    req.TxnId,
		Database: req.Database,
	}

	result := rh.engine.Abort(ctx, engineReq)

	return &TransactionResponse{
		Success:      result.Success,
		ErrorMessage: result.Error,
	}, nil
}

// handleReplay processes anti-entropy replay: Apply already-committed transactions directly.
// This bypasses 2PC state tracking since these transactions are already committed on the source.
// Used by delta sync to repair divergent nodes without requiring PREPARE phase.
func (rh *ReplicationHandler) handleReplay(ctx context.Context, req *TransactionRequest) (*TransactionResponse, error) {
	log.Debug().
		Uint64("node_id", rh.nodeID).
		Str("database", req.Database).
		Uint64("txn_id", req.TxnId).
		Int("num_statements", len(req.Statements)).
		Msg("handleReplay called - applying already-committed transaction")

	// Get the target database from request (database name is required)
	dbName := req.Database
	if dbName == "" {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: "database name is required in replay request",
		}, nil
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("database %s not found: %v", dbName, err),
		}, nil
	}

	sqliteDB := dbInstance.GetDB()

	// Execute statements directly in a SQLite transaction
	tx, err := sqliteDB.BeginTx(ctx, nil)
	if err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to begin transaction: %v", err),
		}, nil
	}
	defer tx.Rollback()

	for _, stmt := range req.Statements {
		// Check for CDC data (RowChange payload)
		if rowChange := stmt.GetRowChange(); rowChange != nil && (len(rowChange.NewValues) > 0 || len(rowChange.OldValues) > 0) {
			// CDC path: apply row data directly
			if err := rh.applyReplayCDCStatement(tx, dbName, stmt); err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to apply CDC statement: %v", err),
				}, nil
			}
			continue
		}

		// DDL path: execute SQL directly
		if ddl := stmt.GetDdlChange(); ddl != nil && ddl.Sql != "" {
			if _, err := tx.ExecContext(ctx, ddl.Sql); err != nil {
				return &TransactionResponse{
					Success:      false,
					ErrorMessage: fmt.Sprintf("failed to execute DDL: %v", err),
				}, nil
			}
			continue
		}

		// No CDC data and no DDL - this shouldn't happen for replay
		log.Warn().
			Str("table", stmt.TableName).
			Int32("type", int32(stmt.Type)).
			Msg("handleReplay: statement has no CDC data or DDL")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return &TransactionResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("failed to commit: %v", err),
		}, nil
	}

	// Store TransactionRecord in MetaStore so GetCommittedTxnCount/GetMaxTxnID return correct values.
	// Without this, anti-entropy keeps thinking we're behind because these metrics read from PebbleDB.
	metaStore := dbInstance.GetMetaStore()
	if metaStore != nil {
		commitTS := hlc.Timestamp{
			WallTime: req.Timestamp.WallTime,
			Logical:  req.Timestamp.Logical,
			NodeID:   req.Timestamp.NodeId,
		}

		// Convert gRPC statements to protocol.Statement and serialize for storage.
		// This is CRITICAL: if this node later becomes a delta sync source, it needs
		// to have the statement data to stream to other lagging nodes.
		serializedStatements := rh.serializeReplayStatements(req.Statements, dbName)

		if err := metaStore.StoreReplayedTransaction(req.TxnId, req.Timestamp.NodeId, commitTS, serializedStatements, dbName); err != nil {
			// Log but don't fail - data is already in SQLite
			log.Warn().
				Err(err).
				Uint64("txn_id", req.TxnId).
				Msg("handleReplay: failed to store transaction record in MetaStore")
		}
	}

	log.Debug().
		Uint64("txn_id", req.TxnId).
		Str("database", dbName).
		Int("statements", len(req.Statements)).
		Msg("handleReplay: transaction applied successfully")

	telemetry.ReplicationRequestsTotal.With("replay", "success").Inc()
	return &TransactionResponse{
		Success: true,
		AppliedAt: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}

// applyReplayCDCStatement applies a CDC statement during replay
func (rh *ReplicationHandler) applyReplayCDCStatement(tx *sql.Tx, dbName string, stmt *Statement) error {
	rowChange := stmt.GetRowChange()
	if rowChange == nil {
		return fmt.Errorf("no row change data")
	}

	switch stmt.Type {
	case pb.StatementType_INSERT, pb.StatementType_REPLACE:
		return rh.applyReplayCDCInsert(tx, stmt.TableName, rowChange.NewValues)
	case pb.StatementType_UPDATE:
		return rh.applyReplayCDCUpdate(tx, dbName, stmt.TableName, rowChange.OldValues, rowChange.NewValues)
	case pb.StatementType_DELETE:
		return rh.applyReplayCDCDelete(tx, dbName, stmt.TableName, rowChange.OldValues)
	default:
		return fmt.Errorf("unsupported statement type for CDC replay: %v", stmt.Type)
	}
}

// applyReplayCDCInsert performs INSERT OR REPLACE using CDC row data
func (rh *ReplicationHandler) applyReplayCDCInsert(tx *sql.Tx, tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	for col := range newValues {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		var value interface{}
		if err := encoding.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStmt, values...)
	return err
}

// applyReplayCDCUpdate performs UPDATE using CDC row data
// Uses oldValues for WHERE clause (to find existing row) and newValues for SET clause
func (rh *ReplicationHandler) applyReplayCDCUpdate(tx *sql.Tx, dbName, tableName string, oldValues, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to update")
	}

	// Get database instance to access cached schema
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %w", dbName, err)
	}

	// Use cached schema - does NOT query SQLite
	schema, err := dbInstance.GetCachedTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("failed to get cached schema for table %s: %w", tableName, err)
	}

	if len(schema.PrimaryKeys) == 0 {
		return fmt.Errorf("no primary key columns found for table %s", tableName)
	}

	// Build UPDATE statement with SET clause using newValues
	// Sort columns for deterministic SQL generation
	columns := make([]string, 0, len(newValues))
	for col := range newValues {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	setClauses := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues)+len(schema.PrimaryKeys))

	for _, col := range columns {
		valBytes := newValues[col]
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))

		var value interface{}
		if err := encoding.Unmarshal(valBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	// Build WHERE clause using primary key columns from oldValues
	// This is critical for PK changes: we need the OLD PK to find the row
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		// Use oldValues for WHERE clause if available, fallback to newValues
		pkBytes, ok := oldValues[pkCol]
		if !ok {
			// Fallback to newValues if oldValues doesn't have PK (shouldn't happen for UPDATEs)
			pkBytes, ok = newValues[pkCol]
			if !ok {
				return fmt.Errorf("primary key column %s not found in CDC data", pkCol)
			}
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		var value interface{}
		if err := encoding.Unmarshal(pkBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	_, err = tx.Exec(sqlStmt, values...)
	return err
}

// applyReplayCDCDelete performs DELETE using CDC row data
func (rh *ReplicationHandler) applyReplayCDCDelete(tx *sql.Tx, dbName string, tableName string, oldValues map[string][]byte) error {
	if len(oldValues) == 0 {
		return fmt.Errorf("no old values for delete")
	}

	// Get database instance to access cached schema
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %w", dbName, err)
	}

	// Use cached schema - does NOT query SQLite
	schema, err := dbInstance.GetCachedTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("failed to get cached schema for table %s: %w", tableName, err)
	}

	// Build WHERE clause using primary key columns from oldValues
	// Extract each PK value from oldValues and unmarshal
	whereClauses := make([]string, 0, len(schema.PrimaryKeys))
	values := make([]interface{}, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		pkValue, ok := oldValues[pkCol]
		if !ok {
			return fmt.Errorf("primary key column %s not found in CDC old values", pkCol)
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		var value interface{}
		if err := encoding.Unmarshal(pkValue, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
		tableName,
		strings.Join(whereClauses, " AND "))

	_, err = tx.Exec(sqlStmt, values...)
	return err
}

// serializeReplayStatements converts gRPC Statement slice to protocol.Statement slice
// and serializes them with msgpack for storage in TransactionRecord.SerializedStatements.
// This ensures replayed transactions can be streamed to other lagging nodes.
func (rh *ReplicationHandler) serializeReplayStatements(stmts []*Statement, dbName string) []byte {
	if len(stmts) == 0 {
		return nil
	}

	protoStmts := make([]protocol.Statement, 0, len(stmts))
	for _, stmt := range stmts {
		protoStmt := protocol.Statement{
			Type:      common.MustFromWireType(stmt.Type),
			TableName: stmt.TableName,
			Database:  dbName,
		}

		// Extract CDC data from RowChange payload
		if rowChange := stmt.GetRowChange(); rowChange != nil {
			protoStmt.IntentKey = rowChange.IntentKey
			protoStmt.OldValues = rowChange.OldValues
			protoStmt.NewValues = rowChange.NewValues
		}

		// Extract SQL from DDL payload
		if ddl := stmt.GetDdlChange(); ddl != nil {
			protoStmt.SQL = ddl.Sql
		}

		protoStmts = append(protoStmts, protoStmt)
	}

	data, err := encoding.Marshal(protoStmts)
	if err != nil {
		log.Error().Err(err).Int("statement_count", len(protoStmts)).Msg("serializeReplayStatements: failed to marshal - anti-entropy may not work for this transaction")
		return nil
	}

	return data
}

// HandleRead handles incoming read requests with MVCC snapshot isolation
func (rh *ReplicationHandler) HandleRead(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	// Update local clock with incoming timestamp
	snapshotTS := hlc.Timestamp{
		WallTime: req.SnapshotTs.WallTime,
		Logical:  req.SnapshotTs.Logical,
		NodeID:   req.SnapshotTs.NodeId,
	}
	rh.clock.Update(snapshotTS)

	// Get the target database from request (database name is required)
	dbName := req.Database
	if dbName == "" {
		return &ReadResponse{
			Timestamp: &HLC{
				WallTime: rh.clock.Now().WallTime,
				Logical:  rh.clock.Now().Logical,
				NodeId:   rh.nodeID,
			},
		}, fmt.Errorf("database name is required in read request")
	}

	// Get database instance
	dbInstance, err := rh.dbMgr.GetDatabase(dbName)
	if err != nil {
		return &ReadResponse{
			Timestamp: &HLC{
				WallTime: rh.clock.Now().WallTime,
				Logical:  rh.clock.Now().Logical,
				NodeId:   rh.nodeID,
			},
		}, fmt.Errorf("database %s not found: %w", dbName, err)
	}

	database := dbInstance.GetDB()

	// Execute local snapshot read
	rows, err := database.QueryContext(ctx, req.Query)
	if err != nil {
		return &ReadResponse{
			Timestamp: &HLC{
				WallTime: rh.clock.Now().WallTime,
				Logical:  rh.clock.Now().Logical,
				NodeId:   rh.nodeID,
			},
		}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read all rows
	var results []*Row
	for rows.Next() {
		// Create a slice of interface{}'s to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		// Build result map
		rowMap := make(map[string][]byte)
		for i, col := range columns {
			val := values[i]
			// Convert to bytes
			if b, ok := val.([]byte); ok {
				rowMap[col] = b
			} else if s, ok := val.(string); ok {
				rowMap[col] = []byte(s)
			} else {
				rowMap[col] = []byte(fmt.Sprintf("%v", val))
			}
		}
		results = append(results, &Row{Columns: rowMap})
	}

	return &ReadResponse{
		Rows: results,
		Timestamp: &HLC{
			WallTime: rh.clock.Now().WallTime,
			Logical:  rh.clock.Now().Logical,
			NodeId:   rh.nodeID,
		},
	}, nil
}
