package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// defaultLogListLimit is ListCommittedLog's page size when the request
	// leaves Limit unset (0).
	defaultLogListLimit = 256

	// maxLogListLimit caps ListCommittedLog's page size regardless of what
	// the request asks for.
	maxLogListLimit = 4096

	// maxFetchTransactionIDs caps how many txn ids one FetchTransactions
	// call accepts, so a single call cannot be made to iterate an unbounded
	// number of transactions.
	maxFetchTransactionIDs = 4096
)

// ListCommittedLog serves one page of this node's local commit log for one
// database, strictly after (after_seq, after_txn_id) and up to the store's
// stable point, to a peer pulling for anti-entropy. Before it answers, it durably records the requester's consumed
// position R[self,requester,d] = (consumed_seq, consumed_txn_id) as is - not
// as a max. It is separate from the
// list position because a requester lists past an entry it could not apply
// yet while its consumed position stays
// before that entry, so GC keeps it.
//
// A database this node does not have (never created, or tombstoned) answers
// database_absent=true rather than an error: the caller's database-set
// reconciliation (ListDatabaseRegistry) is what decides whether that is
// expected.
func (s *Server) ListCommittedLog(ctx context.Context, req *LogListRequest) (*LogListResponse, error) {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()
	if dbManager == nil {
		return nil, status.Error(codes.Unavailable, "database manager not initialized")
	}
	if req.RequestingNodeId != 0 {
		s.registry.TouchLastSeen(req.RequestingNodeId)
	}

	mdb, err := dbManager.GetDatabase(req.Database)
	if err != nil {
		if errors.Is(err, db.ErrDatabaseDetached) {
			return nil, status.Errorf(codes.Unavailable, "database %s: %v", req.Database, err)
		}
		return &LogListResponse{DatabaseAbsent: true}, nil
	}
	metaStore := mdb.GetMetaStore()

	after := db.LogPosition{Seq: req.AfterSeq, TxnID: req.AfterTxnId}
	if req.RequestingNodeId != 0 {
		consumed := db.LogPosition{Seq: req.ConsumedSeq, TxnID: req.ConsumedTxnId}
		if err := metaStore.SetConsumedPosition(req.RequestingNodeId, consumed); err != nil {
			return nil, status.Errorf(codes.Internal, "record consumed position: %v", err)
		}
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = defaultLogListLimit
	}
	if limit > maxLogListLimit {
		limit = maxLogListLimit
	}

	entries, stable, more, err := metaStore.ListCommittedLog(after, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list committed log: %v", err)
	}
	truncated, err := metaStore.TruncatedThrough()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read truncation point: %v", err)
	}

	pbEntries, err := logEntriesToProto(metaStore, entries)
	if err != nil {
		return nil, err
	}

	resp := &LogListResponse{
		Entries:        pbEntries,
		StableSeq:      stable,
		TruncatedSeq:   truncated.Seq,
		TruncatedTxnId: truncated.TxnID,
		More:           more,
		SchemaVersion:  mdb.SchemaVersion(),
	}
	if req.CountRemaining {
		resp.RemainingCommitted = remainingCommitted(ctx, metaStore, req.Database, after, entries, stable, more)
	}
	return resp, nil
}

// remainingCommitted counts the COMMITTED entries through stable after the
// page listed from `after`: none when the page was the last one, and
// otherwise every entry past the page's last, which walks them all. The
// count only feeds a metric, so a count that fails or outlasts ctx returns
// nil (unknown) rather than failing the page.
func remainingCommitted(ctx context.Context, metaStore db.MetaStore, database string, after db.LogPosition, entries []db.CommittedLogEntry, stable uint64, more bool) *uint64 {
	var n uint64
	if !more {
		return &n
	}
	if len(entries) > 0 {
		after = entries[len(entries)-1].LogPosition
	}
	n, err := metaStore.CountCommittedLog(ctx, after, stable)
	if err != nil {
		ev := log.Warn()
		if ctx.Err() != nil {
			ev = log.Debug() // the requester gave up; nothing is wrong here
		}
		ev.Err(err).Str("database", database).Msg("count of remaining committed log entries abandoned; left unset")
		return nil
	}
	return &n
}

// logEntriesToProto converts listed log entries to LogEntry messages, each
// carrying the commit timestamp stored with its position, which a puller
// that holds the transaction prepared commits it with. Only an entry
// written before positions carried a timestamp reads it from the
// transaction's record.
func logEntriesToProto(metaStore db.MetaStore, entries []db.CommittedLogEntry) ([]*LogEntry, error) {
	pbEntries := make([]*LogEntry, 0, len(entries))
	for _, e := range entries {
		commitTS := e.CommitTS
		if commitTS.IsZero() {
			rec, err := metaStore.GetTransaction(e.TxnID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "read transaction %d: %v", e.TxnID, err)
			}
			if rec != nil {
				commitTS = hlc.Timestamp{WallTime: rec.CommitTSWall, Logical: rec.CommitTSLogical, NodeID: rec.NodeID}
			}
		}
		entry := &LogEntry{Seq: e.Seq, TxnId: e.TxnID}
		if !commitTS.IsZero() {
			entry.CommitTimestamp = &HLC{WallTime: commitTS.WallTime, Logical: commitTS.Logical, NodeId: commitTS.NodeID}
		}
		pbEntries = append(pbEntries, entry)
	}
	return pbEntries, nil
}

// FetchTransactions streams the named committed transactions of one
// database, in the order requested, then ends (EOF). Unlike StreamChanges'
// existing lenient semantics (kept unchanged for read replicas),
// it fails the WHOLE call rather than silently skip or send a partial
// transaction:
//   - a requested id that is missing, or not COMMITTED (GC may have removed
//     it): codes.NotFound;
//   - any captured-row iterate or decode error, or a captured row count that
//     does not match the commit record's own RowCount: codes.FailedPrecondition.
//
// Each event carries the transaction's immutable NodeID as origin_node_id
// (the DDL incarnation owner, which must not change across replays) and the
// commit record's RowCount, with the HLC timestamp's NodeId also set to the
// origin.
func (s *Server) FetchTransactions(req *FetchTransactionsRequest, stream MarmotService_FetchTransactionsServer) error {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()
	if dbManager == nil {
		return status.Error(codes.Unavailable, "database manager not initialized")
	}
	if req.RequestingNodeId != 0 {
		s.registry.TouchLastSeen(req.RequestingNodeId)
	}
	if len(req.TxnIds) > maxFetchTransactionIDs {
		return status.Errorf(codes.InvalidArgument, "fetch transactions: %d ids exceeds the %d per-call limit", len(req.TxnIds), maxFetchTransactionIDs)
	}

	mdb, err := dbManager.GetDatabase(req.Database)
	if err != nil {
		return status.Errorf(codes.NotFound, "database %s: %v", req.Database, err)
	}
	metaStore := mdb.GetMetaStore()

	for _, txnID := range req.TxnIds {
		rec, err := metaStore.GetTransaction(txnID)
		if err != nil {
			return status.Errorf(codes.Internal, "read transaction %d: %v", txnID, err)
		}
		if rec == nil || rec.Status != db.TxnStatusCommitted {
			return status.Errorf(codes.NotFound, "transaction %d is not a committed local log entry", txnID)
		}

		statements, err := strictStatementsForFetch(req.Database, rec, metaStore)
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "transaction %d: %v", txnID, err)
		}

		event := &ChangeEvent{
			TxnId:      rec.TxnID,
			Statements: statements,
			Timestamp: &HLC{
				WallTime: rec.CommitTSWall,
				Logical:  rec.CommitTSLogical,
				NodeId:   rec.NodeID,
			},
			Database:              req.Database,
			RequiredSchemaVersion: rec.RequiredSchemaVersion,
			SeqNum:                rec.SeqNum,
			OriginNodeId:          rec.NodeID,
			RowCount:              rec.RowCount,
		}
		if err := stream.Send(event); err != nil {
			return err
		}
	}
	return nil
}

// ListDatabaseRegistry lists this node's database registry, tombstones
// included, so a peer's anti-entropy round can reconcile the database set
// before pulling logs.
func (s *Server) ListDatabaseRegistry(ctx context.Context, req *DatabaseRegistryRequest) (*DatabaseRegistryResponse, error) {
	s.mu.RLock()
	dbManager := s.dbManager
	s.mu.RUnlock()
	if dbManager == nil {
		return nil, status.Error(codes.Unavailable, "database manager not initialized")
	}
	if req.RequestingNodeId != 0 {
		s.registry.TouchLastSeen(req.RequestingNodeId)
	}

	entries, err := dbManager.RegistryEntries()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list database registry: %v", err)
	}

	pbEntries := make([]*DatabaseRegistryEntry, 0, len(entries))
	for _, e := range entries {
		pbEntries = append(pbEntries, &DatabaseRegistryEntry{
			Name:       e.Name,
			Generation: e.Key.Generation,
			Dropped:    e.Key.Dropped,
			Legacy:     e.Legacy,
		})
	}
	return &DatabaseRegistryResponse{Entries: pbEntries}, nil
}

// statementFromCapturedRow converts one decoded captured row into the wire
// Statement both sendChangeEvent (StreamChanges) and FetchTransactions send.
// encodedRow is the row's raw msgpack encoding, carried as-is on the DML
// path rather than re-encoded.
func statementFromCapturedRow(dbName string, row *db.EncodedCapturedRow, encodedRow []byte) (*Statement, error) {
	switch db.OpType(row.Op) {
	case db.OpTypeVectorIndex:
		if row.VectorIndexChange == nil {
			return nil, fmt.Errorf("vector index CDC row missing payload")
		}
		return &Statement{
			Type:      common.MustToWireType(common.StatementVectorIndexControl),
			TableName: row.Table,
			Database:  dbName,
			Payload: &Statement_VectorIndexChange{
				VectorIndexChange: vectorChangeToProto(*row.VectorIndexChange),
			},
		}, nil
	case db.OpTypeDDL:
		return &Statement{
			Type:      common.MustToWireType(db.OpTypeToStatementType(db.OpType(row.Op))),
			TableName: row.Table,
			Database:  dbName,
			Payload: &Statement_DdlChange{
				DdlChange: &DDLChange{Sql: row.DDLSQL},
			},
		}, nil
	case db.OpTypeLoadData:
		return &Statement{
			Type:      common.MustToWireType(db.OpTypeToStatementType(db.OpType(row.Op))),
			TableName: row.Table,
			Database:  dbName,
			Payload: &Statement_LoadDataChange{
				LoadDataChange: &LoadDataChange{Sql: row.LoadSQL, Data: row.LoadData},
			},
		}, nil
	default:
		return &Statement{
			Type:      common.MustToWireType(db.OpTypeToStatementType(db.OpType(row.Op))),
			TableName: row.Table,
			Database:  dbName,
			Payload: &Statement_RowChange{
				RowChange: &RowChange{
					EncodedRow:      append([]byte(nil), encodedRow...),
					EncodedRowCodec: db.EncodedCapturedRowCodecMsgpack(),
				},
			},
		}, nil
	}
}

// lenientStatementsFromLog builds rec's wire statements from its captured
// rows, keeping StreamChanges' existing lenient semantics unchanged for read
// replicas: an iterate or decode error is logged and the
// affected row is skipped rather than failing the whole stream.
func lenientStatementsFromLog(rec *db.TransactionRecord, metaStore db.MetaStore) []*Statement {
	cursor, err := metaStore.IterateCapturedRows(rec.TxnID)
	if err != nil {
		log.Warn().Err(err).Uint64("txn_id", rec.TxnID).Msg("Failed to iterate captured rows for streaming")
		return nil
	}
	defer cursor.Close()

	var statements []*Statement
	for cursor.Next() {
		_, data := cursor.Row()
		row, err := db.DecodeRow(data)
		if err != nil {
			log.Warn().Err(err).Uint64("txn_id", rec.TxnID).Msg("Failed to decode captured row")
			continue
		}
		stmt, err := statementFromCapturedRow(rec.DatabaseName, row, data)
		if err != nil {
			log.Warn().Err(err).Uint64("txn_id", rec.TxnID).Msg("Failed to convert captured row for streaming")
			continue
		}
		statements = append(statements, stmt)
	}
	if err := cursor.Err(); err != nil {
		log.Warn().Err(err).Uint64("txn_id", rec.TxnID).Msg("Failed to iterate captured rows for streaming")
	}
	return statements
}

// strictStatementsForFetch builds rec's wire statements from its captured
// rows for FetchTransactions, refusing to serve a partial transaction: any
// iterate or decode error, or a captured row count that does not
// match rec.RowCount, fails outright instead of skipping the offending row.
func strictStatementsForFetch(database string, rec *db.TransactionRecord, metaStore db.MetaStore) ([]*Statement, error) {
	cursor, err := metaStore.IterateCapturedRows(rec.TxnID)
	if err != nil {
		return nil, fmt.Errorf("iterate captured rows: %w", err)
	}
	defer cursor.Close()

	statements := make([]*Statement, 0, rec.RowCount)
	for cursor.Next() {
		_, data := cursor.Row()
		row, err := db.DecodeRow(data)
		if err != nil {
			return nil, fmt.Errorf("decode captured row: %w", err)
		}
		stmt, err := statementFromCapturedRow(database, row, data)
		if err != nil {
			return nil, fmt.Errorf("convert captured row: %w", err)
		}
		statements = append(statements, stmt)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate captured rows: %w", err)
	}
	if uint32(len(statements)) != rec.RowCount {
		return nil, fmt.Errorf("captured row count mismatch: got %d want %d", len(statements), rec.RowCount)
	}
	return statements, nil
}
