package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// ErrRowCountMismatch is returned by ApplyPulledEvent when a pulled
// ChangeEvent's statement count does not match its RowCount: the event is
// never applied. FetchTransactions
// already refuses to serve such a transaction server-side, but a puller must
// not trust the wire without its own check.
var ErrRowCountMismatch = errors.New("pulled change event: statement count does not match row_count")

// ErrNilCapturedRow is returned by ApplyPulledEvent when a wire statement
// decodes to no captured row at all: an unrecognized
// payload shape, such as one a newer peer added and this node's binary
// predates. Silently dropping it would apply a partial transaction under the
// original RowCount, so the whole event is refused instead, nothing applied.
var ErrNilCapturedRow = errors.New("pulled change event: statement decoded to no captured row")

func wireDMLToOp(stmtType pb.StatementType) (db.OpType, error) {
	stmtCode, ok := common.FromWireType(stmtType)
	if !ok {
		return 0, fmt.Errorf("unknown statement type %v", stmtType)
	}
	switch stmtCode {
	case protocol.StatementInsert, protocol.StatementUpdate, protocol.StatementDelete, protocol.StatementReplace:
		return db.StatementTypeToOpType(stmtCode), nil
	default:
		return 0, fmt.Errorf("unsupported statement type for CDC: %v", stmtType)
	}
}

func HLCToTimestamp(ts *HLC) hlc.Timestamp {
	if ts == nil {
		return hlc.Timestamp{}
	}
	return hlc.Timestamp{
		WallTime: ts.WallTime,
		Logical:  ts.Logical,
		NodeID:   ts.NodeId,
	}
}

// StoreAppliedChangeEvent records a replayed transaction's log entry after
// it has been applied, delegating to db.ReplicatedDatabase.LogReplayedTxn
// (db/replay_apply.go), which this function's logic moved into.
// originNodeID is the transaction's coordinator (the DDL incarnation owner),
// not necessarily the peer this event was pulled from;
// requiredSchemaVersion is the value the originating commit recorded.
// Callers without either value (a rolling-upgrade peer that omits them on
// the wire) pass 0.
func StoreAppliedChangeEvent(mdb *db.ReplicatedDatabase, txnID uint64, timestamp *HLC, statements []*Statement, originNodeID uint64, requiredSchemaVersion uint64) (uint64, error) {
	if mdb == nil {
		return 0, nil
	}
	rows := make([]*db.EncodedCapturedRow, 0, len(statements))
	for _, stmt := range statements {
		row, err := capturedRowFromStatement(stmt)
		if err != nil {
			return 0, err
		}
		if row == nil {
			continue
		}
		rows = append(rows, row)
	}
	return mdb.LogReplayedTxn(&db.ReplayTxn{
		TxnID:                 txnID,
		OriginNodeID:          originNodeID,
		CommitTS:              HLCToTimestamp(timestamp),
		RequiredSchemaVersion: requiredSchemaVersion,
		Rows:                  rows,
	})
}

// ApplyPulledEvent applies one transaction pulled from a peer's local commit
// log via FetchTransactions, exactly-once and atomically with the
// applied-txn marker (db.ReplicatedDatabase.ApplyReplayedTxn). This is the
// log puller's (D2a) sole entry point for applying a pulled ChangeEvent: it
// is called in-process, directly, and never goes through
// TransactionRequest/ReplicateTransaction.
//
// It refuses (ErrRowCountMismatch, touching nothing) any event whose
// statement count does not match RowCount - the same check
// FetchTransactions applies server-side, repeated here because a puller must
// not trust the wire. OriginNodeID and CommitTS.NodeID both fall back to
// ev.Timestamp.NodeId when OriginNodeId is unset, for a rolling-upgrade peer
// that omits it.
func ApplyPulledEvent(ctx context.Context, dbMgr *db.DatabaseManager, ev *ChangeEvent) (bool, error) {
	if ev == nil {
		return false, fmt.Errorf("apply pulled event: nil event")
	}
	if ev.Database == "" {
		return false, fmt.Errorf("apply pulled event: missing database")
	}
	if uint32(len(ev.Statements)) != ev.RowCount {
		return false, fmt.Errorf("%w: txn %d has %d statements, want %d", ErrRowCountMismatch, ev.TxnId, len(ev.Statements), ev.RowCount)
	}

	mdb, err := dbMgr.GetDatabase(ev.Database)
	if err != nil {
		return false, fmt.Errorf("apply pulled event: database %s: %w", ev.Database, err)
	}

	rows := make([]*db.EncodedCapturedRow, 0, len(ev.Statements))
	for _, stmt := range ev.Statements {
		row, err := capturedRowFromStatement(stmt)
		if err != nil {
			return false, fmt.Errorf("apply pulled event: decode statement: %w", err)
		}
		if row == nil {
			return false, fmt.Errorf("%w: txn %d", ErrNilCapturedRow, ev.TxnId)
		}
		rows = append(rows, row)
	}

	origin := ev.OriginNodeId
	if origin == 0 {
		origin = ev.GetTimestamp().GetNodeId()
	}

	t := &db.ReplayTxn{
		TxnID:                 ev.TxnId,
		OriginNodeID:          origin,
		CommitTS:              HLCToTimestamp(ev.Timestamp),
		RequiredSchemaVersion: ev.RequiredSchemaVersion,
		Rows:                  rows,
	}
	return mdb.ApplyReplayedTxn(ctx, t, true)
}

func capturedRowFromStatement(stmt *Statement) (*db.EncodedCapturedRow, error) {
	if stmt == nil {
		return nil, nil
	}
	if change := stmt.GetVectorIndexChange(); change != nil {
		vectorChange := vectorChangeFromProto(change)
		return &db.EncodedCapturedRow{
			Table:             stmt.TableName,
			Op:                uint8(db.OpTypeVectorIndex),
			VectorIndexChange: &vectorChange,
		}, nil
	}
	if rowChange := stmt.GetRowChange(); rowChange != nil {
		row, err := decodeRowChange(stmt)
		if err != nil {
			return nil, err
		}
		return row, nil
	}
	if ddl := stmt.GetDdlChange(); ddl != nil && ddl.Sql != "" {
		return &db.EncodedCapturedRow{
			Table:  stmt.TableName,
			Op:     uint8(db.OpTypeDDL),
			DDLSQL: ddl.Sql,
		}, nil
	}
	if loadData := stmt.GetLoadDataChange(); loadData != nil {
		return &db.EncodedCapturedRow{
			Table:    stmt.TableName,
			Op:       uint8(db.OpTypeLoadData),
			LoadSQL:  loadData.Sql,
			LoadData: loadData.Data,
		}, nil
	}
	return nil, nil
}

func decodeRowChange(stmt *Statement) (*db.EncodedCapturedRow, error) {
	rowChange := stmt.GetRowChange()
	if rowChange == nil {
		return nil, nil
	}
	if rowChange.EncodedRowCodec != db.EncodedCapturedRowCodecMsgpack() {
		return nil, fmt.Errorf("unsupported encoded row codec %d", rowChange.EncodedRowCodec)
	}
	if len(rowChange.EncodedRow) == 0 {
		return nil, fmt.Errorf("missing encoded row for DML statement")
	}
	row, err := db.DecodeRow(rowChange.EncodedRow)
	if err != nil {
		return nil, fmt.Errorf("decode encoded row: %w", err)
	}
	if stmt.TableName != "" && row.Table != "" && row.Table != stmt.TableName {
		return nil, fmt.Errorf("encoded row table mismatch: statement=%s row=%s", stmt.TableName, row.Table)
	}
	op, err := wireDMLToOp(stmt.Type)
	if err != nil {
		return nil, err
	}
	if row.Op != uint8(op) {
		return nil, fmt.Errorf("encoded row op mismatch: statement=%v row=%d", stmt.Type, row.Op)
	}
	return row, nil
}

func DecodeRowChangeForCDC(stmt *Statement) (*db.EncodedCapturedRow, error) {
	return decodeRowChange(stmt)
}
