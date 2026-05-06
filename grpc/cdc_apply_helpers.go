package grpc

import (
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

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

func StoreCapturedRowsFromStatements(metaStore db.MetaStore, txnID uint64, statements []*Statement) error {
	if metaStore == nil || len(statements) == 0 {
		return nil
	}
	for i, stmt := range statements {
		data, err := encodedCapturedRowFromStatement(stmt)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			continue
		}
		if err := metaStore.WriteCapturedRow(txnID, uint64(i+1), data); err != nil {
			return fmt.Errorf("write captured row: %w", err)
		}
	}
	return nil
}

func encodedCapturedRowFromStatement(stmt *Statement) ([]byte, error) {
	if stmt == nil {
		return nil, nil
	}
	if rowChange := stmt.GetRowChange(); rowChange != nil {
		if _, err := decodeRowChange(stmt); err != nil {
			return nil, err
		}
		return rowChange.EncodedRow, nil
	}
	row, err := capturedRowFromStatement(stmt)
	if err != nil || row == nil {
		return nil, err
	}
	data, err := db.EncodeRow(row)
	if err != nil {
		return nil, fmt.Errorf("encode captured row: %w", err)
	}
	return data, nil
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

func StoreAppliedChangeEvent(metaStore db.MetaStore, txnID uint64, timestamp *HLC, database string, statements []*Statement) (uint64, error) {
	if metaStore == nil {
		return 0, nil
	}
	if rec, err := metaStore.GetTransaction(txnID); err == nil && rec != nil && rec.Status == db.TxnStatusCommitted {
		return rec.SeqNum, nil
	}
	if err := StoreCapturedRowsFromStatements(metaStore, txnID, statements); err != nil {
		return 0, err
	}
	if err := metaStore.SealCapturedRows(txnID); err != nil {
		return 0, err
	}
	commitTS := HLCToTimestamp(timestamp)
	if err := metaStore.StoreReplayedTransaction(txnID, commitTS.NodeID, commitTS, database, uint32(len(statements))); err != nil {
		return 0, err
	}
	rec, err := metaStore.GetTransaction(txnID)
	if err != nil || rec == nil {
		return 0, err
	}
	return rec.SeqNum, nil
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
