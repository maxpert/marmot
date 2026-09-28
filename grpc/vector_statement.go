package grpc

import (
	"fmt"

	appcommon "github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
)

func vectorChangeToProto(change appcommon.VectorIndexChange) *VectorIndexChange {
	return &VectorIndexChange{
		Action:              vectorActionToProto(change.Action),
		Database:            change.Database,
		IndexName:           change.IndexName,
		TableName:           change.TableName,
		ColumnName:          change.ColumnName,
		Metric:              change.Metric,
		Dim:                 int32(change.Dim),
		Nlist:               int32(change.Nlist),
		Nprobe:              int32(change.Nprobe),
		AutoTuneNlist:       change.AutoTuneNlist,
		AutoTuneNprobe:      change.AutoTuneNprobe,
		TargetPartitionSize: int32(change.TargetPartitionSize),
		MaxNorm:             change.MaxNorm,
		SourceProbeEpoch:    change.SourceProbeEpoch,
		TargetProbeEpoch:    change.TargetProbeEpoch,
		CutoffTxnId:         change.CutoffTxnID,
		CutoffSeqNum:        change.CutoffSeqNum,
		TrainerVersion:      change.TrainerVersion,
		CodecVersion:        change.CodecVersion,
		Seed:                change.Seed,
		CreatedAt:           change.CreatedAt,
	}
}

func VectorChangeToProto(change appcommon.VectorIndexChange) *VectorIndexChange {
	return vectorChangeToProto(change)
}

func vectorChangeFromProto(change *VectorIndexChange) appcommon.VectorIndexChange {
	if change == nil {
		return appcommon.VectorIndexChange{}
	}
	return appcommon.VectorIndexChange{
		Action:              vectorActionFromProto(change.Action),
		Database:            change.Database,
		IndexName:           change.IndexName,
		TableName:           change.TableName,
		ColumnName:          change.ColumnName,
		Metric:              change.Metric,
		Dim:                 int(change.Dim),
		Nlist:               int(change.Nlist),
		Nprobe:              int(change.Nprobe),
		AutoTuneNlist:       change.AutoTuneNlist,
		AutoTuneNprobe:      change.AutoTuneNprobe,
		TargetPartitionSize: int(change.TargetPartitionSize),
		MaxNorm:             change.MaxNorm,
		SourceProbeEpoch:    change.SourceProbeEpoch,
		TargetProbeEpoch:    change.TargetProbeEpoch,
		CutoffTxnID:         change.CutoffTxnId,
		CutoffSeqNum:        change.CutoffSeqNum,
		TrainerVersion:      change.TrainerVersion,
		CodecVersion:        change.CodecVersion,
		Seed:                change.Seed,
		CreatedAt:           change.CreatedAt,
	}
}

func VectorChangeFromProto(change *VectorIndexChange) appcommon.VectorIndexChange {
	return vectorChangeFromProto(change)
}

func vectorActionToProto(action appcommon.VectorIndexAction) VectorIndexAction {
	switch action {
	case appcommon.VectorIndexActionCreate:
		return VectorIndexAction_VECTOR_INDEX_ACTION_CREATE
	case appcommon.VectorIndexActionDrop:
		return VectorIndexAction_VECTOR_INDEX_ACTION_DROP
	case appcommon.VectorIndexActionReindex:
		return VectorIndexAction_VECTOR_INDEX_ACTION_REINDEX
	case appcommon.VectorIndexActionCheckpoint:
		return VectorIndexAction_VECTOR_INDEX_ACTION_CHECKPOINT
	default:
		return VectorIndexAction_VECTOR_INDEX_ACTION_UNKNOWN
	}
}

func vectorActionFromProto(action VectorIndexAction) appcommon.VectorIndexAction {
	switch action {
	case VectorIndexAction_VECTOR_INDEX_ACTION_CREATE:
		return appcommon.VectorIndexActionCreate
	case VectorIndexAction_VECTOR_INDEX_ACTION_DROP:
		return appcommon.VectorIndexActionDrop
	case VectorIndexAction_VECTOR_INDEX_ACTION_REINDEX:
		return appcommon.VectorIndexActionReindex
	case VectorIndexAction_VECTOR_INDEX_ACTION_CHECKPOINT:
		return appcommon.VectorIndexActionCheckpoint
	default:
		return 0
	}
}

// protocolStatementFromProto converts a wire Statement into the internal
// protocol.Statement representation. It returns an error - rather than
// panicking - when the wire carries a StatementType or a oneof payload case
// this binary does not recognise, so that a newer peer talking to an older
// node fails the transaction instead of crashing the process or silently
// dropping the change.
func protocolStatementFromProto(stmt *Statement) (protocol.Statement, error) {
	code, ok := appcommon.FromWireType(stmt.Type)
	if !ok {
		return protocol.Statement{}, fmt.Errorf("unknown wire StatementType %d", stmt.Type)
	}

	internalStmt := protocol.Statement{
		SQL:                stmt.GetSQL(),
		Type:               code,
		TableName:          stmt.TableName,
		Database:           stmt.Database,
		IntentKey:          stmt.GetIntentKey(),
		AutoIDClaim:        stmt.GetAutoIdClaim(),
		AutoIDClaimPayload: stmt.GetAutoIdClaimPayload(),
		DatabaseGeneration: stmt.GetDatabaseGeneration(),
	}

	switch payload := stmt.Payload.(type) {
	case nil:
		// No payload set - accepted as-is (e.g. tests, control-only statements).
	case *Statement_VectorIndexChange:
		vectorChange := vectorChangeFromProto(payload.VectorIndexChange)
		internalStmt.Type = statementTypeForVectorAction(vectorChange.Action)
		internalStmt.TableName = vectorChange.TableName
		internalStmt.Database = vectorChange.Database
		internalStmt.IntentKey = nil
		internalStmt.VectorIndexName = vectorChange.IndexName
		internalStmt.VectorColumnName = vectorChange.ColumnName
		internalStmt.VectorMetric = vectorChange.Metric
		internalStmt.VectorDim = vectorChange.Dim
		internalStmt.VectorNlist = vectorChange.Nlist
		internalStmt.VectorNprobe = vectorChange.Nprobe
		internalStmt.VectorMaxNorm = vectorChange.MaxNorm
		internalStmt.VectorIndexChange = &vectorChange
		return internalStmt, nil
	case *Statement_RowChange:
		internalStmt.EncodedRow = payload.RowChange.EncodedRow
		internalStmt.EncodedCodec = payload.RowChange.EncodedRowCodec
		if row, err := decodeRowChange(stmt); err == nil && row != nil {
			internalStmt.IntentKey = row.IntentKey
			internalStmt.Operation = row.Op
			internalStmt.OldValues = row.OldValues
			internalStmt.NewValues = row.NewValues
		}
	case *Statement_LoadDataChange:
		internalStmt.SQL = payload.LoadDataChange.Sql
		internalStmt.LoadDataPayload = payload.LoadDataChange.Data
	case *Statement_DdlChange:
		// Already consumed above via stmt.GetSQL().
	case *Statement_DmlIntent:
		// Already consumed above via stmt.GetIntentKey().
	default:
		return protocol.Statement{}, fmt.Errorf("unknown statement payload type %T", payload)
	}
	return internalStmt, nil
}

func statementTypeForVectorAction(action appcommon.VectorIndexAction) protocol.StatementCode {
	switch action {
	case appcommon.VectorIndexActionCreate:
		return protocol.StatementCreateVectorIndex
	case appcommon.VectorIndexActionDrop:
		return protocol.StatementDropVectorIndex
	case appcommon.VectorIndexActionReindex:
		return protocol.StatementReindexVectorIndex
	default:
		return protocol.StatementVectorIndexControl
	}
}
