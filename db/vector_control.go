package db

import (
	"fmt"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
)

const (
	vectorControlTrainerVersion = common.VectorControlTrainerVersion
	vectorControlCodecVersion   = common.VectorControlCodecVersion
)

func vectorIndexChangeFromStatement(stmt protocol.Statement, database string) (common.VectorIndexChange, error) {
	if stmt.VectorIndexChange != nil {
		change := *stmt.VectorIndexChange
		if change.Database == "" {
			change.Database = database
		}
		if change.Seed == 0 {
			change.Seed = StableIndexSeed(change.Meta())
		}
		return change, nil
	}
	action, err := vectorActionForStatement(stmt.Type)
	if err != nil {
		return common.VectorIndexChange{}, err
	}
	change := common.VectorIndexChange{
		Action:              action,
		Database:            database,
		IndexName:           stmt.VectorIndexName,
		TableName:           stmt.TableName,
		ColumnName:          stmt.VectorColumnName,
		Metric:              stmt.VectorMetric,
		Dim:                 stmt.VectorDim,
		Nlist:               stmt.VectorNlist,
		Nprobe:              stmt.VectorNprobe,
		TargetPartitionSize: common.DefaultVectorTargetPartitionSize,
		MaxNorm:             stmt.VectorMaxNorm,
		TrainerVersion:      vectorControlTrainerVersion,
		CodecVersion:        vectorControlCodecVersion,
		CreatedAt:           time.Now().UnixNano(),
	}
	change.AutoTuneNlist = change.Nlist == 0
	change.AutoTuneNprobe = change.Nprobe == 0
	change.Seed = StableIndexSeed(change.Meta())
	return change, nil
}

func vectorActionForStatement(stmtType protocol.StatementCode) (common.VectorIndexAction, error) {
	switch stmtType {
	case protocol.StatementCreateVectorIndex:
		return common.VectorIndexActionCreate, nil
	case protocol.StatementDropVectorIndex:
		return common.VectorIndexActionDrop, nil
	case protocol.StatementReindexVectorIndex:
		return common.VectorIndexActionReindex, nil
	case protocol.StatementVectorIndexControl:
		return common.VectorIndexActionCheckpoint, nil
	default:
		return 0, fmt.Errorf("not a vector index control statement: %d", stmtType)
	}
}

func statementTypeForVectorAction(action common.VectorIndexAction) protocol.StatementCode {
	switch action {
	case common.VectorIndexActionCreate:
		return protocol.StatementCreateVectorIndex
	case common.VectorIndexActionDrop:
		return protocol.StatementDropVectorIndex
	case common.VectorIndexActionReindex:
		return protocol.StatementReindexVectorIndex
	default:
		return protocol.StatementVectorIndexControl
	}
}

func vectorControlIntentKey(change common.VectorIndexChange) string {
	return string(filter.EncodeDDLIntentKey(change.Database + ":vector:" + change.IndexName))
}

func vectorIndexStatementFromChange(change common.VectorIndexChange) protocol.Statement {
	meta := change.Meta()
	return protocol.Statement{
		Type:              statementTypeForVectorAction(change.Action),
		TableName:         meta.TableName,
		Database:          meta.Database,
		IntentKey:         []byte(vectorControlIntentKey(change)),
		VectorIndexName:   meta.IndexName,
		VectorColumnName:  meta.ColumnName,
		VectorMetric:      meta.Metric,
		VectorDim:         meta.Dim,
		VectorNlist:       meta.Nlist,
		VectorNprobe:      meta.Nprobe,
		VectorMaxNorm:     meta.MaxNorm,
		VectorIndexChange: &change,
	}
}

func vectorIndexChangeSnapshot(stmt protocol.Statement, database string, startTS hlc.Timestamp) ([]byte, common.VectorIndexChange, error) {
	change, err := vectorIndexChangeFromStatement(stmt, database)
	if err != nil {
		return nil, common.VectorIndexChange{}, err
	}
	if change.CreatedAt == 0 {
		change.CreatedAt = startTS.WallTime
	}
	if change.Seed == 0 {
		change.Seed = StableIndexSeed(change.Meta())
	}
	data, err := SerializeData(change)
	if err != nil {
		return nil, common.VectorIndexChange{}, err
	}
	return data, change, nil
}
