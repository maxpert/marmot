package grpc

import (
	"testing"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/require"
)

func TestVectorIndexChangeProtoRoundTrip(t *testing.T) {
	t.Parallel()

	change := common.VectorIndexChange{
		Action:              common.VectorIndexActionCheckpoint,
		Database:            "app",
		IndexName:           "docs_embed_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Metric:              "cosine",
		Dim:                 1536,
		Nlist:               256,
		Nprobe:              32,
		AutoTuneNlist:       true,
		AutoTuneNprobe:      true,
		TargetPartitionSize: 512,
		MaxNorm:             1,
		SourceProbeEpoch:    7,
		TargetProbeEpoch:    8,
		CutoffTxnID:         9001,
		CutoffSeqNum:        42,
		TrainerVersion:      3,
		CodecVersion:        4,
		Seed:                99,
		CreatedAt:           12345,
	}

	got := vectorChangeFromProto(vectorChangeToProto(change))
	require.Equal(t, change, got)
}

func TestConvertStatementsToProtoUsesVectorControlPayload(t *testing.T) {
	t.Parallel()

	change := common.VectorIndexChange{
		Action:              common.VectorIndexActionCreate,
		Database:            "app",
		IndexName:           "docs_embed_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Metric:              "l2",
		Dim:                 128,
		Nlist:               64,
		Nprobe:              12,
		TargetPartitionSize: 512,
		TrainerVersion:      1,
		CodecVersion:        1,
		Seed:                77,
	}
	stmts, err := convertStatementsToProto([]protocol.Statement{{
		Type:              protocol.StatementCreateVectorIndex,
		Database:          "app",
		TableName:         "docs",
		VectorIndexChange: &change,
	}}, "app", 11)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.NotNil(t, stmts[0].GetVectorIndexChange())
	require.Empty(t, stmts[0].GetSQL())

	internal := protocolStatementFromProto(stmts[0])
	require.Equal(t, protocol.StatementCreateVectorIndex, internal.Type)
	require.Equal(t, "docs_embed_idx", internal.VectorIndexName)
	require.NotNil(t, internal.VectorIndexChange)
	require.Equal(t, change, *internal.VectorIndexChange)
}
