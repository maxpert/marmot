package replica

import (
	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

func testRowChange(table string, op uint8, intentKey []byte, oldValues, newValues map[string][]byte) *marmotgrpc.RowChange {
	row := &db.EncodedCapturedRow{
		Table:     table,
		Op:        op,
		IntentKey: intentKey,
		OldValues: oldValues,
		NewValues: newValues,
	}
	encoded, err := db.EncodeRow(row)
	if err != nil {
		panic(err)
	}
	return &marmotgrpc.RowChange{
		EncodedRow:      encoded,
		EncodedRowCodec: db.EncodedCapturedRowCodecMsgpack(),
	}
}

func testInsertRowChange(table string, intentKey []byte, newValues map[string][]byte) *marmotgrpc.RowChange {
	return testRowChange(table, uint8(db.OpTypeInsert), intentKey, nil, newValues)
}
