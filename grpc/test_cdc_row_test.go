package grpc

import "github.com/maxpert/marmot/db"

func testRowChange(table string, op uint8, intentKey []byte, oldValues, newValues map[string][]byte) *RowChange {
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
	return &RowChange{
		EncodedRow:      encoded,
		EncodedRowCodec: db.EncodedCapturedRowCodecMsgpack(),
	}
}

func testInsertRowChange(table string, intentKey []byte, newValues map[string][]byte) *RowChange {
	return testRowChange(table, uint8(db.OpTypeInsert), intentKey, nil, newValues)
}

func testUpdateRowChange(table string, intentKey []byte, oldValues, newValues map[string][]byte) *RowChange {
	return testRowChange(table, uint8(db.OpTypeUpdate), intentKey, oldValues, newValues)
}

func testDeleteRowChange(table string, intentKey []byte, oldValues map[string][]byte) *RowChange {
	return testRowChange(table, uint8(db.OpTypeDelete), intentKey, oldValues, nil)
}
