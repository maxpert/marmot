package common

import "github.com/maxpert/marmot/encoding"

const EncodedCapturedRowCodecMsgpack uint32 = 1

// CDCEntry represents a CDC entry for change data capture.
// This type is shared between coordinator and publisher packages.
type CDCEntry struct {
	Table        string
	IntentKey    []byte
	Operation    uint8
	OldValues    map[string][]byte
	NewValues    map[string][]byte
	EncodedRow   []byte
	EncodedCodec uint32
	CommitTxnID  uint64
	CommitSeqNum uint64
}

// EncodedCapturedRow is the canonical DML row-change representation used on
// disk and over the replication wire. Decoded value maps are apply-time state.
type EncodedCapturedRow struct {
	Table             string             `msgpack:"t"`
	Op                uint8              `msgpack:"o"`
	IntentKey         []byte             `msgpack:"k"`
	OldValues         map[string][]byte  `msgpack:"ov,omitempty"`
	NewValues         map[string][]byte  `msgpack:"nv,omitempty"`
	DDLSQL            string             `msgpack:"ddl,omitempty"`
	LoadSQL           string             `msgpack:"ldsql,omitempty"`
	LoadData          []byte             `msgpack:"ldd,omitempty"`
	VectorIndexChange *VectorIndexChange `msgpack:"vic,omitempty"`
}

func EncodeCapturedRow(row *EncodedCapturedRow) ([]byte, error) {
	return encoding.Marshal(row)
}

func DecodeCapturedRow(data []byte) (*EncodedCapturedRow, error) {
	var row EncodedCapturedRow
	if err := encoding.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	return &row, nil
}
