package db

import (
	"github.com/maxpert/marmot/encoding"
)

// EncodedCapturedRow stores schema-encoded row data at capture time.
// All column values are msgpack-encoded when the row is captured.
// For DDL operations (Op == OpTypeDDL), only Table and DDLSQL are used.
type EncodedCapturedRow struct {
	Table     string            `msgpack:"t"`
	Op        uint8             `msgpack:"o"`
	IntentKey []byte            `msgpack:"k"`
	OldValues map[string][]byte `msgpack:"ov,omitempty"`
	NewValues map[string][]byte `msgpack:"nv,omitempty"`
	DDLSQL    string            `msgpack:"ddl,omitempty"`   // DDL statement (only for OpTypeDDL)
	LoadSQL   string            `msgpack:"ldsql,omitempty"` // LOAD DATA statement (only for OpTypeLoadData)
	LoadData  []byte            `msgpack:"ldd,omitempty"`   // LOAD DATA payload bytes
}

// EncodeRow serializes an EncodedCapturedRow to msgpack bytes.
func EncodeRow(row *EncodedCapturedRow) ([]byte, error) {
	return encoding.Marshal(row)
}

// DecodeRow deserializes msgpack bytes to an EncodedCapturedRow.
func DecodeRow(data []byte) (*EncodedCapturedRow, error) {
	var row EncodedCapturedRow
	if err := encoding.Unmarshal(data, &row); err != nil {
		return nil, err
	}
	return &row, nil
}
