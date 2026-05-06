package db

import (
	"github.com/maxpert/marmot/common"
)

const encodedCapturedRowCodecMsgpack = common.EncodedCapturedRowCodecMsgpack

func EncodedCapturedRowCodecMsgpack() uint32 {
	return encodedCapturedRowCodecMsgpack
}

type EncodedCapturedRow = common.EncodedCapturedRow

// EncodeRow serializes an EncodedCapturedRow to msgpack bytes.
func EncodeRow(row *EncodedCapturedRow) ([]byte, error) {
	return common.EncodeCapturedRow(row)
}

// DecodeRow deserializes msgpack bytes to an EncodedCapturedRow.
func DecodeRow(data []byte) (*EncodedCapturedRow, error) {
	return common.DecodeCapturedRow(data)
}
