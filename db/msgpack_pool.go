package db

import "github.com/maxpert/marmot/protocol"

// MarshalMsgpack encodes a value using a pooled encoder.
// Wrapper around protocol.MarshalMsgpack for convenience in db package.
func MarshalMsgpack(v interface{}) ([]byte, error) {
	return protocol.MarshalMsgpack(v)
}
