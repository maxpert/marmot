// Package encoding provides centralized serialization/deserialization for Marmot.
// ALL msgpack operations MUST go through this package to ensure consistent behavior.
//
// Thread Safety: Marshal and Unmarshal are safe for concurrent use.
//
// Type Preservation: When decoding into interface{}, msgpack strings decode as
// Go strings (not []byte). This is critical for SQLite which treats BLOB and TEXT
// as different types for PRIMARY KEY comparison.
package encoding

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

// Marshal encodes a value to msgpack format.
func Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	if err := enc.Encode(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Unmarshal decodes msgpack data using loose interface decoding.
// When decoding into interface{}, strings are preserved as Go strings (not []byte).
// This is essential for SQLite type matching.
func Unmarshal(data []byte, v interface{}) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	// UseLooseInterfaceDecoding converts []byte to string when decoding into interface{}
	// This is CRITICAL for SQLite which treats BLOB and TEXT as different types
	// for PRIMARY KEY comparison. Without this, INSERT OR REPLACE fails to find
	// existing rows because the PK value type doesn't match.
	dec.UseLooseInterfaceDecoding(true)

	return dec.Decode(v)
}
