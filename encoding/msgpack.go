// Package encoding provides centralized serialization/deserialization for Marmot.
// ALL msgpack operations MUST go through this package to ensure consistent behavior.
//
// Thread Safety: Marshal and Unmarshal are safe for concurrent use.
// They use sync.Pool internally for encoder/decoder reuse.
//
// Type Preservation: When decoding into interface{}, msgpack strings decode as
// Go strings (not []byte). This is critical for SQLite which treats BLOB and TEXT
// as different types for PRIMARY KEY comparison.
package encoding

import (
	"bytes"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// encoderPoolEntry provides pooled msgpack encoders for reduced allocations.
type encoderPoolEntry struct {
	buf *bytes.Buffer
	enc *msgpack.Encoder
}

// decoderPoolEntry provides pooled msgpack decoders with loose interface decoding.
type decoderPoolEntry struct {
	dec *msgpack.Decoder
}

var encoderPool = sync.Pool{
	New: func() interface{} {
		buf := new(bytes.Buffer)
		enc := msgpack.NewEncoder(buf)
		return &encoderPoolEntry{buf: buf, enc: enc}
	},
}

var decoderPool = sync.Pool{
	New: func() interface{} {
		dec := msgpack.NewDecoder(nil)
		// UseLooseInterfaceDecoding converts []byte to string when decoding into interface{}
		// This is CRITICAL for SQLite which treats BLOB and TEXT as different types
		// for PRIMARY KEY comparison. Without this, INSERT OR REPLACE fails to find
		// existing rows because the PK value type doesn't match.
		dec.UseLooseInterfaceDecoding(true)
		return &decoderPoolEntry{dec: dec}
	},
}

// Marshal encodes a value to msgpack format using a pooled encoder.
func Marshal(v interface{}) ([]byte, error) {
	entry := encoderPool.Get().(*encoderPoolEntry)
	entry.buf.Reset()

	if err := entry.enc.Encode(v); err != nil {
		encoderPool.Put(entry)
		return nil, err
	}

	// Copy result before returning to pool
	result := make([]byte, entry.buf.Len())
	copy(result, entry.buf.Bytes())
	encoderPool.Put(entry)

	return result, nil
}

// Unmarshal decodes msgpack data using loose interface decoding.
// When decoding into interface{}, strings are preserved as Go strings (not []byte).
// This is essential for SQLite type matching.
func Unmarshal(data []byte, v interface{}) error {
	entry := decoderPool.Get().(*decoderPoolEntry)
	entry.dec.Reset(bytes.NewReader(data))

	err := entry.dec.Decode(v)
	decoderPool.Put(entry)
	return err
}
