package protocol

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

var encoderPool = sync.Pool{
	New: func() interface{} {
		buf := new(bytes.Buffer)
		enc := msgpack.NewEncoder(buf)
		return &encoderPoolEntry{buf: buf, enc: enc}
	},
}

// MarshalMsgpack encodes a value using a pooled encoder.
// Returns the encoded bytes.
func MarshalMsgpack(v interface{}) ([]byte, error) {
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
