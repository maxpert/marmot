package lib

import "github.com/fxamacker/cbor/v2"

type ReplicationEvent[T any] struct {
	FromNodeId uint64
	Payload    *T
}

func (e *ReplicationEvent[T]) Marshal() ([]byte, error) {
	return cbor.Marshal(e)
}

func (e *ReplicationEvent[T]) Unmarshal(data []byte) error {
	return cbor.Unmarshal(data, e)
}
