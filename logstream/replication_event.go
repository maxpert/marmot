package logstream

import (
	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/marmot/core"
)

type ReplicationEvent[T core.ReplicableEvent[T]] struct {
	FromNodeId uint64
	Payload    T
}

func (e *ReplicationEvent[T]) Marshal() ([]byte, error) {
	wrappedPayload, err := e.Payload.Wrap()
	if err != nil {
		return nil, err
	}

	ev := ReplicationEvent[T]{
		FromNodeId: e.FromNodeId,
		Payload:    wrappedPayload,
	}

	em, err := cbor.EncOptions{}.EncModeWithTags(core.CBORTags)
	if err != nil {
		return nil, err
	}

	return em.Marshal(ev)
}

func (e *ReplicationEvent[T]) Unmarshal(data []byte) error {
	dm, err := cbor.DecOptions{}.DecModeWithTags(core.CBORTags)
	if err != nil {
		return nil
	}

	err = dm.Unmarshal(data, e)
	if err != nil {
		return err
	}

	e.Payload, err = e.Payload.Unwrap()
	if err != nil {
		return err
	}

	return nil
}
