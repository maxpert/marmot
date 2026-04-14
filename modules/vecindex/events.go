package vecindex

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// Encode serialises the RetrainCluster event to msgpack bytes.
func (r RetrainCluster) Encode() ([]byte, error) {
	data, err := msgpack.Marshal(&r)
	if err != nil {
		return nil, fmt.Errorf("vecindex: encode RetrainCluster: %w", err)
	}
	return data, nil
}

// DecodeRetrainCluster deserialises a RetrainCluster from msgpack bytes.
func DecodeRetrainCluster(data []byte) (RetrainCluster, error) {
	var r RetrainCluster
	if err := msgpack.Unmarshal(data, &r); err != nil {
		return RetrainCluster{}, fmt.Errorf("vecindex: decode RetrainCluster: %w", err)
	}
	return r, nil
}
