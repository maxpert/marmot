package vecindex

import "errors"

// Encode serialises the RetrainCluster event to msgpack bytes.
func (r RetrainCluster) Encode() ([]byte, error) {
	return nil, errors.New("not implemented: RetrainCluster.Encode")
}

// DecodeRetrainCluster deserialises a RetrainCluster from msgpack bytes.
func DecodeRetrainCluster(data []byte) (RetrainCluster, error) {
	return RetrainCluster{}, errors.New("not implemented: DecodeRetrainCluster")
}
