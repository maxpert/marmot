package grpc

import "github.com/maxpert/marmot/encoding"

// SerializeParams converts []interface{} to [][]byte for proto transport.
// Each element is individually msgpack-encoded.
func SerializeParams(params []interface{}) ([][]byte, error) {
	if params == nil {
		return nil, nil
	}

	if len(params) == 0 {
		return [][]byte{}, nil
	}

	result := make([][]byte, len(params))
	for i, param := range params {
		data, err := encoding.Marshal(param)
		if err != nil {
			return nil, err
		}
		result[i] = data
	}

	return result, nil
}

// DeserializeParams converts [][]byte back to []interface{}.
func DeserializeParams(data [][]byte) ([]interface{}, error) {
	if data == nil {
		return nil, nil
	}

	if len(data) == 0 {
		return []interface{}{}, nil
	}

	result := make([]interface{}, len(data))
	for i, paramData := range data {
		var val interface{}
		if err := encoding.Unmarshal(paramData, &val); err != nil {
			return nil, err
		}
		result[i] = val
	}

	return result, nil
}
