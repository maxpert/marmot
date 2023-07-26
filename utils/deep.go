package utils

import (
	"bytes"
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

func DeepCopy(dst, src any) error {
	var buf bytes.Buffer
	if err := cbor.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return cbor.NewDecoder(&buf).Decode(dst)
}

func DeepEqualArray[T any](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}

	return true
}
