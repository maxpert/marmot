package utils

import (
	"bytes"
	"encoding/gob"
	"reflect"
)

func DeepCopy(dst, src any) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(&buf).Decode(dst)
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
