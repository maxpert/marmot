package core

import "github.com/fxamacker/cbor/v2"

var CBORTags = cbor.NewTagSet()

type ReplicableEvent[T any] interface {
	Wrap() (T, error)
	Unwrap() (T, error)
}
