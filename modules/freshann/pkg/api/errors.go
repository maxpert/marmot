package api

import "errors"

var (
	ErrInvalidSpec       = errors.New("invalid index spec")
	ErrInvalidMutation   = errors.New("invalid mutation")
	ErrIndexExists       = errors.New("index already exists")
	ErrIndexNotFound     = errors.New("index not found")
	ErrClosed            = errors.New("engine/index is closed")
	ErrNotAppliedYet     = errors.New("token not applied yet")
	ErrUnsupportedMetric = errors.New("unsupported metric")
	ErrUnsupportedFormat = errors.New("unsupported index format")
)
