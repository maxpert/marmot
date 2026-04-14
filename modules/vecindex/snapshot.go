package vecindex

import (
	"context"
	"errors"
	"io"
)

// SnapshotIndex writes a binary snapshot of the index identified by id to w.
func (e *Engine) SnapshotIndex(ctx context.Context, id string, w io.Writer) error {
	return errors.New("not implemented: SnapshotIndex")
}

// RestoreIndex reads a snapshot from r and restores it as the index identified
// by id, replacing any existing data for that id.
func RestoreIndex(ctx context.Context, id string, r io.Reader) error {
	return errors.New("not implemented: RestoreIndex")
}
