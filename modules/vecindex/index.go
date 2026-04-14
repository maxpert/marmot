package vecindex

import (
	"context"
	"errors"
)

// Index is an open IVF vector index backed by a Pebble store.
type Index struct {
	spec IVFSpec
}

// Search returns up to req.K nearest neighbours for the given query vector.
func (i *Index) Search(ctx context.Context, req SearchRequest) ([]SearchHit, error) {
	return nil, errors.New("not implemented: Search")
}

// Upsert inserts or updates the vector associated with externalID.
// txnID and seqID advance the watermark for crash-recovery bookkeeping.
func (i *Index) Upsert(ctx context.Context, externalID []byte, vec []float32, txnID, seqID uint64) error {
	return errors.New("not implemented: Upsert")
}

// Delete removes the vector associated with externalID from the index.
func (i *Index) Delete(ctx context.Context, externalID []byte, txnID, seqID uint64) error {
	return errors.New("not implemented: Delete")
}

// Stats returns point-in-time statistics for this index.
func (i *Index) Stats() Stats {
	return Stats{}
}

// Close releases resources held by this index.
func (i *Index) Close() error {
	return errors.New("not implemented: Close")
}
