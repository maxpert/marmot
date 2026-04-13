package main

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	hdindex "github.com/maxpert/marmot/modules/hdindex"
)

// hdindexAdapter bridges hdindex.Engine to db.VectorIndexEngine.
type hdindexAdapter struct {
	engine *hdindex.Engine
}

func newHDIndexAdapter(rootDir string) (*hdindexAdapter, error) {
	engine, err := hdindex.NewEngine(rootDir)
	if err != nil {
		return nil, err
	}
	return &hdindexAdapter{engine: engine}, nil
}

func (a *hdindexAdapter) CreateIndex(ctx context.Context, id string, dim int, metric string, vectors []db.VectorBulkEntry) (db.VectorIndex, error) {
	m, ok := hdindex.ParseMetric(metric)
	if !ok {
		return nil, fmt.Errorf("unknown metric: %s", metric)
	}
	spec := hdindex.DefaultSpec(id, dim, m)

	entries := make([]hdindex.VectorEntry, len(vectors))
	for i, v := range vectors {
		entries[i] = hdindex.VectorEntry{ExternalID: v.ExternalID, Vector: v.Vector}
	}

	idx, err := a.engine.CreateIndex(ctx, spec, entries)
	if err != nil {
		return nil, err
	}
	return &hdindexIndexAdapter{idx: idx}, nil
}

func (a *hdindexAdapter) OpenIndex(ctx context.Context, id string) (db.VectorIndex, error) {
	idx, err := a.engine.OpenIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	return &hdindexIndexAdapter{idx: idx}, nil
}

func (a *hdindexAdapter) DropIndex(ctx context.Context, id string) error {
	return a.engine.DropIndex(ctx, id)
}

func (a *hdindexAdapter) Close() error {
	return a.engine.Close()
}

// hdindexIndexAdapter bridges hdindex.Index to db.VectorIndex.
type hdindexIndexAdapter struct {
	idx *hdindex.Index
}

func (a *hdindexIndexAdapter) Search(ctx context.Context, vector []float32, topK int) ([]common.VectorSearchHit, error) {
	result, err := a.idx.Search(ctx, hdindex.SearchRequest{VectorFP32: vector, TopK: topK})
	if err != nil {
		return nil, err
	}
	hits := make([]common.VectorSearchHit, len(result.Hits))
	for i, h := range result.Hits {
		hits[i] = common.VectorSearchHit{ExternalID: h.ExternalID, Distance: h.Distance, Score: h.Score}
	}
	return hits, nil
}

func (a *hdindexIndexAdapter) Upsert(ctx context.Context, externalID []byte, vector []float32, txnID, seqID uint64) error {
	return a.idx.Upsert(ctx, hdindex.Mutation{TxnID: txnID, SeqID: seqID, ExternalID: externalID, VectorFP32: vector})
}

func (a *hdindexIndexAdapter) Delete(ctx context.Context, externalID []byte, txnID, seqID uint64) error {
	return a.idx.Delete(ctx, hdindex.DeleteMutation{TxnID: txnID, SeqID: seqID, ExternalID: externalID})
}

func (a *hdindexIndexAdapter) Stats() db.VectorIndexStats {
	s := a.idx.Stats()
	return db.VectorIndexStats{VectorCount: s.VectorCount, WatermarkTxnID: s.WatermarkTxnID}
}

func (a *hdindexIndexAdapter) Close() error {
	return a.idx.Close()
}
