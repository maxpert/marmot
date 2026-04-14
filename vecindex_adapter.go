package main

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/rs/zerolog"
)

type vecEngineAdapter struct {
	engine *vecindex.Engine
}

type vecIndexAdapter struct {
	idx *vecindex.Index
}

func newVecIndexAdapter(rootDir string, logger zerolog.Logger) (db.VectorIndexEngine, error) {
	eng, err := vecindex.NewEngine(rootDir, logger)
	if err != nil {
		return nil, fmt.Errorf("vecindex engine: %w", err)
	}
	return &vecEngineAdapter{engine: eng}, nil
}

func parseVecMetric(metric string) (vecindex.Metric, error) {
	switch metric {
	case "", "l2", "euclidean":
		return vecindex.MetricL2, nil
	case "dot", "ip":
		return vecindex.MetricDot, nil
	case "cosine":
		return vecindex.MetricCosine, nil
	default:
		return 0, fmt.Errorf("vecindex: unknown metric %q", metric)
	}
}

func (a *vecEngineAdapter) CreateIndex(ctx context.Context, id string, dim int, metric string, vectors []db.VectorBulkEntry) (db.VectorIndex, error) {
	m, err := parseVecMetric(metric)
	if err != nil {
		return nil, err
	}
	spec := vecindex.DefaultSpec(id, dim, m)
	bulk := make([]vecindex.BulkEntry, len(vectors))
	for i, v := range vectors {
		bulk[i] = vecindex.BulkEntry{ExternalID: v.ExternalID, Vector: v.Vector}
	}
	idx, err := a.engine.CreateIndex(ctx, spec, bulk)
	if err != nil {
		return nil, err
	}
	return &vecIndexAdapter{idx: idx}, nil
}

func (a *vecEngineAdapter) OpenIndex(ctx context.Context, id string) (db.VectorIndex, error) {
	idx, err := a.engine.OpenIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	return &vecIndexAdapter{idx: idx}, nil
}

func (a *vecEngineAdapter) DropIndex(ctx context.Context, id string) error {
	return a.engine.DropIndex(ctx, id)
}

func (a *vecEngineAdapter) Close() error {
	return a.engine.Close()
}

func (a *vecIndexAdapter) Search(ctx context.Context, vector []float32, topK int) ([]db.VectorSearchHit, error) {
	hits, err := a.idx.Search(ctx, vecindex.SearchRequest{Vector: vector, K: topK})
	if err != nil {
		return nil, err
	}
	result := make([]db.VectorSearchHit, len(hits))
	for i, h := range hits {
		result[i] = db.VectorSearchHit{ExternalID: h.ExternalID, Distance: h.Distance}
	}
	return result, nil
}

func (a *vecIndexAdapter) Upsert(ctx context.Context, externalID []byte, vector []float32, txnID, seqID uint64) error {
	return a.idx.Upsert(ctx, externalID, vector, txnID, seqID)
}

func (a *vecIndexAdapter) Delete(ctx context.Context, externalID []byte, txnID, seqID uint64) error {
	return a.idx.Delete(ctx, externalID, txnID, seqID)
}

func (a *vecIndexAdapter) Stats() db.VectorIndexStats {
	s := a.idx.Stats()
	return db.VectorIndexStats{VectorCount: s.VectorCount, WatermarkTxnID: s.WatermarkTxnID}
}

func (a *vecIndexAdapter) Close() error {
	return nil
}
