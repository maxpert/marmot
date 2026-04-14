package main

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/rs/zerolog"
)

// scoreFromDistance converts a raw distance value to a score in (0,1].
// For L2 and Cosine, distance >= 0, so score = 1/(1+d).
// For Dot, distance = -dot(a,b) and may be negative when vectors are similar;
// we clamp so that any negative distance (high similarity) maps to score=1.0.
func scoreFromDistance(m vecindex.Metric, d float32) float32 {
	if m == vecindex.MetricDot && d < 0 {
		return 1.0
	}
	return 1.0 / (1.0 + d)
}

type vecEngineAdapter struct {
	engine *vecindex.Engine
}

type vecIndexAdapter struct {
	idx    *vecindex.Index
	metric vecindex.Metric
}

func newVecIndexAdapter(rootDir string, cacheMB int, logger zerolog.Logger) (db.VectorIndexEngine, error) {
	eng, err := vecindex.NewEngine(rootDir, cacheMB, logger)
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
	return &vecIndexAdapter{idx: idx, metric: m}, nil
}

func (a *vecEngineAdapter) OpenIndex(ctx context.Context, id string) (db.VectorIndex, error) {
	idx, err := a.engine.OpenIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	sp, _ := a.engine.SpecOf(id)
	return &vecIndexAdapter{idx: idx, metric: sp.Metric}, nil
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
		result[i] = db.VectorSearchHit{
			ExternalID: h.ExternalID,
			Distance:   h.Distance,
			Score:      scoreFromDistance(a.metric, h.Distance),
		}
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
