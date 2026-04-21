package db

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/rs/zerolog/log"
)

const (
	kmeansMaxIter = 25
	sampleFloor   = 200_000
)

func BulkPopulate(
	ctx context.Context,
	db *sql.DB,
	engine *vecindex.Engine,
	_ int64,
	tableName string,
	columnName string,
	spec vecindex.IVFSpec,
) error {
	cs, err := computeCentroids(ctx, db, tableName, columnName, spec)
	if err != nil {
		return fmt.Errorf("bulk populate %q: compute centroids: %w", spec.ID, err)
	}
	engine.RegisterWithCentroidSet(spec.ID, spec, cs)
	if err := setIndexReady(ctx, db, spec.ID); err != nil {
		engine.Unregister(spec.ID)
		return fmt.Errorf("bulk populate %q: set ready: %w", spec.ID, err)
	}
	if cs == nil {
		log.Info().Str("index", spec.ID).Msg("BulkPopulate: base table empty, index ready and awaiting automatic bootstrap")
		return nil
	}
	log.Info().Str("index", spec.ID).Uint64("epoch", cs.Epoch()).Int("nlist", cs.Len()).Msg("BulkPopulate: centroid set computed")
	return nil
}

func hasIndexableRows(
	ctx context.Context,
	db *sql.DB,
	tableName, columnName string,
	spec vecindex.IVFSpec,
) (bool, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			quoteIdent(columnName), quoteIdent(tableName), quoteIdent(columnName)),
	)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return false, err
		}
		mv, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return false, err
		}
		if mv != nil {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func setIndexReady(ctx context.Context, db *sql.DB, indexName string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE __marmot_vector_indexes SET status='ready' WHERE index_name=?`,
		indexName,
	)
	return err
}

func computeCentroids(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	columnName string,
	spec vecindex.IVFSpec,
) (*kmeans.CentroidSet, error) {
	tableQ := quoteIdent(tableName)
	colQ := quoteIdent(columnName)

	var nTotal int
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", tableQ, colQ)).Scan(&nTotal)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: count rows: %w", err)
	}
	if nTotal == 0 {
		return nil, nil
	}

	sampleCap := max(sampleFloor, 2*spec.Nlist)
	if sampleCap > nTotal {
		sampleCap = nTotal
	}

	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			colQ, tableQ, colQ),
	)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: sample query: %w", err)
	}
	defer rows.Close()

	rng := rand.New(rand.NewSource(int64(spec.Seed)))
	samples := make([][]float32, 0, sampleCap)
	seen := 0
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return nil, fmt.Errorf("compute centroids: scan blob: %w", err)
		}

		seen++
		slot := reservoirSlot(seen, sampleCap, rng)
		if slot < 0 {
			continue
		}

		raw, decErr := decodeVec(blob)
		if decErr != nil {
			return nil, fmt.Errorf("compute centroids: decode vector: %w", decErr)
		}
		v := make([]float32, len(raw))
		copy(v, raw)
		if spec.Metric == vecindex.MetricDot {
			augmented, augErr := metric.AugmentData(v, spec.MaxNorm, nil)
			if augErr != nil {
				return nil, fmt.Errorf("compute centroids: augment: %w", augErr)
			}
			v = augmented
		}

		if slot < len(samples) {
			samples[slot] = v
		} else {
			samples = append(samples, v)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compute centroids: row iteration: %w", err)
	}
	if len(samples) == 0 {
		return nil, nil
	}

	actualK := spec.Nlist
	if actualK > len(samples) {
		actualK = len(samples)
	}

	centroids, err := kmeans.KMeansPlusPlus(samples, actualK, spec.Seed, kmeansMaxIter)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: k-means: %w", err)
	}
	return kmeans.NewCentroidSet(1, centroids)
}

func assignPreparedAgainstSet(vecBytes []byte, spec vecindex.IVFSpec, cs *kmeans.CentroidSet) (int64, error) {
	if cs == nil || cs.Len() == 0 {
		return 0, vecindex.ErrNoCentroidsLoaded
	}
	if len(vecBytes) != spec.InternalDim()*4 {
		return 0, fmt.Errorf("prepared vector length %d does not match internal dim %d", len(vecBytes), spec.InternalDim())
	}
	clusterID, _, err := cs.AssignNearest(metric.BytesToFloat32(vecBytes), spec.InternalMetric())
	if err != nil {
		return 0, err
	}
	return int64(clusterID) + 1, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
