package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/rs/zerolog/log"
)

func retuneReindexMeta(meta common.VectorIndexMeta, spec vecindex.IVFSpec, rows int64) (common.VectorIndexMeta, vecindex.IVFSpec) {
	if meta.AutoTuneNlist {
		meta.Nlist = autoTuneNlist(rows)
		spec.Nlist = meta.Nlist
	}
	if meta.AutoTuneNprobe {
		meta.Nprobe = autoTuneNprobe(meta.Nlist)
		spec.Nprobe = meta.Nprobe
	}
	if meta.AutoTuneNlist || meta.AutoTuneNprobe {
		spec.Seed = StableIndexSeed(meta)
	}
	return meta, spec
}

func Reindex(
	ctx context.Context,
	db *sql.DB,
	engine *vecindex.Engine,
	meta common.VectorIndexMeta,
	_ int,
	_ int64,
) (common.VectorIndexMeta, *vecindex.IndexState, error) {
	state, ok := engine.Lookup(meta.IndexName)
	if !ok {
		return meta, nil, fmt.Errorf("MARMOT-VEC-013: vector index %q not registered in engine", meta.IndexName)
	}

	spec := state.Spec()
	currentN, err := countIndexableRows(ctx, db, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		return meta, nil, fmt.Errorf("reindex: count rows for tuning: %w", err)
	}
	meta, spec = retuneReindexMeta(meta, spec, currentN)

	oldEpoch := state.ProbeVersion()
	cs, err := computeCentroids(ctx, db, meta.TableName, meta.ColumnName, spec)
	if err != nil {
		return meta, nil, fmt.Errorf("reindex: compute centroids: %w", err)
	}
	if cs != nil {
		nextEpoch := oldEpoch + 1
		if nextEpoch == 0 {
			nextEpoch = 1
		}
		cs, err = kmeans.NewCentroidSet(nextEpoch, cs.Snapshot())
		if err != nil {
			return meta, nil, fmt.Errorf("reindex: re-epoch centroids: %w", err)
		}
	}

	newState := vecindex.NewIndexState(spec, cs)

	log.Info().
		Str("index", meta.IndexName).
		Uint64("old_epoch", oldEpoch).
		Uint64("new_epoch", newState.ProbeVersion()).
		Int64("rows", currentN).
		Msg("Reindex: prepared local generation")
	return meta, newState, nil
}

func countIndexableRows(
	ctx context.Context,
	db *sql.DB,
	tableName, columnName string,
	spec vecindex.IVFSpec,
) (int64, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid",
			quoteIdent(columnName), quoteIdent(tableName), quoteIdent(columnName)),
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return 0, err
		}
		mv, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return 0, err
		}
		if mv != nil {
			n++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return n, nil
}
