package db

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/rs/zerolog/log"
)

func BulkPopulate(
	ctx context.Context,
	db *sql.DB,
	engine *vecindex.Engine,
	_ int64,
	tableName string,
	columnName string,
	spec vecindex.IVFSpec,
	targetPartitionSize int,
) error {
	cs, err := computeCentroids(ctx, db, tableName, columnName, spec, targetPartitionSize, nil)
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
	targetClusterSize int,
	initCentroids [][]float32,
) (*kmeans.CentroidSet, error) {
	tableQ := quoteIdent(tableName)
	colQ := quoteIdent(columnName)

	nTotal64, err := countIndexableRows(ctx, db, tableName, columnName, spec)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: count indexable rows: %w", err)
	}
	nTotal := int(nTotal64)
	if nTotal == 0 {
		return nil, nil
	}

	actualK := spec.Nlist
	if actualK > nTotal {
		actualK = nTotal
	}
	if targetClusterSize <= 0 {
		targetClusterSize = max(1, (nTotal+actualK-1)/actualK)
	}

	opts := kmeans.MiniBatchBalancedOptions{
		BatchSize:         min(max(4096, targetClusterSize*4), 16384),
		MaxIter:           kmeans.DefaultMiniBatchMaxIter,
		TargetClusterSize: targetClusterSize,
	}
	if len(initCentroids) > actualK {
		initCentroids = initCentroids[:actualK]
	}
	initSize := max(actualK*32, opts.BatchSize*kmeans.DefaultMiniBatchInitFactor)
	if initSize < actualK {
		initSize = actualK
	}
	if initSize > nTotal {
		initSize = nTotal
	}
	samples, err := collectMaterializedReservoirSample(ctx, db, tableQ, colQ, spec, initSize, spec.Seed)
	if err != nil {
		return nil, err
	}
	if len(initCentroids) == 0 {
		if len(samples) == 0 {
			return nil, nil
		}
		initCentroids, err = kmeans.KMeansPlusPlus(samples, actualK, spec.Seed, 1)
		if err != nil {
			return nil, fmt.Errorf("compute centroids: init mini-batch centroids: %w", err)
		}
	} else if len(initCentroids) < actualK {
		initCentroids, err = supplementMiniBatchCentroids(initCentroids, samples, actualK, spec.Seed)
		if err != nil {
			return nil, fmt.Errorf("compute centroids: extend warm start: %w", err)
		}
	}
	initCentroids, err = kmeans.RebalanceInitialCentroids(samples, initCentroids, opts, spec.Seed)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: rebalance init centroids: %w", err)
	}
	trainer, err := kmeans.NewMiniBatchBalancedTrainer(initCentroids, opts)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: init trainer: %w", err)
	}
	stablePasses := 0
	var lastResult kmeans.MiniBatchPassResult
	var bestAssignedCentroids [][]float32
	bestAssignedShift := float32(math.MaxFloat32)
	var bestCentroids [][]float32
	bestShift := float32(math.MaxFloat32)
	for iter := 0; iter < opts.MaxIter; iter++ {
		result, err := runMiniBatchTrainerPass(ctx, db, tableQ, colQ, spec, opts.BatchSize, trainer, spec.Seed^uint64(iter+1))
		if err != nil {
			return nil, err
		}
		lastResult = result
		shapeOK := trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize)
		if !result.Repaired && result.MaxShift < bestAssignedShift {
			bestAssignedCentroids = trainer.Centroids()
			bestAssignedShift = result.MaxShift
		}
		if !result.Repaired && shapeOK && result.MaxShift < bestShift {
			bestCentroids = trainer.Centroids()
			bestShift = result.MaxShift
		}
		if result.Repaired || !result.Converged || !shapeOK {
			stablePasses = 0
			continue
		}
		stablePasses++
		if stablePasses >= 2 {
			break
		}
	}
	for extra := 0; extra < 4 && (lastResult.Repaired || !trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize)); extra++ {
		result, err := runMiniBatchTrainerPass(ctx, db, tableQ, colQ, spec, opts.BatchSize, trainer, spec.Seed^uint64(opts.MaxIter+extra+1))
		if err != nil {
			return nil, err
		}
		lastResult = result
		if !result.Repaired && result.MaxShift < bestAssignedShift {
			bestAssignedCentroids = trainer.Centroids()
			bestAssignedShift = result.MaxShift
		}
		if !result.Repaired && trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize) && result.MaxShift < bestShift {
			bestCentroids = trainer.Centroids()
			bestShift = result.MaxShift
		}
	}
	if lastResult.Repaired || !trainerClusterShapeAcceptable(trainer.Counts(), targetClusterSize) {
		if len(bestCentroids) > 0 {
			return kmeans.NewCentroidSet(1, bestCentroids)
		}
		if len(bestAssignedCentroids) == 0 {
			return nil, fmt.Errorf("compute centroids: mini-batch trainer failed to converge to an assigned layout")
		}
		return kmeans.NewCentroidSet(1, bestAssignedCentroids)
	}
	return kmeans.NewCentroidSet(1, trainer.Centroids())
}

func runMiniBatchTrainerPass(
	ctx context.Context,
	db *sql.DB,
	tableQ, colQ string,
	spec vecindex.IVFSpec,
	batchSize int,
	trainer *kmeans.MiniBatchBalancedTrainer,
	seed uint64,
) (kmeans.MiniBatchPassResult, error) {
	if err := trainer.BeginPass(); err != nil {
		return kmeans.MiniBatchPassResult{}, fmt.Errorf("compute centroids: start trainer pass: %w", err)
	}
	batch := make([][]float32, 0, batchSize)
	if err := scanMaterializedVectors(ctx, db, tableQ, colQ, spec, func(vec []float32) error {
		batch = append(batch, vec)
		if len(batch) < batchSize {
			return nil
		}
		if err := trainer.ObserveBatch(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}); err != nil {
		return kmeans.MiniBatchPassResult{}, err
	}
	if len(batch) > 0 {
		if err := trainer.ObserveBatch(batch); err != nil {
			return kmeans.MiniBatchPassResult{}, fmt.Errorf("compute centroids: flush trainer batch: %w", err)
		}
	}
	result, err := trainer.EndPass(seed)
	if err != nil {
		return kmeans.MiniBatchPassResult{}, fmt.Errorf("compute centroids: finish trainer pass: %w", err)
	}
	return result, nil
}

func trainerClusterShapeAcceptable(counts []int64, targetClusterSize int) bool {
	if len(counts) == 0 || targetClusterSize <= 0 {
		return true
	}
	var nonzero []int64
	var maxCount int64
	for _, count := range counts {
		if count <= 0 {
			continue
		}
		nonzero = append(nonzero, count)
		if count > maxCount {
			maxCount = count
		}
	}
	if len(nonzero) != len(counts) {
		return false
	}
	slices.Sort(nonzero)
	p95 := nonzero[(len(nonzero)*95+99)/100-1]
	if maxCount > int64(targetClusterSize*2) {
		return false
	}
	return p95 <= int64(float64(targetClusterSize)*repairP95Factor)
}

func supplementMiniBatchCentroids(initCentroids [][]float32, samples [][]float32, want int, seed uint64) ([][]float32, error) {
	if len(initCentroids) >= want {
		return initCentroids[:want], nil
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("no sample vectors available to extend warm start from %d to %d centroids", len(initCentroids), want)
	}
	centroids := make([][]float32, len(initCentroids), want)
	for i, centroid := range initCentroids {
		cp := make([]float32, len(centroid))
		copy(cp, centroid)
		centroids[i] = cp
	}
	used := make(map[int]struct{}, want-len(initCentroids))
	for len(centroids) < want {
		best := -1
		bestScore := float32(-1)
		for _, sampleIdx := range kmeansOrder(len(samples), seed^uint64(len(centroids))) {
			if _, ok := used[sampleIdx]; ok {
				continue
			}
			score := minDistanceToCentroids(samples[sampleIdx], centroids)
			if best == -1 || score > bestScore {
				best = sampleIdx
				bestScore = score
			}
		}
		if best < 0 {
			return nil, fmt.Errorf("insufficient distinct sample vectors to extend warm start to %d centroids", want)
		}
		cp := make([]float32, len(samples[best]))
		copy(cp, samples[best])
		centroids = append(centroids, cp)
		used[best] = struct{}{}
	}
	return centroids, nil
}

func minDistanceToCentroids(vec []float32, centroids [][]float32) float32 {
	best := float32(0)
	initialized := false
	for _, centroid := range centroids {
		var dist float32
		for i := range vec {
			diff := vec[i] - centroid[i]
			dist += diff * diff
		}
		if !initialized || dist < best {
			best = dist
			initialized = true
		}
	}
	if !initialized {
		return 0
	}
	return best
}

func kmeansOrder(n int, seed uint64) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	if n <= 1 {
		return order
	}
	rng := rand.New(rand.NewSource(int64(seed ^ uint64(n))))
	rng.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
	return order
}

func collectMaterializedReservoirSample(
	ctx context.Context,
	db *sql.DB,
	tableQ, colQ string,
	spec vecindex.IVFSpec,
	sampleCap int,
	seed uint64,
) ([][]float32, error) {
	if sampleCap <= 0 {
		return nil, nil
	}
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid", colQ, tableQ, colQ),
	)
	if err != nil {
		return nil, fmt.Errorf("compute centroids: sample query: %w", err)
	}
	defer rows.Close()

	rng := rand.New(rand.NewSource(int64(seed)))
	samples := make([][]float32, 0, sampleCap)
	seen := 0
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return nil, fmt.Errorf("compute centroids: scan blob: %w", err)
		}
		prepared, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return nil, fmt.Errorf("compute centroids: materialize sample: %w", err)
		}
		if prepared == nil {
			continue
		}
		seen++
		slot := reservoirSlot(seen, sampleCap, rng)
		if slot < 0 {
			continue
		}
		vec := append([]float32(nil), metric.BytesToFloat32(prepared)...)
		if slot < len(samples) {
			samples[slot] = vec
		} else {
			samples = append(samples, vec)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compute centroids: sample row iteration: %w", err)
	}
	return samples, nil
}

func scanMaterializedVectors(
	ctx context.Context,
	db *sql.DB,
	tableQ, colQ string,
	spec vecindex.IVFSpec,
	visit func(vec []float32) error,
) error {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY rowid", colQ, tableQ, colQ),
	)
	if err != nil {
		return fmt.Errorf("compute centroids: training query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return fmt.Errorf("compute centroids: training scan: %w", err)
		}
		prepared, err := materializeVectorBlob(blob, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return fmt.Errorf("compute centroids: materialize training vector: %w", err)
		}
		if prepared == nil {
			continue
		}
		if err := visit(append([]float32(nil), metric.BytesToFloat32(prepared)...)); err != nil {
			return fmt.Errorf("compute centroids: trainer observe: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("compute centroids: training row iteration: %w", err)
	}
	return nil
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
