package db

import (
	"fmt"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

// vectorUDFProvider is the process-wide provider used by per-connection
// UDFs that must reach back into the vector-index engine. It is set once
// at startup via SetVectorUDFProvider before any SQLite connection is
// opened, and read without locking on the hot path via atomic.Pointer.
var vectorUDFProvider atomic.Pointer[vecindex.VectorUDFProvider]

// SetVectorUDFProvider installs the engine implementation used by the
// `__marmot_vec_assign` and `__marmot_vec_notify_centroid_change` UDFs.
// A nil argument clears the provider. With no provider installed,
// `__marmot_vec_assign` errors with MARMOT-VEC-013 (it cannot produce a
// cluster id without the engine) while `__marmot_vec_notify_centroid_change`
// is best-effort and silently returns 0 so replica cold-start writes are
// never aborted by a missing bootstrap dependency. Safe for concurrent use.
func SetVectorUDFProvider(p vecindex.VectorUDFProvider) {
	if p == nil {
		vectorUDFProvider.Store(nil)
		return
	}
	vectorUDFProvider.Store(&p)
}

func loadVectorUDFProvider() vecindex.VectorUDFProvider {
	if p := vectorUDFProvider.Load(); p != nil {
		return *p
	}
	return nil
}

// RegisterVectorUDFs registers the v5.4 vector-search UDFs on a single
// SQLite connection. It is intended to be called from the driver's
// ConnectHook (see sqlite_driver.go) alongside regexp and MySQL compat
// registration. The engine argument may be nil during bootstrap; the
// side-effect UDFs will error out until an engine is installed.
func RegisterVectorUDFs(conn *sqlite3.SQLiteConn) error {
	funcs := []struct {
		name string
		impl interface{}
		pure bool
	}{
		{"vec_distance_l2", vecDistanceL2, true},
		{"vec_distance_cosine", vecDistanceCosine, true},
		{"vec_distance_dot", vecDistanceDot, true},
		{"__marmot_vec_assign", vecAssign, false},
		{"__marmot_vec_notify_centroid_change", vecNotifyCentroidChange, false},
		{"vec_match", vecMatchSentinel, true},
	}
	for _, f := range funcs {
		if err := conn.RegisterFunc(f.name, f.impl, f.pure); err != nil {
			return fmt.Errorf("register %s: %w", f.name, err)
		}
	}
	return nil
}

// decodeVec reinterprets a little-endian float32-packed BLOB as a
// []float32 without copying. The returned slice must not be retained
// beyond the lifetime of b. Rejects empty or non-multiple-of-4 inputs.
func decodeVec(b []byte) ([]float32, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("MARMOT-VEC-014: empty vector blob")
	}
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("MARMOT-VEC-014: vector blob length %d is not a multiple of 4", len(b))
	}
	return metric.BytesToFloat32(b), nil
}

func distanceArgs(a, b []byte) ([]float32, []float32, error) {
	va, err := decodeVec(a)
	if err != nil {
		return nil, nil, err
	}
	vb, err := decodeVec(b)
	if err != nil {
		return nil, nil, err
	}
	if len(va) != len(vb) {
		return nil, nil, fmt.Errorf("MARMOT-VEC-014: vector dimension mismatch: %d vs %d", len(va), len(vb))
	}
	return va, vb, nil
}

func vecDistanceL2(a, b []byte) (float64, error) {
	va, vb, err := distanceArgs(a, b)
	if err != nil {
		return 0, err
	}
	return float64(metric.L2Squared(va, vb)), nil
}

func vecDistanceCosine(a, b []byte) (float64, error) {
	va, vb, err := distanceArgs(a, b)
	if err != nil {
		return 0, err
	}
	return float64(1 - metric.CosineSimilarity(va, vb)), nil
}

func vecDistanceDot(a, b []byte) (float64, error) {
	va, vb, err := distanceArgs(a, b)
	if err != nil {
		return 0, err
	}
	return float64(-metric.DotProduct(va, vb)), nil
}

func vecAssign(indexName string, vec []byte) (int64, error) {
	p := loadVectorUDFProvider()
	if p == nil {
		return 0, fmt.Errorf("MARMOT-VEC-013: vector index engine not initialised")
	}
	if len(vec) == 0 || len(vec)%4 != 0 {
		return 0, fmt.Errorf("MARMOT-VEC-014: invalid vector blob length %d", len(vec))
	}
	return p.AssignNearest(indexName, vec)
}

// vecNotifyCentroidChange is a best-effort side-effect UDF invoked from
// AFTER INSERT / AFTER UPDATE triggers on the centroids table. It MUST
// NEVER abort the writer transaction:
//   - No provider installed (cold start or test) → silently return 0.
//   - Provider returns an error → swallow it.
//
// The engine's own retrain loop is the source of truth; the trigger only
// exists to wake the engine up early when a replicated centroid row lands.
// Contract: non-blocking, always returns (0, nil).
func vecNotifyCentroidChange(indexName string, version int64) (int64, error) {
	if p := loadVectorUDFProvider(); p != nil {
		_ = p.NotifyCentroidChange(indexName, version)
	}
	return 0, nil
}

// vecMatchSentinel is a placeholder implementation: the coordinator
// transpiler rewrites vec_match away before SQLite ever sees it. If it
// reaches this UDF, something is misconfigured — fail loudly.
func vecMatchSentinel(vec, query []byte, k int64) (int64, error) {
	return 0, fmt.Errorf("MARMOT-VEC-010: vec_match must be transpiled by coordinator; direct invocation not supported")
}
