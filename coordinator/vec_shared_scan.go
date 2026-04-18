package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const (
	defaultVecSharedScanWindow           = 100 * time.Microsecond
	defaultVecSharedScanMaxRequests      = 8
	defaultVecSharedScanMaxUnionClusters = 64
)

// vecSharedScanKey identifies requests that can share a single sidecar scan.
// K is intentionally excluded so different top-k callers can co-batch.
type vecSharedScanKey struct {
	database   string
	indexName  string
	probeEpoch uint64
	rankMetric metric.Metric
	queryDim   int
}

func makeVecSharedScanKey(plan *GoRankPlan) (vecSharedScanKey, bool) {
	if plan == nil || plan.AllowCache || plan.HasUserPredicate || plan.K <= 0 || len(plan.QueryVec) == 0 || len(plan.CandidateArgFilter) != 0 {
		return vecSharedScanKey{}, false
	}
	return vecSharedScanKey{
		database:   plan.Database,
		indexName:  plan.IndexName,
		probeEpoch: plan.ProbeEpoch,
		rankMetric: plan.RankMetric,
		queryDim:   len(plan.QueryVec),
	}, true
}

type vecSharedScanOptions struct {
	MicrobatchWindow time.Duration
	MaxRequests      int
	MaxUnionClusters int
}

type vecSharedScanClusterScanner interface {
	ScanCluster(ctx context.Context, key vecSharedScanKey, clusterID int64, yield func(rowid int64, vecBytes []byte) error) error
}

type vecSharedScanBatchScanner interface {
	ScanClusters(ctx context.Context, key vecSharedScanKey, clusterIDs []int64, yield func(clusterID, rowid int64, vecBytes []byte) error) error
}

type vecSharedScanClusterScannerFunc func(ctx context.Context, key vecSharedScanKey, clusterID int64, yield func(rowid int64, vecBytes []byte) error) error

func (fn vecSharedScanClusterScannerFunc) ScanCluster(ctx context.Context, key vecSharedScanKey, clusterID int64, yield func(rowid int64, vecBytes []byte) error) error {
	return fn(ctx, key, clusterID, yield)
}

type vecSharedScanCoordinator struct {
	scanner          vecSharedScanClusterScanner
	microbatchWindow time.Duration
	maxRequests      int
	maxUnionClusters int

	mu     sync.Mutex
	active map[vecSharedScanKey]*vecSharedScanBatch
	stats  vecSharedScanCounters
}

var errVecSharedScanSkip = errors.New("shared scan skipped")

type VecSharedScanStats struct {
	ExecuteCalls                int64
	KeyRejects                  int64
	ProbeRefreshFallbacks       int64
	OversizedRequestFallbacks   int64
	BatchesStarted              int64
	BatchesCompleted            int64
	SharedBatches               int64
	SharedRequests              int64
	SingletonFallbacks          int64
	BatchRequestsTotal          int64
	BatchRequestedClustersTotal int64
	BatchUnionClustersTotal     int64
	ScanClusters                int64
	ScanRows                    int64
	SealByTimer                 int64
	SealByMaxRequests           int64
	SealByMaxUnion              int64
	MaxBatchSize                int64
	MaxUnionClusters            int64
}

type vecSharedScanCounters struct {
	executeCalls                atomic.Int64
	keyRejects                  atomic.Int64
	probeRefreshFallbacks       atomic.Int64
	oversizedRequestFallbacks   atomic.Int64
	batchesStarted              atomic.Int64
	batchesCompleted            atomic.Int64
	sharedBatches               atomic.Int64
	sharedRequests              atomic.Int64
	singletonFallbacks          atomic.Int64
	batchRequestsTotal          atomic.Int64
	batchRequestedClustersTotal atomic.Int64
	batchUnionClustersTotal     atomic.Int64
	scanClusters                atomic.Int64
	scanRows                    atomic.Int64
	sealByTimer                 atomic.Int64
	sealByMaxRequests           atomic.Int64
	sealByMaxUnion              atomic.Int64
	maxBatchSize                atomic.Int64
	maxUnionClusters            atomic.Int64
}

type vecSharedSealReason uint8

const (
	vecSharedSealNone vecSharedSealReason = iota
	vecSharedSealTimer
	vecSharedSealMaxRequests
	vecSharedSealMaxUnion
	vecSharedSealRetry
)

func newVecSharedScanCoordinator(scanner vecSharedScanClusterScanner, opts vecSharedScanOptions) *vecSharedScanCoordinator {
	window := opts.MicrobatchWindow
	if window <= 0 {
		window = defaultVecSharedScanWindow
	}
	maxRequests := opts.MaxRequests
	if maxRequests <= 0 {
		maxRequests = defaultVecSharedScanMaxRequests
	}
	maxUnionClusters := opts.MaxUnionClusters
	if maxUnionClusters <= 0 {
		maxUnionClusters = defaultVecSharedScanMaxUnionClusters
	}
	return &vecSharedScanCoordinator{
		scanner:          scanner,
		microbatchWindow: window,
		maxRequests:      maxRequests,
		maxUnionClusters: maxUnionClusters,
		active:           make(map[vecSharedScanKey]*vecSharedScanBatch),
	}
}

func (h *CoordinatorHandler) sharedScanRank(plan *GoRankPlan) ([]rankItem, bool, error) {
	if h == nil || !h.canUseSharedScan(plan) {
		return nil, false, nil
	}
	coord := h.sharedScanCoordinator()
	if refreshed := h.refreshProbeClusterIDs(plan); !sameClusterIDs(refreshed, plan.ClusterIDs) {
		coord.stats.probeRefreshFallbacks.Add(1)
		return nil, false, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	items, ok, err := coord.Execute(ctx, plan)
	return items, ok, err
}

func (h *CoordinatorHandler) sharedScanCoordinatorIfLoaded() *vecSharedScanCoordinator {
	if h == nil {
		return nil
	}
	if v, ok := h.vecSharedScanCoordinators.Load("default"); ok {
		return v.(*vecSharedScanCoordinator)
	}
	return nil
}

func (h *CoordinatorHandler) sharedScanCoordinator() *vecSharedScanCoordinator {
	if h == nil {
		return nil
	}
	if v, ok := h.vecSharedScanCoordinators.Load("default"); ok {
		return v.(*vecSharedScanCoordinator)
	}
	created := newVecSharedScanCoordinator(vecSharedSQLClusterScanner{handler: h}, vecSharedScanOptions{})
	actual, _ := h.vecSharedScanCoordinators.LoadOrStore("default", created)
	return actual.(*vecSharedScanCoordinator)
}

func (c *vecSharedScanCoordinator) Execute(ctx context.Context, plan *GoRankPlan) ([]rankItem, bool, error) {
	c.stats.executeCalls.Add(1)
	key, ok := makeVecSharedScanKey(plan)
	if !ok {
		c.stats.keyRejects.Add(1)
		return nil, false, nil
	}
	req, err := newVecSharedScanRequest(plan)
	if err != nil {
		return nil, true, err
	}
	items, err := c.submit(ctx, key, req)
	if errors.Is(err, errVecSharedScanSkip) {
		return nil, false, nil
	}
	return items, true, err
}

func (c *vecSharedScanCoordinator) submit(ctx context.Context, key vecSharedScanKey, req *vecSharedScanRequest) ([]rankItem, error) {
	if c == nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: shared scan coordinator is nil")
	}
	if c.scanner == nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: shared scan scanner is nil")
	}
	if req == nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: shared scan request is nil")
	}
	if len(req.clusterOrder) > c.maxUnionClusters {
		c.stats.oversizedRequestFallbacks.Add(1)
		return nil, errVecSharedScanSkip
	}

	for {
		c.mu.Lock()
		batch := c.active[key]
		if batch == nil {
			batch = newVecSharedScanBatch(c, key)
			c.active[key] = batch
		}
		joined, sealReason := batch.tryJoin(req)
		if joined {
			c.mu.Unlock()
			if sealReason != vecSharedSealNone {
				batch.sealAndRun(sealReason)
			}
			return req.wait(ctx)
		}
		if c.active[key] == batch {
			delete(c.active, key)
		}
		c.mu.Unlock()
		batch.sealAndRun(vecSharedSealRetry)
	}
}

func (c *vecSharedScanCoordinator) discardActive(batch *vecSharedScanBatch) {
	if c == nil || batch == nil {
		return
	}
	c.mu.Lock()
	if c.active[batch.key] == batch {
		delete(c.active, batch.key)
	}
	c.mu.Unlock()
}

type vecSharedScanBatch struct {
	coord *vecSharedScanCoordinator
	key   vecSharedScanKey

	mu           sync.Mutex
	timer        *time.Timer
	sealed       bool
	processOnce  sync.Once
	requests     []*vecSharedScanRequest
	unionOrder   []int64
	unionSet     map[int64]struct{}
	clusterRoute map[int64][]*vecSharedScanRequest
}

func newVecSharedScanBatch(coord *vecSharedScanCoordinator, key vecSharedScanKey) *vecSharedScanBatch {
	coord.stats.batchesStarted.Add(1)
	batch := &vecSharedScanBatch{
		coord:        coord,
		key:          key,
		unionSet:     make(map[int64]struct{}),
		clusterRoute: make(map[int64][]*vecSharedScanRequest),
	}
	batch.timer = time.AfterFunc(coord.microbatchWindow, func() {
		batch.sealAndRun(vecSharedSealTimer)
	})
	return batch
}

func (b *vecSharedScanBatch) tryJoin(req *vecSharedScanRequest) (joined bool, sealReason vecSharedSealReason) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed || len(b.requests) >= b.coord.maxRequests {
		return false, vecSharedSealRetry
	}

	addedClusters := 0
	for _, cid := range req.clusterOrder {
		if _, ok := b.unionSet[cid]; !ok {
			addedClusters++
		}
	}
	if len(b.unionOrder)+addedClusters > b.coord.maxUnionClusters {
		return false, vecSharedSealRetry
	}

	b.requests = append(b.requests, req)
	for _, cid := range req.clusterOrder {
		if _, ok := b.unionSet[cid]; !ok {
			b.unionSet[cid] = struct{}{}
			b.unionOrder = append(b.unionOrder, cid)
		}
		b.clusterRoute[cid] = append(b.clusterRoute[cid], req)
	}

	switch {
	case len(b.requests) >= b.coord.maxRequests:
		return true, vecSharedSealMaxRequests
	case len(b.unionOrder) >= b.coord.maxUnionClusters:
		return true, vecSharedSealMaxUnion
	default:
		return true, 0
	}
}

func (b *vecSharedScanBatch) sealAndRun(reason vecSharedSealReason) {
	b.processOnce.Do(func() {
		b.mu.Lock()
		if b.sealed {
			b.mu.Unlock()
			return
		}
		b.sealed = true
		timer := b.timer
		b.mu.Unlock()

		if timer != nil {
			timer.Stop()
		}
		switch reason {
		case vecSharedSealTimer:
			b.coord.stats.sealByTimer.Add(1)
		case vecSharedSealMaxRequests:
			b.coord.stats.sealByMaxRequests.Add(1)
		case vecSharedSealMaxUnion:
			b.coord.stats.sealByMaxUnion.Add(1)
		}

		b.coord.discardActive(b)
		b.run()
	})
}

func (b *vecSharedScanBatch) run() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b.coord.stats.batchesCompleted.Add(1)
	recordInt64Max(&b.coord.stats.maxBatchSize, int64(len(b.requests)))
	recordInt64Max(&b.coord.stats.maxUnionClusters, int64(len(b.unionOrder)))

	if len(b.requests) <= 1 {
		b.coord.stats.singletonFallbacks.Add(1)
		for _, req := range b.requests {
			req.finish(nil, errVecSharedScanSkip)
		}
		return
	}
	b.coord.stats.sharedBatches.Add(1)
	b.coord.stats.sharedRequests.Add(int64(len(b.requests)))
	b.coord.stats.batchRequestsTotal.Add(int64(len(b.requests)))
	var requestedClusters int64
	for _, req := range b.requests {
		requestedClusters += int64(len(req.clusterOrder))
	}
	b.coord.stats.batchRequestedClustersTotal.Add(requestedClusters)
	b.coord.stats.batchUnionClustersTotal.Add(int64(len(b.unionOrder)))
	b.coord.stats.scanClusters.Add(int64(len(b.unionOrder)))

	var runErr error
	if scanner, ok := b.coord.scanner.(vecSharedScanBatchScanner); ok {
		runErr = scanner.ScanClusters(ctx, b.key, b.unionOrder, func(cid, rowid int64, vecBytes []byte) error {
			b.coord.stats.scanRows.Add(1)
			if len(vecBytes) != b.key.queryDim*4 {
				return nil
			}
			for _, req := range b.clusterRoute[cid] {
				req.push(rowid, vecBytes, b.key.rankMetric)
			}
			return nil
		})
	} else {
		for _, cid := range b.unionOrder {
			reqs := b.clusterRoute[cid]
			if len(reqs) == 0 {
				continue
			}
			runErr = b.coord.scanner.ScanCluster(ctx, b.key, cid, func(rowid int64, vecBytes []byte) error {
				b.coord.stats.scanRows.Add(1)
				if len(vecBytes) != b.key.queryDim*4 {
					return nil
				}
				for _, req := range reqs {
					req.push(rowid, vecBytes, b.key.rankMetric)
				}
				return nil
			})
			if runErr != nil {
				break
			}
		}
	}

	for _, req := range b.requests {
		if runErr != nil {
			req.finish(nil, fmt.Errorf("MARMOT-VEC-030: shared cluster scan failed: %w", runErr))
			continue
		}
		req.finish(req.topK.Drain(), nil)
	}
}

type vecSharedScanRequest struct {
	queryVec     []float32
	topK         *topKHeap
	clusterOrder []int64
	resultCh     chan vecSharedScanResult
}

type vecSharedScanResult struct {
	items []rankItem
	err   error
}

type vecSharedSQLClusterScanner struct {
	handler *CoordinatorHandler
}

func (s vecSharedSQLClusterScanner) ScanClusters(ctx context.Context, key vecSharedScanKey, clusterIDs []int64, yield func(clusterID, rowid int64, vecBytes []byte) error) error {
	if s.handler == nil || s.handler.dbManager == nil {
		return fmt.Errorf("shared scan handler is not configured")
	}
	conn, err := s.handler.dbManager.GetDatabaseReadConnection(key.database)
	if err != nil {
		return fmt.Errorf("get db for shared scan: %w", err)
	}
	return scanVecSharedClusters(ctx, conn, key.indexName, clusterIDs, yield)
}

func (s vecSharedSQLClusterScanner) ScanCluster(ctx context.Context, key vecSharedScanKey, clusterID int64, yield func(rowid int64, vecBytes []byte) error) error {
	if s.handler == nil || s.handler.dbManager == nil {
		return fmt.Errorf("shared scan handler is not configured")
	}
	conn, err := s.handler.dbManager.GetDatabaseReadConnection(key.database)
	if err != nil {
		return fmt.Errorf("get db for shared scan: %w", err)
	}
	return scanVecSharedCluster(ctx, conn, key.indexName, clusterID, yield)
}

func newVecSharedScanRequest(plan *GoRankPlan) (*vecSharedScanRequest, error) {
	if plan == nil {
		return nil, fmt.Errorf("MARMOT-VEC-030: shared scan plan is nil")
	}
	if len(plan.QueryVec) == 0 {
		return nil, fmt.Errorf("MARMOT-VEC-030: shared scan query vector is empty")
	}
	if plan.K <= 0 {
		return nil, fmt.Errorf("MARMOT-VEC-030: shared scan top-k must be positive")
	}

	clusterOrder := make([]int64, 0, len(plan.ClusterIDs)+1)
	clusterSet := make(map[int64]struct{}, len(plan.ClusterIDs)+1)
	for _, cid := range clusterIDsWithDelta(plan.ClusterIDs) {
		if _, ok := clusterSet[cid]; ok {
			continue
		}
		clusterSet[cid] = struct{}{}
		clusterOrder = append(clusterOrder, cid)
	}

	return &vecSharedScanRequest{
		queryVec:     append([]float32(nil), plan.QueryVec...),
		topK:         newTopKHeap(plan.K),
		clusterOrder: clusterOrder,
		resultCh:     make(chan vecSharedScanResult, 1),
	}, nil
}

func (r *vecSharedScanRequest) push(rowid int64, vecBytes []byte, rankMetric metric.Metric) {
	switch rankMetric {
	case metric.MetricCosine:
		r.topK.Push(rowid, metric.CosineDistanceUnitFromBytes(r.queryVec, vecBytes))
	default:
		r.topK.Push(rowid, metric.DistanceFromBytes(rankMetric, r.queryVec, vecBytes))
	}
}

func (r *vecSharedScanRequest) finish(items []rankItem, err error) {
	r.resultCh <- vecSharedScanResult{items: items, err: err}
}

func (r *vecSharedScanRequest) wait(ctx context.Context) ([]rankItem, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-r.resultCh:
		return res.items, res.err
	}
}

func scanVecSharedCluster(ctx context.Context, conn *sql.DB, indexName string, clusterID int64, yield func(rowid int64, vecBytes []byte) error) error {
	if conn == nil {
		return fmt.Errorf("shared scan connection is nil")
	}
	rows, err := conn.QueryContext(
		ctx,
		fmt.Sprintf("SELECT `rowid`, `vec` FROM `%s` WHERE `cluster_id` = ? ORDER BY `rowid`", vecindex.MembersTable(indexName)),
		clusterID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rowid int64
		var vecBytes []byte
		if err := rows.Scan(&rowid, &vecBytes); err != nil {
			return err
		}
		if err := yield(rowid, vecBytes); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *vecSharedScanCoordinator) SnapshotStats() VecSharedScanStats {
	if c == nil {
		return VecSharedScanStats{}
	}
	return VecSharedScanStats{
		ExecuteCalls:                c.stats.executeCalls.Load(),
		KeyRejects:                  c.stats.keyRejects.Load(),
		ProbeRefreshFallbacks:       c.stats.probeRefreshFallbacks.Load(),
		OversizedRequestFallbacks:   c.stats.oversizedRequestFallbacks.Load(),
		BatchesStarted:              c.stats.batchesStarted.Load(),
		BatchesCompleted:            c.stats.batchesCompleted.Load(),
		SharedBatches:               c.stats.sharedBatches.Load(),
		SharedRequests:              c.stats.sharedRequests.Load(),
		SingletonFallbacks:          c.stats.singletonFallbacks.Load(),
		BatchRequestsTotal:          c.stats.batchRequestsTotal.Load(),
		BatchRequestedClustersTotal: c.stats.batchRequestedClustersTotal.Load(),
		BatchUnionClustersTotal:     c.stats.batchUnionClustersTotal.Load(),
		ScanClusters:                c.stats.scanClusters.Load(),
		ScanRows:                    c.stats.scanRows.Load(),
		SealByTimer:                 c.stats.sealByTimer.Load(),
		SealByMaxRequests:           c.stats.sealByMaxRequests.Load(),
		SealByMaxUnion:              c.stats.sealByMaxUnion.Load(),
		MaxBatchSize:                c.stats.maxBatchSize.Load(),
		MaxUnionClusters:            c.stats.maxUnionClusters.Load(),
	}
}

func (c *vecSharedScanCoordinator) Configure(opts vecSharedScanOptions) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if opts.MicrobatchWindow > 0 {
		c.microbatchWindow = opts.MicrobatchWindow
	}
	if opts.MaxRequests > 0 {
		c.maxRequests = opts.MaxRequests
	}
	if opts.MaxUnionClusters > 0 {
		c.maxUnionClusters = opts.MaxUnionClusters
	}
}

func (c *vecSharedScanCoordinator) ResetStats() {
	if c == nil {
		return
	}
	c.stats.executeCalls.Store(0)
	c.stats.keyRejects.Store(0)
	c.stats.probeRefreshFallbacks.Store(0)
	c.stats.oversizedRequestFallbacks.Store(0)
	c.stats.batchesStarted.Store(0)
	c.stats.batchesCompleted.Store(0)
	c.stats.sharedBatches.Store(0)
	c.stats.sharedRequests.Store(0)
	c.stats.singletonFallbacks.Store(0)
	c.stats.batchRequestsTotal.Store(0)
	c.stats.batchRequestedClustersTotal.Store(0)
	c.stats.batchUnionClustersTotal.Store(0)
	c.stats.scanClusters.Store(0)
	c.stats.scanRows.Store(0)
	c.stats.sealByTimer.Store(0)
	c.stats.sealByMaxRequests.Store(0)
	c.stats.sealByMaxUnion.Store(0)
	c.stats.maxBatchSize.Store(0)
	c.stats.maxUnionClusters.Store(0)
}

func recordInt64Max(dst *atomic.Int64, value int64) {
	for {
		cur := dst.Load()
		if value <= cur {
			return
		}
		if dst.CompareAndSwap(cur, value) {
			return
		}
	}
}

func scanVecSharedClusters(
	ctx context.Context,
	conn *sql.DB,
	indexName string,
	clusterIDs []int64,
	yield func(clusterID, rowid int64, vecBytes []byte) error,
) error {
	if conn == nil {
		return fmt.Errorf("shared scan connection is nil")
	}
	if len(clusterIDs) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("SELECT `cluster_id`, `rowid`, `vec` FROM `")
	sb.WriteString(vecindex.MembersTable(indexName))
	sb.WriteString("` WHERE `cluster_id` IN (")
	args := make([]interface{}, len(clusterIDs))
	for i, cid := range clusterIDs {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
		args[i] = cid
	}
	sb.WriteString(") ORDER BY `cluster_id`, `rowid`")

	rows, err := conn.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var clusterID, rowid int64
		var vecBytes []byte
		if err := rows.Scan(&clusterID, &rowid, &vecBytes); err != nil {
			return err
		}
		if err := yield(clusterID, rowid, vecBytes); err != nil {
			return err
		}
	}
	return rows.Err()
}

func sameClusterIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
