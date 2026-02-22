package freshann

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/ristretto"
	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	annbudget "github.com/maxpert/marmot/modules/freshann/pkg/budget"
	annfilter "github.com/maxpert/marmot/modules/freshann/pkg/filter"
	"github.com/maxpert/marmot/modules/freshann/pkg/graph"
	"github.com/maxpert/marmot/modules/freshann/pkg/query"
	"github.com/maxpert/marmot/modules/freshann/pkg/repair"
	"github.com/maxpert/marmot/modules/freshann/pkg/segment"
	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
	annverify "github.com/maxpert/marmot/modules/freshann/pkg/verify"
)

var iterateAllVectorsForSearch = func(store *storage.IndexStore, fn func([]byte, storage.VectorRecord) error) error {
	return store.IterateVectors(fn)
}

type mutationOp string

const (
	mutationUpsert mutationOp = "upsert"
	mutationDelete mutationOp = "delete"
)

type mutationEnvelope struct {
	Op           mutationOp
	Token        ApplyToken
	ExternalID   []byte
	VectorFP32   []float32
	PartitionKey string
	Tags         map[string]string
}

type index struct {
	dir  string
	spec IndexSpec

	store    *storage.IndexStore
	manifest segment.Manifest

	graphIdx *graph.Index

	repairQ             *repair.Queue
	graphDirty          bool
	mutationsSinceBuild int
	compactThreshold    int
	lastCheckpointAt    time.Time
	minCheckpointDelay  time.Duration
	pendingMu           sync.RWMutex
	pendingIDs          map[uint64]struct{}

	applyMu sync.Mutex
	mu      sync.RWMutex
	closed  bool

	notify chan struct{}

	coarse *coarseIndex
	cache  *ristretto.Cache

	budgetResolver annbudget.Resolver

	fallbackScans    uint64
	graphPageReads   uint64
	vectorBlockReads uint64
}

type coarseIndex struct {
	mu             sync.RWMutex
	metric         Metric
	maxCentroids   int
	maxListSize    int
	centroids      [][]float32
	centroidCounts []int
	lists          [][]uint64
	nextReplace    []int
	docToCentroid  map[uint64]int
}

func newCoarseIndex(metric Metric, maxCentroids int, maxListSize int) *coarseIndex {
	if maxCentroids <= 0 {
		maxCentroids = 2048
	}
	if maxListSize <= 0 {
		maxListSize = 512
	}
	return &coarseIndex{
		metric:        metric,
		maxCentroids:  maxCentroids,
		maxListSize:   maxListSize,
		docToCentroid: make(map[uint64]int),
	}
}

func removeDocID(ids []uint64, needle uint64) []uint64 {
	for i := range ids {
		if ids[i] == needle {
			ids[i] = ids[len(ids)-1]
			return ids[:len(ids)-1]
		}
	}
	return ids
}

func coarseScore(metric Metric, q, v []float32) float32 {
	switch metric {
	case MetricDot:
		var s float32
		for i := range q {
			s += q[i] * v[i]
		}
		return s
	case MetricCosine:
		var dot, qn, vn float64
		for i := range q {
			qf := float64(q[i])
			vf := float64(v[i])
			dot += qf * vf
			qn += qf * qf
			vn += vf * vf
		}
		if qn == 0 || vn == 0 {
			return 0
		}
		return float32(dot / (math.Sqrt(qn) * math.Sqrt(vn)))
	case MetricEuclidean:
		var l2 float32
		for i := range q {
			d := q[i] - v[i]
			l2 += d * d
		}
		return -l2
	default:
		return 0
	}
}

func internalMetric(metric Metric) Metric {
	if metric == MetricCosine {
		return MetricDot
	}
	return metric
}

func normalizeVectorInPlace(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x * x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1.0 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

func (c *coarseIndex) nearestCentroid(vec []float32) int {
	best := 0
	bestScore := float32(-1e30)
	for idx := range c.centroids {
		s := coarseScore(c.metric, vec, c.centroids[idx])
		if idx == 0 || s > bestScore {
			best = idx
			bestScore = s
		}
	}
	return best
}

func (c *coarseIndex) upsert(docID uint64, vec []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if old, ok := c.docToCentroid[docID]; ok {
		c.lists[old] = removeDocID(c.lists[old], docID)
		delete(c.docToCentroid, docID)
	}

	if len(c.centroids) < c.maxCentroids {
		cp := append([]float32(nil), vec...)
		c.centroids = append(c.centroids, cp)
		c.centroidCounts = append(c.centroidCounts, 1)
		c.lists = append(c.lists, []uint64{docID})
		c.nextReplace = append(c.nextReplace, 0)
		c.docToCentroid[docID] = len(c.centroids) - 1
		return
	}

	idx := c.nearestCentroid(vec)
	if len(c.lists[idx]) < c.maxListSize {
		c.lists[idx] = append(c.lists[idx], docID)
		c.docToCentroid[docID] = idx
	} else if c.maxListSize > 0 {
		slot := c.nextReplace[idx]
		if slot < 0 || slot >= c.maxListSize {
			slot = 0
		}
		replaced := c.lists[idx][slot]
		if replaced != docID {
			delete(c.docToCentroid, replaced)
			c.lists[idx][slot] = docID
			c.docToCentroid[docID] = idx
		}
		c.nextReplace[idx] = (slot + 1) % c.maxListSize
	}

	n := c.centroidCounts[idx] + 1
	c.centroidCounts[idx] = n
	inv := 1.0 / float32(n)
	for d := range c.centroids[idx] {
		c.centroids[idx][d] += (vec[d] - c.centroids[idx][d]) * inv
	}
}

func (c *coarseIndex) delete(docID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx, ok := c.docToCentroid[docID]
	if !ok {
		return
	}
	c.lists[idx] = removeDocID(c.lists[idx], docID)
	if len(c.lists[idx]) == 0 {
		c.nextReplace[idx] = 0
	} else if c.nextReplace[idx] >= len(c.lists[idx]) {
		c.nextReplace[idx] = c.nextReplace[idx] % len(c.lists[idx])
	}
	delete(c.docToCentroid, docID)
}

func (c *coarseIndex) candidates(query []float32, nprobe, budget int) []uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.candidatesLocked(query, nprobe, budget)
}

func (c *coarseIndex) candidatesLocked(query []float32, nprobe, budget int) []uint64 {
	if len(c.centroids) == 0 || budget <= 0 {
		return nil
	}
	if nprobe <= 0 {
		nprobe = 16
	}
	if nprobe > len(c.centroids) {
		nprobe = len(c.centroids)
	}

	type scored struct {
		idx   int
		score float32
	}
	scores := make([]scored, len(c.centroids))
	for i := range c.centroids {
		scores[i] = scored{idx: i, score: coarseScore(c.metric, query, c.centroids[i])}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].score > scores[j].score })

	out := make([]uint64, 0, budget)
	seen := make(map[uint64]struct{}, budget)
	selected := make([]int, nprobe)
	for i := 0; i < nprobe; i++ {
		selected[i] = scores[i].idx
	}
	for depth := 0; len(out) < budget; depth++ {
		progress := false
		for _, centroidIdx := range selected {
			list := c.lists[centroidIdx]
			if depth >= len(list) {
				continue
			}
			progress = true
			docID := list[depth]
			if _, ok := seen[docID]; ok {
				continue
			}
			seen[docID] = struct{}{}
			out = append(out, docID)
			if len(out) >= budget {
				return out
			}
		}
		if !progress {
			break
		}
	}
	return out
}

func openIndex(indexDir string, createSpec IndexSpec, periodicSync time.Duration, create bool) (*index, error) {
	_ = periodicSync
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return nil, err
	}
	openOpts := storage.OpenOptions{}
	if create {
		def := withDefaultSpec(createSpec)
		openOpts.PebbleCacheBytes = def.Storage.PebbleCacheBytes
		openOpts.BloomBitsPerKey = def.Storage.BloomBitsPerKey
	}
	store, err := storage.Open(storage.PebblePath(indexDir), openOpts)
	if err != nil {
		return nil, err
	}

	var spec IndexSpec
	if create {
		spec = withDefaultSpec(createSpec)
		if err := store.SaveSpec(spec); err != nil {
			_ = store.Close()
			return nil, err
		}
	} else {
		spec, err = store.LoadSpec()
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("load spec: %w", err)
		}
		spec = withDefaultSpec(spec)
	}

	idx := &index{
		dir:                indexDir,
		spec:               spec,
		store:              store,
		notify:             make(chan struct{}, 1),
		repairQ:            repair.NewQueue(),
		compactThreshold:   spec.Graph.LBuild,
		lastCheckpointAt:   time.Now().UTC(),
		minCheckpointDelay: 5 * time.Second,
		pendingIDs:         make(map[uint64]struct{}),
		budgetResolver:     annbudget.NewResolver(spec.BudgetPolicy),
	}
	if spec.Storage.VectorCacheBytes > 0 {
		numCounters := spec.Storage.VectorCacheBytes / 32
		if numCounters < 1_000 {
			numCounters = 1_000
		}
		cache, cerr := ristretto.NewCache(&ristretto.Config{
			NumCounters: numCounters,
			MaxCost:     spec.Storage.VectorCacheBytes,
			BufferItems: 64,
		})
		if cerr != nil {
			_ = store.Close()
			return nil, fmt.Errorf("init vector cache: %w", cerr)
		}
		idx.cache = cache
	}

	if err := idx.loadOrInitManifest(create); err != nil {
		_ = idx.Close()
		return nil, err
	}
	if err := idx.loadOrInitGraph(); err != nil {
		_ = idx.Close()
		return nil, err
	}
	if err := idx.reconcileGraphState(); err != nil {
		_ = idx.Close()
		return nil, err
	}
	if err := idx.rebuildCoarseIndex(); err != nil {
		_ = idx.Close()
		return nil, err
	}

	// Ensure a usable base segment exists after recovery.
	if idx.manifest.ActiveSegment == "" {
		if err := idx.rebuildFromStoreState(); err != nil {
			_ = idx.Close()
			return nil, err
		}
	}
	return idx, nil
}

func (i *index) reconcileGraphState() error {
	count, err := i.store.CountVectors()
	if err != nil {
		return err
	}
	if i.manifest.ActiveSegment == "" || i.manifest.VectorCount != count {
		return i.rebuildFromStoreState()
	}
	return nil
}

func (i *index) coarseCentroidCount() int {
	n := i.spec.Storage.VectorBlockSize * 16
	if n < 128 {
		n = 128
	}
	if n > 4096 {
		n = 4096
	}
	return n
}

func (i *index) coarseListSizeLimit() int {
	n := i.spec.SearchDefaults.CandidateBudget / 4
	if n < 64 {
		n = 64
	}
	if n > 1024 {
		n = 1024
	}
	return n
}

func (i *index) rebuildCoarseIndex() error {
	c := newCoarseIndex(internalMetric(i.spec.Metric), i.coarseCentroidCount(), i.coarseListSizeLimit())
	err := i.store.IterateVectorsByDoc(func(docID uint64, _ []byte, rec storage.VectorRecord) error {
		c.upsert(docID, rec.VectorFP32)
		return nil
	})
	if err != nil {
		return err
	}
	i.coarse = c
	return nil
}

func (i *index) loadOrInitManifest(create bool) error {
	m, err := segment.LoadManifest(i.dir)
	if err == nil {
		i.manifest = m
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) && !create {
		return err
	}
	i.manifest = segment.Manifest{Version: 1, UpdatedAt: time.Now().UTC()}
	return segment.SaveManifestAtomic(i.dir, i.manifest)
}

func (i *index) loadOrInitGraph() error {
	state, ok, err := i.store.LoadGraphState()
	if err != nil {
		return err
	}
	if ok {
		state.Metric = internalMetric(state.Metric)
		i.graphIdx = graph.FromState(state)
		return nil
	}
	i.graphIdx = graph.New(internalMetric(i.spec.Metric), i.spec.Graph.R)
	return nil
}

func withDefaultSpec(spec IndexSpec) IndexSpec {
	if spec.ApplyMode == "" {
		spec.ApplyMode = ApplyModeSync
	}
	if spec.DurabilityMode == "" {
		spec.DurabilityMode = DurabilityPeriodic
	}
	if spec.BudgetPolicy.Mode == "" {
		spec.BudgetPolicy.Mode = api.BudgetPolicyAdaptive
	}
	if spec.BudgetPolicy.TargetRecall <= 0 {
		spec.BudgetPolicy.TargetRecall = 0.90
	}
	if spec.BudgetPolicy.MinEfSearch <= 0 {
		spec.BudgetPolicy.MinEfSearch = 32
	}
	if spec.BudgetPolicy.MaxEfSearch <= 0 {
		spec.BudgetPolicy.MaxEfSearch = 1024
	}
	if spec.BudgetPolicy.MinBeam <= 0 {
		spec.BudgetPolicy.MinBeam = 4
	}
	if spec.BudgetPolicy.MaxBeam <= 0 {
		spec.BudgetPolicy.MaxBeam = 64
	}
	if spec.BudgetPolicy.MinCandidateBudget <= 0 {
		spec.BudgetPolicy.MinCandidateBudget = 128
	}
	if spec.BudgetPolicy.MaxCandidateBudget <= 0 {
		spec.BudgetPolicy.MaxCandidateBudget = 16384
	}
	if spec.BudgetPolicy.MinRerankK <= 0 {
		spec.BudgetPolicy.MinRerankK = 32
	}
	if spec.BudgetPolicy.MaxRerankK <= 0 {
		spec.BudgetPolicy.MaxRerankK = 4096
	}
	if spec.Graph.R <= 0 {
		spec.Graph.R = 16
	}
	if spec.Graph.LSearch <= 0 {
		spec.Graph.LSearch = 64
	}
	if spec.Graph.Beam <= 0 {
		spec.Graph.Beam = 16
	}
	if spec.Graph.LBuild <= 0 {
		spec.Graph.LBuild = 256
	}
	if spec.Storage.PebbleCacheBytes <= 0 {
		spec.Storage.PebbleCacheBytes = 256 << 20
	}
	if spec.Storage.VectorCacheBytes <= 0 {
		spec.Storage.VectorCacheBytes = 64 << 20
	}
	if spec.Storage.VectorCacheBytes > api.MaxVectorCacheBytes {
		spec.Storage.VectorCacheBytes = api.MaxVectorCacheBytes
	}
	if spec.Storage.BloomBitsPerKey <= 0 {
		spec.Storage.BloomBitsPerKey = 10
	}
	if spec.Storage.VectorBlockSize <= 0 {
		spec.Storage.VectorBlockSize = 128
	}
	if spec.Storage.GraphPageSize <= 0 {
		spec.Storage.GraphPageSize = 64
	}
	if spec.Storage.PostingChunkSize <= 0 {
		spec.Storage.PostingChunkSize = 65536
	}
	defaultSearch := annbudget.DefaultSearchTuning(spec.Dim, spec.Metric, spec.BudgetPolicy.TargetRecall)
	if spec.SearchDefaults.EfSearch <= 0 {
		spec.SearchDefaults.EfSearch = defaultSearch.EfSearch
	}
	if spec.SearchDefaults.Beam <= 0 {
		spec.SearchDefaults.Beam = defaultSearch.Beam
	}
	if spec.SearchDefaults.CandidateBudget <= 0 {
		spec.SearchDefaults.CandidateBudget = defaultSearch.CandidateBudget
	}
	if spec.SearchDefaults.RerankK <= 0 {
		spec.SearchDefaults.RerankK = defaultSearch.RerankK
	}
	if spec.SearchDefaults.TargetRecall <= 0 {
		spec.SearchDefaults.TargetRecall = spec.BudgetPolicy.TargetRecall
	}
	if spec.SearchDefaults.BudgetScale <= 0 {
		spec.SearchDefaults.BudgetScale = 1
	}
	if spec.SearchDefaults.ShardWorkers <= 0 {
		spec.SearchDefaults.ShardWorkers = runtime.GOMAXPROCS(0)
		if spec.SearchDefaults.ShardWorkers > 4 {
			spec.SearchDefaults.ShardWorkers = 4
		}
	}
	return spec
}

func mergeSearchTuning(base SearchTuning, req SearchTuning) SearchTuning {
	out := base
	if req.EfSearch > 0 {
		out.EfSearch = req.EfSearch
	}
	if req.Beam > 0 {
		out.Beam = req.Beam
	}
	if req.CandidateBudget > 0 {
		out.CandidateBudget = req.CandidateBudget
	}
	if req.RerankK > 0 {
		out.RerankK = req.RerankK
	}
	if req.ShardWorkers > 0 {
		out.ShardWorkers = req.ShardWorkers
	}
	if req.TargetRecall > 0 {
		out.TargetRecall = req.TargetRecall
	}
	if req.BudgetScale > 0 {
		out.BudgetScale = req.BudgetScale
	}
	if req.AllowExactFallback {
		out.AllowExactFallback = true
	}
	return out
}

func (i *index) resolveSearchTuning(req SearchRequest, filteredCount int) SearchTuning {
	if i.budgetResolver == nil {
		return mergeSearchTuning(i.spec.SearchDefaults, req.Tuning)
	}
	return i.budgetResolver.Resolve(annbudget.Input{
		Spec:               i.spec,
		TopK:               req.TopK,
		FilteredCount:      filteredCount,
		Requested:          req.Tuning,
		AllowExactFallback: req.Tuning.AllowExactFallback,
	})
}

func (i *index) Upsert(ctx context.Context, mut Mutation) (ApplyToken, error) {
	if err := ctx.Err(); err != nil {
		return ApplyToken{}, err
	}
	if err := api.ValidateMutation(i.spec.Dim, mut); err != nil {
		return ApplyToken{}, err
	}
	env := mutationEnvelope{
		Op:           mutationUpsert,
		Token:        ApplyToken{TxnID: mut.TxnID, SeqID: mut.SeqID},
		ExternalID:   append([]byte(nil), mut.ExternalID...),
		VectorFP32:   append([]float32(nil), mut.VectorFP32...),
		PartitionKey: mut.PartitionKey,
		Tags:         cloneTags(mut.Tags),
	}
	i.applyMu.Lock()
	defer i.applyMu.Unlock()
	return env.Token, i.applyEnvelope(env)
}

func (i *index) Delete(ctx context.Context, mut DeleteMutation) (ApplyToken, error) {
	if err := ctx.Err(); err != nil {
		return ApplyToken{}, err
	}
	if err := api.ValidateDeleteMutation(mut); err != nil {
		return ApplyToken{}, err
	}
	env := mutationEnvelope{
		Op:         mutationDelete,
		Token:      ApplyToken{TxnID: mut.TxnID, SeqID: mut.SeqID},
		ExternalID: append([]byte(nil), mut.ExternalID...),
	}
	i.applyMu.Lock()
	defer i.applyMu.Unlock()
	return env.Token, i.applyEnvelope(env)
}

func (i *index) applyEnvelope(env mutationEnvelope) error {
	applied, err := i.store.IsApplied(env.Token)
	if err != nil {
		return err
	}
	if applied {
		i.signalApplied()
		return nil
	}

	writeOpts := pebble.NoSync
	if i.spec.DurabilityMode == DurabilitySyncEveryCommit {
		writeOpts = pebble.Sync
	}

	var resolvedDocID uint64
	var resolvedDocOK bool
	switch env.Op {
	case mutationUpsert:
		if i.spec.Metric == MetricCosine {
			normalizeVectorInPlace(env.VectorFP32)
		}
		err = i.store.PutVector(env.ExternalID, storage.VectorRecord{
			PartitionKey: env.PartitionKey,
			Tags:         cloneTags(env.Tags),
			VectorFP32:   append([]float32(nil), env.VectorFP32...),
		}, writeOpts)
		if err == nil {
			resolvedDocID, resolvedDocOK, err = i.store.DocIDForExternalID(env.ExternalID)
		}
	case mutationDelete:
		docID, ok, derr := i.store.DocIDForExternalID(env.ExternalID)
		if derr != nil {
			return derr
		}
		resolvedDocID, resolvedDocOK = docID, ok
		err = i.store.DeleteVector(env.ExternalID, writeOpts)
		if ok {
			i.repairQ.Enqueue(strconv.FormatUint(docID, 10))
		}
	default:
		err = fmt.Errorf("unknown op %q", env.Op)
	}
	if err != nil {
		return err
	}
	if err := i.store.MarkApplied(env.Token, writeOpts); err != nil {
		return err
	}

	if env.Op == mutationUpsert && resolvedDocOK {
		if i.cache != nil {
			i.cache.Set(resolvedDocID, append([]float32(nil), env.VectorFP32...), int64(len(env.VectorFP32))*4)
		}
		id := strconv.FormatUint(resolvedDocID, 10)
		getVec := func(nodeID string) ([]float32, bool) {
			docID, err := strconv.ParseUint(nodeID, 10, 64)
			if err != nil {
				return nil, false
			}
			rec, ok, err := i.store.GetVectorByDocID(docID)
			if err != nil || !ok {
				return nil, false
			}
			return rec.VectorFP32, true
		}
		i.graphIdx.Insert(id, env.VectorFP32, i.spec.Graph.LSearch, i.spec.Graph.Beam, getVec)
	}
	if env.Op == mutationDelete && resolvedDocOK {
		if i.cache != nil {
			i.cache.Del(resolvedDocID)
		}
		i.graphIdx.RemoveNode(strconv.FormatUint(resolvedDocID, 10))
	}

	i.pendingMu.Lock()
	switch env.Op {
	case mutationUpsert:
		if resolvedDocOK {
			i.pendingIDs[resolvedDocID] = struct{}{}
			if i.coarse == nil {
				i.coarse = newCoarseIndex(internalMetric(i.spec.Metric), i.coarseCentroidCount(), i.coarseListSizeLimit())
			}
			i.coarse.upsert(resolvedDocID, env.VectorFP32)
		}
	case mutationDelete:
		if resolvedDocOK {
			delete(i.pendingIDs, resolvedDocID)
			if i.coarse != nil {
				i.coarse.delete(resolvedDocID)
			}
		}
	}
	i.pendingMu.Unlock()
	i.graphDirty = true
	i.mutationsSinceBuild++
	if i.mutationsSinceBuild >= i.compactThreshold {
		if err := i.runMaintenance(false); err != nil {
			return err
		}
	}
	i.signalApplied()
	return nil
}

func (i *index) runMaintenance(force bool) error {
	if err := i.repairQ.RunOnce(context.Background(), func(id string) error {
		i.graphIdx.RemoveNode(id)
		return nil
	}); err != nil {
		return err
	}
	if force {
		return i.checkpointActiveState()
	}
	if !i.graphDirty || i.mutationsSinceBuild < i.compactThreshold {
		return nil
	}
	if i.minCheckpointDelay > 0 && time.Since(i.lastCheckpointAt) < i.minCheckpointDelay {
		return nil
	}
	return i.checkpointActiveState()
}

func (i *index) writeOptsForDurability() *pebble.WriteOptions {
	if i.spec.DurabilityMode == DurabilitySyncEveryCommit {
		return pebble.Sync
	}
	return pebble.NoSync
}

func (i *index) persistActiveState(records map[string]storage.VectorRecord) error {
	segName := segment.NewSegmentName(time.Now())
	if err := segment.WriteSnapshot(i.dir, segName, records); err != nil {
		return err
	}
	i.manifest.Version++
	i.manifest.ActiveSegment = segName
	i.manifest.VectorCount = uint64(len(records))
	i.manifest.UpdatedAt = time.Now().UTC()
	if err := segment.SaveManifestAtomic(i.dir, i.manifest); err != nil {
		return err
	}
	if err := i.store.SaveGraphState(i.graphIdx.SnapshotState(), i.writeOptsForDurability()); err != nil {
		return err
	}
	i.graphDirty = false
	i.mutationsSinceBuild = 0
	i.lastCheckpointAt = time.Now().UTC()
	i.pendingMu.Lock()
	i.pendingIDs = make(map[uint64]struct{})
	i.pendingMu.Unlock()
	return nil
}

func (i *index) checkpointActiveState() error {
	records, err := i.store.SnapshotVectorsMap()
	if err != nil {
		return err
	}
	return i.persistActiveState(records)
}

func (i *index) rebuildFromStoreState() error {
	records, err := i.store.SnapshotVectorsMap()
	if err != nil {
		return err
	}
	vectors := make(map[string][]float32, len(records))
	err = i.store.IterateVectorsByDoc(func(docID uint64, _ []byte, rec storage.VectorRecord) error {
		vectors[strconv.FormatUint(docID, 10)] = rec.VectorFP32
		return nil
	})
	if err != nil {
		return err
	}
	if err := i.graphIdx.Build(vectors); err != nil {
		return err
	}
	if err := i.rebuildCoarseIndex(); err != nil {
		return err
	}
	return i.persistActiveState(records)
}

func (i *index) signalApplied() {
	select {
	case i.notify <- struct{}{}:
	default:
	}
}

func (i *index) snapshotPendingIDs() []uint64 {
	i.pendingMu.RLock()
	defer i.pendingMu.RUnlock()
	out := make([]uint64, 0, len(i.pendingIDs))
	for id := range i.pendingIDs {
		out = append(out, id)
	}
	return out
}

func (i *index) Search(ctx context.Context, req SearchRequest) (SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	if len(req.VectorFP32) != i.spec.Dim {
		return SearchResult{}, fmt.Errorf("query dimension mismatch expected=%d actual=%d", i.spec.Dim, len(req.VectorFP32))
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	queryVector := append([]float32(nil), req.VectorFP32...)
	searchMetric := i.spec.Metric
	if i.spec.Metric == MetricCosine {
		normalizeVectorInPlace(queryVector)
		searchMetric = MetricDot
	}
	filteredDocIDs, err := i.store.CandidateDocIDs(req.PartitionKey, req.Tags)
	if err != nil {
		return SearchResult{}, err
	}
	tuning := i.resolveSearchTuning(req, len(filteredDocIDs))
	if tuning.CandidateBudget < req.TopK {
		tuning.CandidateBudget = req.TopK
	}
	allowSet := map[uint64]struct{}{}
	if len(filteredDocIDs) > 0 {
		for _, id := range filteredDocIDs {
			allowSet[id] = struct{}{}
		}
	}
	vecLookup, err := i.store.NewVectorLookup()
	if err != nil {
		return SearchResult{}, err
	}
	defer vecLookup.Close()
	docIDCache := make(map[string]uint64, tuning.CandidateBudget)
	parseDocID := func(id string) (uint64, bool) {
		if v, ok := docIDCache[id]; ok {
			return v, true
		}
		v, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			return 0, false
		}
		docIDCache[id] = v
		return v, true
	}
	allowFn := func(id string) bool {
		docID, ok := parseDocID(id)
		if !ok {
			return false
		}
		if len(allowSet) == 0 {
			return true
		}
		_, present := allowSet[docID]
		return present
	}

	vectorCache := make(map[uint64][]float32, tuning.CandidateBudget)
	loadVec := func(docID uint64) ([]float32, bool) {
		if vec, ok := vectorCache[docID]; ok {
			return vec, true
		}
		if i.cache != nil && req.PartitionKey == "" && len(req.Tags) == 0 {
			if cached, ok := i.cache.Get(docID); ok {
				if vec, vok := cached.([]float32); vok {
					vectorCache[docID] = vec
					return vec, true
				}
			}
		}
		if req.PartitionKey == "" && len(req.Tags) == 0 {
			vec, ok, err := vecLookup.GetVectorFP32ByDocID(docID)
			if err != nil || !ok {
				return nil, false
			}
			vectorCache[docID] = vec
			if i.cache != nil {
				i.cache.Set(docID, vec, int64(len(vec))*4)
			}
			return vec, true
		}
		rec, ok, err := vecLookup.GetVectorByDocID(docID)
		if err != nil || !ok {
			return nil, false
		}
		if !annfilter.Match(rec, req.PartitionKey, req.Tags) {
			return nil, false
		}
		vectorCache[docID] = rec.VectorFP32
		if i.cache != nil {
			i.cache.Set(docID, rec.VectorFP32, int64(len(rec.VectorFP32))*4)
		}
		return rec.VectorFP32, true
	}

	getVec := func(id string) ([]float32, bool) {
		docID, ok := parseDocID(id)
		if !ok {
			return nil, false
		}
		return loadVec(docID)
	}

	candidateMap := make(map[uint64][]float32, tuning.CandidateBudget)
	if i.coarse != nil {
		coarseBudget := tuning.CandidateBudget
		nprobe := tuning.Beam * 4
		if tuning.EfSearch/4 > nprobe {
			nprobe = tuning.EfSearch / 4
		}
		if nprobe < 16 {
			nprobe = 16
		}
		// Cosine queries benefit from leaving room for graph expansion. For
		// euclidean workloads, keeping full coarse budget preserves recall.
		if i.spec.Metric == MetricCosine {
			coarseBudget = tuning.CandidateBudget / 2
			if coarseBudget < req.TopK*8 {
				coarseBudget = req.TopK * 8
			}
			if coarseBudget > tuning.CandidateBudget {
				coarseBudget = tuning.CandidateBudget
			}
			nprobe = tuning.Beam * 6
			if tuning.EfSearch/3 > nprobe {
				nprobe = tuning.EfSearch / 3
			}
			if nprobe < 24 {
				nprobe = 24
			}
		}
		for _, docID := range i.coarse.candidates(queryVector, nprobe, coarseBudget) {
			if vec, ok := loadVec(docID); ok {
				candidateMap[docID] = vec
				atomic.AddUint64(&i.vectorBlockReads, 1)
				if len(candidateMap) >= coarseBudget {
					break
				}
			}
		}
	}

	// Top up candidates from graph traversal if coarse candidates are not enough.
	graphTopK := tuning.CandidateBudget * 2
	if graphTopK < req.TopK*16 {
		graphTopK = req.TopK * 16
	}
	if graphTopK < tuning.RerankK {
		graphTopK = tuning.RerankK
	}
	graphIDs, gerr := i.graphIdx.Search(queryVector, graphTopK, tuning.EfSearch, tuning.Beam, getVec, allowFn)
	if gerr == nil {
		for _, id := range graphIDs {
			docID, ok := parseDocID(id)
			if !ok {
				continue
			}
			if vec, ok := loadVec(docID); ok {
				candidateMap[docID] = vec
				atomic.AddUint64(&i.graphPageReads, 1)
				if len(candidateMap) >= tuning.CandidateBudget {
					break
				}
			}
		}
	}

	// Merge pending mutations since last graph rebuild instead of full-scanning the table.
	var pendingIDs []uint64
	if i.coarse == nil {
		pendingIDs = i.snapshotPendingIDs()
		if len(pendingIDs) <= tuning.CandidateBudget {
			for _, id := range pendingIDs {
				if _, ok := candidateMap[id]; ok {
					continue
				}
				if vec, ok := loadVec(id); ok {
					candidateMap[id] = vec
					if len(candidateMap) >= tuning.CandidateBudget {
						break
					}
				}
			}
		}
	}

	// Filtered search keeps deterministic coverage by augmenting from filter postings when needed.
	if len(filteredDocIDs) > 0 && len(candidateMap) < req.TopK {
		for _, id := range filteredDocIDs {
			if _, ok := candidateMap[id]; ok {
				continue
			}
			if vec, ok := loadVec(id); ok {
				candidateMap[id] = vec
				if len(candidateMap) >= tuning.CandidateBudget {
					break
				}
			}
		}
	}

	// Catastrophic fallback only when graph produced nothing and there is no pending delta to search.
	if tuning.AllowExactFallback && len(candidateMap) == 0 && len(filteredDocIDs) == 0 {
		atomic.AddUint64(&i.fallbackScans, 1)
		err := iterateAllVectorsForSearch(i.store, func(externalID []byte, rec storage.VectorRecord) error {
			if !annfilter.Match(rec, req.PartitionKey, req.Tags) {
				return nil
			}
			docID, ok, derr := i.store.DocIDForExternalID(externalID)
			if derr != nil || !ok {
				return derr
			}
			candidateMap[docID] = rec.VectorFP32
			atomic.AddUint64(&i.vectorBlockReads, 1)
			if len(candidateMap) >= tuning.CandidateBudget {
				return io.EOF
			}
			return nil
		})
		if err != nil && !errors.Is(err, io.EOF) {
			return SearchResult{}, err
		}
	}

	if len(candidateMap) > tuning.RerankK {
		topRerank := query.TopKDocIDsWithWorkers(searchMetric, queryVector, candidateMap, tuning.RerankK, tuning.ShardWorkers)
		trimmed := make(map[uint64][]float32, len(topRerank))
		for _, cand := range topRerank {
			if vec, ok := candidateMap[cand.DocID]; ok {
				trimmed[cand.DocID] = vec
			}
		}
		candidateMap = trimmed
	}
	top := query.TopKDocIDsWithWorkers(searchMetric, queryVector, candidateMap, req.TopK, tuning.ShardWorkers)
	res := SearchResult{Hits: make([]SearchHit, 0, len(top))}
	for idx := range top {
		externalID, ok, err := i.store.ExternalIDForDocID(top[idx].DocID)
		if err != nil || !ok {
			continue
		}
		res.Hits = append(res.Hits, SearchHit{
			ExternalID: externalID,
			Score:      top[idx].Score,
			Distance:   query.DistanceFromScore(i.spec.Metric, top[idx].Score),
		})
	}
	return res, nil
}

func (i *index) WaitApplied(ctx context.Context, token ApplyToken) error {
	for {
		applied, err := i.store.IsApplied(token)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-i.notify:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (i *index) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.applyMu.Lock()
	defer i.applyMu.Unlock()
	if err := i.runMaintenance(true); err != nil {
		return err
	}
	return i.store.Flush()
}

func (i *index) Snapshot(ctx context.Context, dst string) error {
	if err := i.Flush(ctx); err != nil {
		return err
	}
	return copyDir(i.dir, dst)
}

func (i *index) Verify(ctx context.Context, opts VerifyOptions) (VerifyReport, error) {
	if err := ctx.Err(); err != nil {
		return VerifyReport{}, err
	}
	return annverify.RunComprehensive(i.spec, i.store, i.manifest, i.dir, opts.Deep)
}

func (i *index) Stats(ctx context.Context) (IndexStats, error) {
	if err := ctx.Err(); err != nil {
		return IndexStats{}, err
	}
	vectorCount, err := i.store.CountVectors()
	if err != nil {
		return IndexStats{}, err
	}
	applied, err := i.store.CountApplied()
	if err != nil {
		return IndexStats{}, err
	}
	wm, err := i.store.Watermark()
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return IndexStats{}, err
	}
	stats := IndexStats{
		VectorCount:      vectorCount,
		AppliedMutations: applied,
		CurrentWatermark: wm,
		FallbackScans:    atomic.LoadUint64(&i.fallbackScans),
		GraphPageReads:   atomic.LoadUint64(&i.graphPageReads),
		VectorBlockReads: atomic.LoadUint64(&i.vectorBlockReads),
	}
	return stats, nil
}

func (i *index) Close() error {
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return nil
	}
	i.closed = true
	i.mu.Unlock()
	if i.cache != nil {
		i.cache.Close()
		i.cache = nil
	}
	return i.store.Close()
}

func cloneTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		outPath := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(outPath, info.Mode())
		}
		if err := copyFile(path, outPath, info.Mode()); err != nil {
			return err
		}
		return nil
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
