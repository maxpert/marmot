package freshann

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/freshann/pkg/api"
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
	pendingMu           sync.RWMutex
	pendingIDs          map[string]struct{}

	applyMu sync.Mutex
	mu      sync.RWMutex
	closed  bool

	notify chan struct{}
}

func openIndex(indexDir string, createSpec IndexSpec, periodicSync time.Duration, create bool) (*index, error) {
	_ = periodicSync
	if err := os.MkdirAll(indexDir, 0o755); err != nil {
		return nil, err
	}
	store, err := storage.Open(storage.PebblePath(indexDir), storage.OpenOptions{})
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
		dir:              indexDir,
		spec:             spec,
		store:            store,
		notify:           make(chan struct{}, 1),
		repairQ:          repair.NewQueue(),
		compactThreshold: spec.Graph.LBuild,
		pendingIDs:       make(map[string]struct{}),
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

	// Ensure a usable base segment exists after recovery.
	if idx.manifest.ActiveSegment == "" {
		if err := idx.compactAndRebuild(); err != nil {
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
		return i.compactAndRebuild()
	}
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
		i.graphIdx = graph.FromState(state)
		return nil
	}
	i.graphIdx = graph.New(i.spec.Metric, i.spec.Graph.R)
	return nil
}

func withDefaultSpec(spec IndexSpec) IndexSpec {
	if spec.ApplyMode == "" {
		spec.ApplyMode = ApplyModeSync
	}
	if spec.DurabilityMode == "" {
		spec.DurabilityMode = DurabilityPeriodic
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
	return spec
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

	switch env.Op {
	case mutationUpsert:
		err = i.store.PutVector(env.ExternalID, storage.VectorRecord{
			PartitionKey: env.PartitionKey,
			Tags:         cloneTags(env.Tags),
			VectorFP32:   append([]float32(nil), env.VectorFP32...),
		}, writeOpts)
	case mutationDelete:
		err = i.store.DeleteVector(env.ExternalID, writeOpts)
		i.repairQ.Enqueue(string(env.ExternalID))
	default:
		err = fmt.Errorf("unknown op %q", env.Op)
	}
	if err != nil {
		return err
	}
	if err := i.store.MarkApplied(env.Token, writeOpts); err != nil {
		return err
	}
	i.pendingMu.Lock()
	switch env.Op {
	case mutationUpsert:
		i.pendingIDs[string(env.ExternalID)] = struct{}{}
	case mutationDelete:
		delete(i.pendingIDs, string(env.ExternalID))
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
	if force || (i.graphDirty && i.mutationsSinceBuild >= i.compactThreshold) {
		return i.compactAndRebuild()
	}
	if force && i.graphDirty {
		return i.compactAndRebuild()
	}
	return nil
}

func (i *index) compactAndRebuild() error {
	records, err := i.store.SnapshotVectorsMap()
	if err != nil {
		return err
	}
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
	vectors := make(map[string][]float32, len(records))
	for id, rec := range records {
		vectors[id] = rec.VectorFP32
	}
	if err := i.graphIdx.Build(vectors); err != nil {
		return err
	}
	writeOpts := pebble.NoSync
	if i.spec.DurabilityMode == DurabilitySyncEveryCommit {
		writeOpts = pebble.Sync
	}
	if err := i.store.SaveGraphState(i.graphIdx.SnapshotState(), writeOpts); err != nil {
		return err
	}
	i.graphDirty = false
	i.mutationsSinceBuild = 0
	i.pendingMu.Lock()
	i.pendingIDs = make(map[string]struct{})
	i.pendingMu.Unlock()
	return nil
}

func (i *index) signalApplied() {
	select {
	case i.notify <- struct{}{}:
	default:
	}
}

func (i *index) snapshotPendingIDs() []string {
	i.pendingMu.RLock()
	defer i.pendingMu.RUnlock()
	out := make([]string, 0, len(i.pendingIDs))
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

	filteredIDs, err := i.store.CandidateExternalIDs(req.PartitionKey, req.Tags)
	if err != nil {
		return SearchResult{}, err
	}
	allowSet := map[string]struct{}{}
	if len(filteredIDs) > 0 {
		for _, id := range filteredIDs {
			allowSet[string(id)] = struct{}{}
		}
	}
	allowFn := func(id string) bool {
		if len(allowSet) == 0 {
			return true
		}
		_, ok := allowSet[id]
		return ok
	}

	getVec := func(id string) ([]float32, bool) {
		rec, ok, err := i.store.GetVector([]byte(id))
		if err != nil || !ok {
			return nil, false
		}
		if !annfilter.Match(rec, req.PartitionKey, req.Tags) {
			return nil, false
		}
		return rec.VectorFP32, true
	}

	candidateMap := make(map[string][]float32)
	graphIDs, gerr := i.graphIdx.Search(req.VectorFP32, req.TopK*4, i.spec.Graph.LSearch, i.spec.Graph.Beam, getVec, allowFn)
	if gerr == nil {
		for _, id := range graphIDs {
			if vec, ok := getVec(id); ok {
				candidateMap[id] = vec
			}
		}
	}

	// Merge pending mutations since last graph rebuild instead of full-scanning the table.
	pendingIDs := i.snapshotPendingIDs()
	for _, id := range pendingIDs {
		if _, ok := candidateMap[id]; ok {
			continue
		}
		if vec, ok := getVec(id); ok {
			candidateMap[id] = vec
		}
	}

	// Filtered search keeps deterministic coverage by augmenting from filter postings when needed.
	if len(filteredIDs) > 0 && len(candidateMap) < req.TopK {
		for _, id := range filteredIDs {
			key := string(id)
			if _, ok := candidateMap[key]; ok {
				continue
			}
			if vec, ok := getVec(key); ok {
				candidateMap[key] = vec
				if len(candidateMap) >= req.TopK*4 {
					break
				}
			}
		}
	}

	// Catastrophic fallback only when graph produced nothing and there is no pending delta to search.
	if len(candidateMap) == 0 && len(filteredIDs) == 0 && len(pendingIDs) == 0 {
		err := iterateAllVectorsForSearch(i.store, func(externalID []byte, rec storage.VectorRecord) error {
			if !annfilter.Match(rec, req.PartitionKey, req.Tags) {
				return nil
			}
			candidateMap[string(externalID)] = rec.VectorFP32
			return nil
		})
		if err != nil {
			return SearchResult{}, err
		}
	}

	top := query.TopKWithWorkers(i.spec.Metric, req.VectorFP32, candidateMap, req.TopK, 0)
	res := SearchResult{Hits: make([]SearchHit, len(top))}
	for idx := range top {
		res.Hits[idx] = SearchHit{
			ExternalID: append([]byte(nil), top[idx].ExternalID...),
			Score:      top[idx].Score,
			Distance:   query.DistanceFromScore(i.spec.Metric, top[idx].Score),
		}
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
