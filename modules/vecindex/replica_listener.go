package vecindex

import (
	"context"
	"sync"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

// rebuildReq is a request to rebuild an index's probeState from a new
// centroid blob. Sent by NotifyCentroidChange, consumed by the listener
// worker goroutine.
type rebuildReq struct {
	indexName string
	version   int64
}

// CentroidLoader loads the current centroid blob for an index from the
// _marmot_vec_<idx>_centroids table. Returns the raw zstd+msgpack blob
// and the stored version. Injected by the db layer.
type CentroidLoader func(ctx context.Context, indexName string) (blob []byte, version int64, err error)

// RebuildFromBlobFunc executes the rebuild pipeline for an index using
// externally-supplied centroids (skip k-means). Called on replicas when
// the origin node's centroid blob replicates in. Injected by the db layer.
type RebuildFromBlobFunc func(ctx context.Context, indexName string, cs *kmeans.CentroidSet) error

// ReplicaLogFunc is an optional logging callback for the replica listener.
type ReplicaLogFunc func(level, msg string, indexName string, err error)

// ReplicaListener implements the Go-side receiver for centroid-change
// notifications (design §8.8). The SQL trigger fires
// __marmot_vec_notify_centroid_change which calls Engine.NotifyCentroidChange,
// which does a non-blocking send to rebuildCh. The worker goroutine drains
// the channel, loads the new centroid blob, decodes it, and swaps probeState.
type ReplicaListener struct {
	engine    *Engine
	loadFn    CentroidLoader
	rebuildFn RebuildFromBlobFunc
	logFn     ReplicaLogFunc

	rebuildCh chan rebuildReq

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewReplicaListener creates a listener with a buffered channel of the given
// size. Typical buffer: 16 — enough to absorb burst reindexes across all
// indexes without blocking the writer transaction.
func NewReplicaListener(engine *Engine, bufSize int, loadFn CentroidLoader, rebuildFn RebuildFromBlobFunc, logFn ReplicaLogFunc) *ReplicaListener {
	if bufSize < 1 {
		bufSize = 1
	}
	return &ReplicaListener{
		engine:    engine,
		loadFn:    loadFn,
		rebuildFn: rebuildFn,
		logFn:     logFn,
		rebuildCh: make(chan rebuildReq, bufSize),
	}
}

// Start begins the worker goroutine that drains rebuildCh.
func (l *ReplicaListener) Start(ctx context.Context) {
	ctx, l.cancel = context.WithCancel(ctx)
	l.done = make(chan struct{})
	go l.worker(ctx)
}

// Stop signals the worker to shut down and waits for it to exit.
func (l *ReplicaListener) Stop() {
	l.once.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
	})
	if l.done != nil {
		<-l.done
	}
}

// Notify enqueues a rebuild request. Non-blocking: if the channel is full
// the request is coalesced (dropped) — a newer notification will arrive
// shortly. Called from the UDF inside a writer transaction; must return
// immediately.
func (l *ReplicaListener) Notify(indexName string, version int64) {
	select {
	case l.rebuildCh <- rebuildReq{indexName: indexName, version: version}:
	default:
		// Channel full — coalesce. The next drain will pick up the latest state.
	}
}

func (l *ReplicaListener) log(level, msg, indexName string, err error) {
	if l.logFn != nil {
		l.logFn(level, msg, indexName, err)
	}
}

func (l *ReplicaListener) worker(ctx context.Context) {
	defer close(l.done)

	for {
		select {
		case <-ctx.Done():
			return
		case req := <-l.rebuildCh:
			l.handleReq(ctx, req)
		}
	}
}

func (l *ReplicaListener) handleReq(ctx context.Context, req rebuildReq) {
	state, ok := l.engine.Lookup(req.indexName)
	if !ok {
		return
	}

	// Skip if our probeState is already at or past the requested version.
	// This covers: (a) origin self-notify after own REINDEX, (b) stale
	// coalesced notifications.
	if state.ProbeVersion() >= uint64(req.version) {
		return
	}

	blob, storedVersion, err := l.loadFn(ctx, req.indexName)
	if err != nil {
		l.log("warn", "failed to load centroid blob", req.indexName, err)
		return
	}

	// Re-check after load — another notification may have completed first.
	if state.ProbeVersion() >= uint64(storedVersion) {
		return
	}

	cs, err := DecodeCentroidBlob(blob)
	if err != nil {
		l.log("warn", "failed to decode centroid blob", req.indexName, err)
		return
	}

	if l.rebuildFn != nil {
		if err := l.rebuildFn(ctx, req.indexName, cs); err != nil {
			l.log("warn", "rebuild from blob failed", req.indexName, err)
			return
		}
	}

	// Atomic swap — install new centroids.
	state.SwapProbeState(cs)
	state.ResetDriftState(cs)
	l.log("info", "replica centroid swap complete", req.indexName, nil)
}
