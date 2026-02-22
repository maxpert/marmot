package freshann

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
)

type EngineOptions struct {
	RootDir              string
	PeriodicSyncInterval time.Duration
}

type engine struct {
	rootDir              string
	periodicSyncInterval time.Duration

	mu     sync.RWMutex
	closed bool
	open   map[IndexID]*index
}

func NewEngine(opts EngineOptions) (Engine, error) {
	if opts.RootDir == "" {
		return nil, fmt.Errorf("root directory is required")
	}
	if err := os.MkdirAll(opts.RootDir, 0o755); err != nil {
		return nil, err
	}
	if opts.PeriodicSyncInterval <= 0 {
		opts.PeriodicSyncInterval = 2 * time.Second
	}
	return &engine{
		rootDir:              opts.RootDir,
		periodicSyncInterval: opts.PeriodicSyncInterval,
		open:                 make(map[IndexID]*index),
	}, nil
}

func (e *engine) CreateIndex(ctx context.Context, spec IndexSpec) (IndexHandle, error) {
	if err := ctx.Err(); err != nil {
		return IndexHandle{}, err
	}
	if err := api.ValidateSpec(spec); err != nil {
		return IndexHandle{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return IndexHandle{}, ErrClosed
	}
	if _, exists := e.open[spec.ID]; exists {
		return IndexHandle{}, fmt.Errorf("%w: %s", ErrIndexExists, spec.ID)
	}
	idxDir := e.indexDir(spec.ID)
	if _, err := os.Stat(idxDir); err == nil {
		return IndexHandle{}, fmt.Errorf("%w: %s", ErrIndexExists, spec.ID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return IndexHandle{}, err
	}
	idx, err := openIndex(idxDir, spec, e.periodicSyncInterval, true)
	if err != nil {
		return IndexHandle{}, err
	}
	e.open[spec.ID] = idx
	return IndexHandle{ID: spec.ID}, nil
}

func (e *engine) OpenIndex(ctx context.Context, id IndexID) (Index, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, ErrClosed
	}
	if idx, ok := e.open[id]; ok {
		return idx, nil
	}
	idxDir := e.indexDir(id)
	if _, err := os.Stat(idxDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrIndexNotFound, id)
		}
		return nil, err
	}
	idx, err := openIndex(idxDir, IndexSpec{}, e.periodicSyncInterval, false)
	if err != nil {
		return nil, err
	}
	e.open[id] = idx
	return idx, nil
}

func (e *engine) DropIndex(ctx context.Context, id IndexID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if idx, ok := e.open[id]; ok {
		if err := idx.Close(); err != nil {
			return err
		}
		delete(e.open, id)
	}
	idxDir := e.indexDir(id)
	if err := os.RemoveAll(idxDir); err != nil {
		return err
	}
	return nil
}

func (e *engine) ListIndexes(ctx context.Context) ([]IndexMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	e.mu.RUnlock()

	entries, err := os.ReadDir(e.rootDir)
	if err != nil {
		return nil, err
	}
	out := make([]IndexMeta, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := IndexID(entry.Name())
		idx, err := e.OpenIndex(ctx, id)
		if err != nil {
			continue
		}
		native := idx.(*index)
		out = append(out, IndexMeta{ID: id, Spec: native.spec})
	}
	return out, nil
}

func (e *engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for id, idx := range e.open {
		if err := idx.Close(); err != nil {
			return fmt.Errorf("close index %s: %w", id, err)
		}
	}
	e.open = nil
	return nil
}

func (e *engine) indexDir(id IndexID) string {
	name := string(id)
	name = strings.ReplaceAll(name, string(filepath.Separator), "_")
	return filepath.Join(e.rootDir, name)
}
