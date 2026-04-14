package vecindex

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
)

// Engine opens and manages a collection of IVF vector indexes backed by Pebble.
type Engine struct {
	rootDir string
	logger  zerolog.Logger
}

// NewEngine creates an Engine that stores index data under rootDir.
func NewEngine(rootDir string, logger zerolog.Logger) (*Engine, error) {
	return nil, errors.New("not implemented: NewEngine")
}

// CreateIndex builds a new IVF index from bulk entries and persists it.
func (e *Engine) CreateIndex(ctx context.Context, spec IVFSpec, bulk []BulkEntry) (*Index, error) {
	return nil, errors.New("not implemented: CreateIndex")
}

// OpenIndex re-opens an existing index by ID.
func (e *Engine) OpenIndex(ctx context.Context, id string) (*Index, error) {
	return nil, errors.New("not implemented: OpenIndex")
}

// DropIndex permanently removes an index and its backing data.
func (e *Engine) DropIndex(ctx context.Context, id string) error {
	return errors.New("not implemented: DropIndex")
}

// Close releases all resources held by the engine and its open indexes.
func (e *Engine) Close() error {
	return errors.New("not implemented: Close")
}
