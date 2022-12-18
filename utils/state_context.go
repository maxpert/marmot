package utils

import (
	"context"
	"time"
)

type StateContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewStateContext() *StateContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &StateContext{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *StateContext) Cancel() {
	s.cancel()
}

func (s *StateContext) IsCanceled() bool {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err() == context.Canceled
	case <-time.After(0):
		return false
	}
}
