package repair

import (
	"context"
	"sync"
)

// Queue tracks pending node cleanup actions caused by deletions.
type Queue struct {
	mu      sync.Mutex
	pending map[string]struct{}
}

func NewQueue() *Queue {
	return &Queue{pending: make(map[string]struct{})}
}

func (q *Queue) Enqueue(id string) {
	q.mu.Lock()
	q.pending[id] = struct{}{}
	q.mu.Unlock()
}

func (q *Queue) Len() int {
	q.mu.Lock()
	n := len(q.pending)
	q.mu.Unlock()
	return n
}

func (q *Queue) RunOnce(ctx context.Context, fn func(id string) error) error {
	q.mu.Lock()
	ids := make([]string, 0, len(q.pending))
	for id := range q.pending {
		ids = append(ids, id)
	}
	q.pending = make(map[string]struct{})
	q.mu.Unlock()

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			q.Enqueue(id)
			return err
		}
		if err := fn(id); err != nil {
			q.Enqueue(id)
			return err
		}
	}
	return nil
}
