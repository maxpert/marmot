package repair

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueRunOnce(t *testing.T) {
	q := NewQueue()
	q.Enqueue("a")
	q.Enqueue("b")
	seen := map[string]bool{}
	err := q.RunOnce(context.Background(), func(id string) error {
		seen[id] = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, seen["a"])
	require.True(t, seen["b"])
	require.Equal(t, 0, q.Len())
}
