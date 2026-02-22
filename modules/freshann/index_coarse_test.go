package freshann

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoarseIndexReplacementRoundRobin(t *testing.T) {
	t.Parallel()

	c := newCoarseIndex(MetricDot, 1, 2)
	vec := []float32{1}

	c.upsert(1, vec)
	c.upsert(2, vec)
	c.upsert(3, vec) // replaces slot 0
	c.upsert(4, vec) // replaces slot 1

	require.Len(t, c.lists, 1)
	require.ElementsMatch(t, []uint64{3, 4}, c.lists[0])
	_, has1 := c.docToCentroid[1]
	_, has2 := c.docToCentroid[2]
	_, has3 := c.docToCentroid[3]
	_, has4 := c.docToCentroid[4]
	require.False(t, has1)
	require.False(t, has2)
	require.True(t, has3)
	require.True(t, has4)
}

func TestCoarseIndexDeleteKeepsReplacementCursorValid(t *testing.T) {
	t.Parallel()

	c := newCoarseIndex(MetricDot, 1, 2)
	vec := []float32{1}

	c.upsert(1, vec)
	c.upsert(2, vec)
	c.upsert(3, vec)
	c.delete(3)
	c.upsert(4, vec)

	require.Len(t, c.lists, 1)
	require.LessOrEqual(t, c.nextReplace[0], len(c.lists[0]))
}
