package filter

import (
	"testing"

	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatch(t *testing.T) {
	rec := storage.VectorRecord{PartitionKey: "p1", Tags: map[string]string{"lang": "en"}}
	require.True(t, Match(rec, "p1", map[string]string{"lang": "en"}))
	require.False(t, Match(rec, "p2", nil))
	require.False(t, Match(rec, "", map[string]string{"lang": "fr"}))
}
