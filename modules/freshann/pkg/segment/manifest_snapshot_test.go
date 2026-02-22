package segment

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := Manifest{Version: 2, ActiveSegment: "x.seg", VectorCount: 10, UpdatedAt: time.Now().UTC()}
	require.NoError(t, SaveManifestAtomic(dir, m))
	m2, err := LoadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, m.Version, m2.Version)
	require.Equal(t, m.ActiveSegment, m2.ActiveSegment)
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	records := map[string]storage.VectorRecord{
		"a": {PartitionKey: "p", VectorFP32: []float32{1, 2}},
	}
	name := NewSegmentName(time.Now())
	require.NoError(t, WriteSnapshot(dir, name, records))
	out, err := ReadSnapshot(dir, name)
	require.NoError(t, err)
	require.Equal(t, records["a"].VectorFP32, out["a"].VectorFP32)
}
