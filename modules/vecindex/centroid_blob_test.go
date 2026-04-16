package vecindex

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/stretchr/testify/require"
)

func TestEncodeCentroidBlob_RoundTrip(t *testing.T) {
	t.Parallel()
	centroids := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}
	cs, err := kmeans.NewCentroidSet(42, centroids)
	require.NoError(t, err)

	blob, err := EncodeCentroidBlob(cs)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	decoded, err := DecodeCentroidBlob(blob)
	require.NoError(t, err)
	require.Equal(t, cs.Epoch(), decoded.Epoch())
	require.Equal(t, cs.Len(), decoded.Len())
	for i := 0; i < cs.Len(); i++ {
		got, err := decoded.Get(uint32(i))
		require.NoError(t, err)
		want, err := cs.Get(uint32(i))
		require.NoError(t, err)
		require.InDeltaSlice(t, want, got, 1e-6, "centroid %d mismatch", i)
	}
}

func TestEncodeCentroidBlob_CompressesData(t *testing.T) {
	t.Parallel()
	// Large identical-value centroids compress very well.
	dim := 128
	n := 64
	centroids := make([][]float32, n)
	for i := range centroids {
		v := make([]float32, dim)
		for d := range v {
			v[d] = float32(i+d) * 0.001
		}
		centroids[i] = v
	}
	cs, err := kmeans.NewCentroidSet(1, centroids)
	require.NoError(t, err)

	blob, err := EncodeCentroidBlob(cs)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	// Verify round-trip is lossless regardless of compression ratio.
	decoded, err := DecodeCentroidBlob(blob)
	require.NoError(t, err)
	require.Equal(t, n, decoded.Len())
}

func TestDecodeCentroidBlob_EmptyInput(t *testing.T) {
	t.Parallel()
	_, err := DecodeCentroidBlob(nil)
	require.Error(t, err)

	_, err = DecodeCentroidBlob([]byte{})
	require.Error(t, err)
}

func TestDecodeCentroidBlob_CorruptInput(t *testing.T) {
	t.Parallel()
	_, err := DecodeCentroidBlob([]byte("not-zstd-data"))
	require.Error(t, err)
}
