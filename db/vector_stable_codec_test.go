package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStableCodecReservoirSamplesAreClusterCapped(t *testing.T) {
	reservoir, err := newStableCodecReservoir(1, 2)
	require.NoError(t, err)
	defer reservoir.Close()

	for i := 0; i < stableCodecTrainingSampleLimit; i++ {
		clusterID := int64(1)
		if i >= stableCodecTrainingSampleLimit-10 {
			clusterID = 2
		}
		reservoir.Add(clusterID, encodeVec(t, []float32{float32(i), float32(clusterID)}))
	}
	samples, err := reservoir.Samples()
	require.NoError(t, err)

	counts := map[int64]int{}
	for _, sample := range samples {
		counts[sample.ClusterID]++
	}
	require.Equal(t, stableCodecTrainingSampleLimit/2, counts[1])
	require.Equal(t, 10, counts[2])
}
