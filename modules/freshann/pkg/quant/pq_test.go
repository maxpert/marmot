package quant

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPQTrainEncodeDecode(t *testing.T) {
	vectors := [][]float32{
		{1, 0, 0, 1},
		{0.9, 0.1, 0.1, 0.9},
		{-1, 0, 0, -1},
		{-0.9, -0.1, -0.1, -0.9},
	}
	m, err := TrainPQ(vectors, 2, 2, 42, 4)
	require.NoError(t, err)
	code, err := m.Encode(vectors[0])
	require.NoError(t, err)
	require.Len(t, code, 2)
	decoded, err := m.Decode(code)
	require.NoError(t, err)
	require.Len(t, decoded, 4)
}
