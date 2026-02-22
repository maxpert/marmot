package quant

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFP16RoundTrip(t *testing.T) {
	in := []float32{0, 1.5, -2.25, 1000.1}
	encoded := FP16Encode(in)
	decoded := FP16Decode(encoded)
	require.Len(t, decoded, len(in))
	for i := range in {
		require.Less(t, math.Abs(float64(decoded[i]-in[i])), 1.5)
	}
}
