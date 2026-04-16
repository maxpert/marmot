package metric

import (
	"math/rand"
	"strconv"
)

func randSource(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

func randomVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

func itoaTest(n int) string { return strconv.Itoa(n) }
