package db

import (
	"encoding/binary"
	"math"
	"testing"
)

func testPreparedVec(values ...float32) []byte {
	out := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(value))
	}
	return out
}

func TestStableCodecReservoirSamplesDoNotDiscardUnusedQuota(t *testing.T) {
	t.Parallel()

	reservoir, err := newStableCodecReservoir(17, 2)
	if err != nil {
		t.Fatalf("newStableCodecReservoir: %v", err)
	}
	defer reservoir.Close()

	for i := 0; i < stableCodecTrainingSampleLimit; i++ {
		reservoir.Add(1, testPreparedVec(float32(i), 1))
	}
	for i := 0; i < 10; i++ {
		reservoir.Add(2, testPreparedVec(float32(i), 2))
	}

	samples, err := reservoir.Samples()
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(samples) != reservoir.Count() {
		t.Fatalf("Samples() returned %d vectors, want all reservoir slots %d", len(samples), reservoir.Count())
	}
}
