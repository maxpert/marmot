package filter

import (
	"math"
	"math/rand"
	"testing"
)

func TestBloomFilter_BasicOperations(t *testing.T) {
	f := NewBloomFilter(100)

	// Add some keys
	for i := uint64(0); i < 100; i++ {
		f.Add(i)
	}

	// All added keys should be found
	for i := uint64(0); i < 100; i++ {
		if !f.Contains(i) {
			t.Errorf("Expected key %d to be found", i)
		}
	}

	// Count should match
	if f.Count() != 100 {
		t.Errorf("Expected count 100, got %d", f.Count())
	}
}

func TestBloomFilter_EmptyFilter(t *testing.T) {
	f := NewBloomFilter(0)

	// Empty filter should not contain anything
	if f.Contains(123) {
		t.Error("Empty filter should not contain any key")
	}

	// Nil filter should not panic
	var nilFilter *BloomFilter
	if nilFilter.Contains(123) {
		t.Error("Nil filter should return false")
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	// Test with 10,000 elements
	n := 10000
	f := NewBloomFilter(n)

	// Add n elements
	for i := 0; i < n; i++ {
		f.Add(uint64(i))
	}

	// Test with 1,000,000 non-existent elements
	testCount := 1000000
	falsePositives := 0

	for i := n; i < n+testCount; i++ {
		if f.Contains(uint64(i)) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(testCount)
	expectedFPRate := 0.000001 // 0.0001%

	// Allow 10x tolerance (FP rate can vary)
	if actualFPRate > expectedFPRate*10 {
		t.Errorf("False positive rate too high: got %.6f%%, expected < %.6f%%",
			actualFPRate*100, expectedFPRate*10*100)
	}

	t.Logf("False positive rate: %.6f%% (%.0f false positives out of %d tests)",
		actualFPRate*100, float64(falsePositives), testCount)
}

func TestBloomFilter_Serialization(t *testing.T) {
	f := NewBloomFilter(1000)

	// Add some keys
	for i := uint64(0); i < 1000; i++ {
		f.Add(i)
	}

	// Serialize
	data := f.Serialize()

	// Deserialize
	f2, err := DeserializeBloom(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Verify all keys are still found
	for i := uint64(0); i < 1000; i++ {
		if !f2.Contains(i) {
			t.Errorf("Key %d not found after deserialization", i)
		}
	}

	// Verify metadata
	if f2.m != f.m {
		t.Errorf("m mismatch: got %d, expected %d", f2.m, f.m)
	}
	if f2.k != f.k {
		t.Errorf("k mismatch: got %d, expected %d", f2.k, f.k)
	}
	if f2.count != f.count {
		t.Errorf("count mismatch: got %d, expected %d", f2.count, f.count)
	}
}

func TestBloomFilter_SerializationEmpty(t *testing.T) {
	// Empty data
	f, err := DeserializeBloom(nil)
	if err != nil {
		t.Fatalf("Failed to deserialize empty data: %v", err)
	}
	if f.Contains(123) {
		t.Error("Empty filter should not contain any key")
	}

	// Too short data
	_, err = DeserializeBloom([]byte{1, 2, 3})
	if err == nil {
		t.Error("Expected error for too short data")
	}
}

func TestBloomFilter_ContainsAny(t *testing.T) {
	f := NewBloomFilter(100)

	// Add keys 0-99
	for i := uint64(0); i < 100; i++ {
		f.Add(i)
	}

	// Test with keys that exist
	found, key := f.ContainsAny([]uint64{200, 50, 300})
	if !found {
		t.Error("Should find key 50")
	}
	if key != 50 {
		t.Errorf("Expected key 50, got %d", key)
	}

	// Test with keys that don't exist
	found, _ = f.ContainsAny([]uint64{200, 300, 400})
	// May have false positives, so we don't assert false
	t.Logf("ContainsAny for non-existent keys: %v", found)
}

func TestBloomFilter_Size(t *testing.T) {
	testCases := []struct {
		n            int
		expectedBits float64
	}{
		{250, 250 * BitsPerKey},
		{1000, 1000 * BitsPerKey},
		{10000, 10000 * BitsPerKey},
		{100000, 100000 * BitsPerKey},
		{1000000, 1000000 * BitsPerKey},
	}

	for _, tc := range testCases {
		f := NewBloomFilter(tc.n)

		// Allow for rounding to 64-bit words
		expectedWords := uint64(math.Ceil(tc.expectedBits / 64))
		expectedBits := expectedWords * 64

		if f.m != expectedBits {
			t.Errorf("n=%d: got %d bits, expected %d bits",
				tc.n, f.m, expectedBits)
		}

		actualBytes := f.Size()
		expectedBytes := int(expectedWords) * 8

		if actualBytes != expectedBytes {
			t.Errorf("n=%d: got %d bytes, expected %d bytes",
				tc.n, actualBytes, expectedBytes)
		}

		t.Logf("n=%d: %d bits = %d bytes", tc.n, f.m, actualBytes)
	}
}

func TestBloomFilter_EstimateSize(t *testing.T) {
	testCases := []int{250, 500, 1000, 10000, 100000, 1000000}

	for _, n := range testCases {
		estimated := EstimateBloomSize(n)
		actual := NewBloomFilter(n).Size() + 20 // Size() returns just bits, add header

		// Should be close (within a few bytes due to rounding)
		diff := estimated - actual
		if diff < -64 || diff > 64 {
			t.Errorf("n=%d: estimated %d, actual %d, diff %d",
				n, estimated, actual, diff)
		}

		t.Logf("n=%d: estimated=%d bytes", n, estimated)
	}
}

func TestBloomFilter_FillRatio(t *testing.T) {
	n := 10000
	f := NewBloomFilter(n)

	// Initially empty
	if f.FillRatio() != 0 {
		t.Error("Empty filter should have 0 fill ratio")
	}

	// Add elements
	for i := 0; i < n; i++ {
		f.Add(uint64(i))
	}

	// For 0.0001% FP, expected fill ratio is ~50%
	fillRatio := f.FillRatio()
	expectedFillRatio := 1 - math.Exp(-float64(OptimalK)/BitsPerKey)

	// Allow 10% tolerance
	if math.Abs(fillRatio-expectedFillRatio) > 0.1 {
		t.Errorf("Fill ratio %.2f%%, expected ~%.2f%%",
			fillRatio*100, expectedFillRatio*100)
	}

	t.Logf("Fill ratio: %.2f%% (expected ~%.2f%%)", fillRatio*100, expectedFillRatio*100)
}

func TestBloomFilter_HashRowKey(t *testing.T) {
	// Same key should produce same hash
	h1 := HashRowKeyXXH64("test_key_123")
	h2 := HashRowKeyXXH64("test_key_123")
	if h1 != h2 {
		t.Error("Same key should produce same hash")
	}

	// Different keys should produce different hashes (with high probability)
	h3 := HashRowKeyXXH64("test_key_456")
	if h1 == h3 {
		t.Error("Different keys should produce different hashes")
	}
}

func TestBloomFilter_HashPrimaryKey(t *testing.T) {
	pk1 := map[string][]byte{"id": []byte("123")}
	pk2 := map[string][]byte{"id": []byte("123")}
	pk3 := map[string][]byte{"id": []byte("456")}

	h1 := HashPrimaryKeyXXH64("users", pk1)
	h2 := HashPrimaryKeyXXH64("users", pk2)
	h3 := HashPrimaryKeyXXH64("users", pk3)
	h4 := HashPrimaryKeyXXH64("orders", pk1)

	if h1 != h2 {
		t.Error("Same table and pk should produce same hash")
	}
	if h1 == h3 {
		t.Error("Different pk should produce different hash")
	}
	if h1 == h4 {
		t.Error("Different table should produce different hash")
	}
}

func TestBloomFilterBuilder(t *testing.T) {
	b := NewBloomFilterBuilder()

	// Add various keys
	b.AddKey(123)
	b.AddRowKey("row_key_1")
	b.AddRowKey("row_key_2")
	b.AddPrimaryKey("users", map[string][]byte{"id": []byte("1")})

	if b.Count() != 4 {
		t.Errorf("Expected count 4, got %d", b.Count())
	}

	// Build filter
	f := b.Build()

	// Verify keys are in filter
	if !f.Contains(123) {
		t.Error("Filter should contain key 123")
	}
	if !f.Contains(HashRowKeyXXH64("row_key_1")) {
		t.Error("Filter should contain row_key_1")
	}

	// Get keys
	keys := b.Keys()
	if len(keys) != 4 {
		t.Errorf("Expected 4 keys, got %d", len(keys))
	}
}

func TestBloomFilter_LargeDataSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	n := 1000000
	f := NewBloomFilter(n)

	// Add 1M random keys
	rand.Seed(42)
	keys := make([]uint64, n)
	for i := 0; i < n; i++ {
		keys[i] = rand.Uint64()
		f.Add(keys[i])
	}

	// Verify all keys are found
	for i := 0; i < 1000; i++ { // Sample check
		if !f.Contains(keys[i]) {
			t.Errorf("Key %d not found", i)
		}
	}

	// Check size
	expectedSize := EstimateBloomSize(n)
	actualSize := f.Size() + 20
	t.Logf("1M keys: %d bytes (%.2f MB)", actualSize, float64(actualSize)/(1024*1024))

	if actualSize > expectedSize+1000 {
		t.Errorf("Size larger than expected: %d > %d", actualSize, expectedSize)
	}
}

// Benchmarks

func BenchmarkBloomFilter_Add(b *testing.B) {
	f := NewBloomFilter(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add(uint64(i))
	}
}

func BenchmarkBloomFilter_Contains(b *testing.B) {
	f := NewBloomFilter(100000)
	for i := 0; i < 100000; i++ {
		f.Add(uint64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Contains(uint64(i % 200000)) // Mix of hits and misses
	}
}

func BenchmarkBloomFilter_Serialize(b *testing.B) {
	f := NewBloomFilter(100000)
	for i := 0; i < 100000; i++ {
		f.Add(uint64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Serialize()
	}
}

func BenchmarkBloomFilter_Deserialize(b *testing.B) {
	f := NewBloomFilter(100000)
	for i := 0; i < 100000; i++ {
		f.Add(uint64(i))
	}
	data := f.Serialize()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DeserializeBloom(data)
	}
}

func BenchmarkHashRowKeyXXH64(b *testing.B) {
	key := "user:12345:profile"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HashRowKeyXXH64(key)
	}
}
