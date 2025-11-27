package filter

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// BloomFilter is a space-efficient probabilistic data structure for set membership testing.
// Uses XXH64 with double hashing to generate k hash functions.
//
// Mathematical foundation for 0.0001% (10^-6) false positive rate:
//   - bits/key = -ln(10^-6) / (ln(2))^2 = 28.75
//   - optimal k = 28.75 * ln(2) = 20 hash functions
type BloomFilter struct {
	bits  []uint64 // Bit array stored as uint64 words
	m     uint64   // Total number of bits
	k     uint32   // Number of hash functions
	count uint64   // Elements added
	mu    sync.RWMutex
}

const (
	// DefaultFPRate is the default false positive rate (0.0001% = 10^-6)
	DefaultFPRate = 0.000001

	// BitsPerKey for 0.0001% FP rate
	BitsPerKey = 28.75

	// OptimalK for 0.0001% FP rate
	OptimalK = 20

	// Seeds for double hashing
	seed1 = 0x9E3779B97F4A7C15 // Golden ratio derived
	seed2 = 0xC6A4A7935BD1E995 // Murmur hash constant
)

// NewBloomFilter creates a new Bloom filter sized for n elements at the target FP rate.
// Uses 0.0001% FP rate by default (28.75 bits/key, k=20).
func NewBloomFilter(n int) *BloomFilter {
	if n <= 0 {
		n = 1
	}

	// Calculate optimal size: m = -n * ln(p) / (ln(2))^2
	// For p = 10^-6: m/n = 28.75
	m := uint64(math.Ceil(float64(n) * BitsPerKey))

	// Round up to nearest 64-bit word
	numWords := (m + 63) / 64
	m = numWords * 64

	return &BloomFilter{
		bits: make([]uint64, numWords),
		m:    m,
		k:    OptimalK,
	}
}

// NewBloomFilterFromKeys creates a Bloom filter and populates it with the given keys.
func NewBloomFilterFromKeys(keys []uint64) *BloomFilter {
	f := NewBloomFilter(len(keys))
	for _, key := range keys {
		f.Add(key)
	}
	return f
}

// Add inserts a key into the filter.
func (f *BloomFilter) Add(key uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	h1, h2 := f.hash(key)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % f.m
		wordIdx := idx / 64
		bitIdx := idx % 64
		f.bits[wordIdx] |= 1 << bitIdx
	}
	f.count++
}

// Contains checks if a key might be in the set.
// Returns true if the key is possibly in the set (may be false positive).
// Returns false if the key is definitely not in the set.
func (f *BloomFilter) Contains(key uint64) bool {
	if f == nil || len(f.bits) == 0 {
		return false
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	h1, h2 := f.hash(key)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % f.m
		wordIdx := idx / 64
		bitIdx := idx % 64
		if f.bits[wordIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

// ContainsAny checks if any of the given keys might be in the set.
// Returns true and the first potentially matching key if found.
func (f *BloomFilter) ContainsAny(keys []uint64) (bool, uint64) {
	for _, key := range keys {
		if f.Contains(key) {
			return true, key
		}
	}
	return false, 0
}

// hash computes two hash values for double hashing using XXH64.
func (f *BloomFilter) hash(key uint64) (uint64, uint64) {
	// Convert key to bytes for hashing
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, key)

	// Double hashing: h(i) = h1 + i*h2
	h1 := xxhash.Sum64(buf)

	// XOR with seed for second hash
	buf2 := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf2, key)
	binary.LittleEndian.PutUint64(buf2[8:], seed2)
	h2 := xxhash.Sum64(buf2)

	// Ensure h2 is odd to guarantee full period
	if h2%2 == 0 {
		h2++
	}

	return h1, h2
}

// Count returns the number of elements added to the filter.
func (f *BloomFilter) Count() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.count
}

// Size returns the size of the filter in bytes.
func (f *BloomFilter) Size() int {
	return len(f.bits) * 8
}

// BitCount returns the total number of bits in the filter.
func (f *BloomFilter) BitCount() uint64 {
	return f.m
}

// FillRatio returns the fraction of bits that are set.
func (f *BloomFilter) FillRatio() float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	set := 0
	for _, word := range f.bits {
		set += popcount(word)
	}
	return float64(set) / float64(f.m)
}

// popcount counts the number of set bits in a uint64.
func popcount(x uint64) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}

// Serialize converts the filter to bytes for network transmission.
// Format: [m:8][k:4][count:8][bits...]
func (f *BloomFilter) Serialize() []byte {
	if f == nil || len(f.bits) == 0 {
		return nil
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Header: m (8 bytes) + k (4 bytes) + count (8 bytes) = 20 bytes
	size := 20 + len(f.bits)*8
	buf := make([]byte, size)

	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], f.m)
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], f.k)
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:], f.count)
	offset += 8

	for _, word := range f.bits {
		binary.LittleEndian.PutUint64(buf[offset:], word)
		offset += 8
	}

	return buf
}

// DeserializeBloom reconstructs a Bloom filter from bytes.
func DeserializeBloom(data []byte) (*BloomFilter, error) {
	if len(data) == 0 {
		return &BloomFilter{}, nil
	}

	// Minimum size: 20 bytes header
	if len(data) < 20 {
		return nil, errors.New("invalid bloom filter data: too short")
	}

	offset := 0
	m := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	k := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	count := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// Validate
	numWords := (m + 63) / 64
	expectedSize := 20 + int(numWords)*8
	if len(data) != expectedSize {
		return nil, errors.New("invalid bloom filter data: size mismatch")
	}

	bits := make([]uint64, numWords)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}

	return &BloomFilter{
		bits:  bits,
		m:     m,
		k:     k,
		count: count,
	}, nil
}

// EstimateBloomSize returns the approximate size in bytes for a filter with n keys.
// Uses 0.0001% FP rate (28.75 bits/key).
func EstimateBloomSize(n int) int {
	if n <= 0 {
		return 0
	}
	// bits = n * 28.75, rounded up to 64-bit words, plus 20 byte header
	bits := uint64(math.Ceil(float64(n) * BitsPerKey))
	numWords := (bits + 63) / 64
	return 20 + int(numWords)*8
}

// HashRowKey creates a uint64 hash from a row key string using XXH64.
func HashRowKeyXXH64(rowKey string) uint64 {
	return xxhash.Sum64String(rowKey)
}

// HashPrimaryKeyXXH64 creates a uint64 hash from table name and primary key values.
func HashPrimaryKeyXXH64(table string, pkValues map[string][]byte) uint64 {
	h := xxhash.New()
	h.Write([]byte(table))

	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(pkValues))
	for k := range pkValues {
		keys = append(keys, k)
	}
	// Simple sort for determinism
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write(pkValues[k])
	}

	return h.Sum64()
}

// BloomFilterBuilder accumulates keys and builds a filter.
type BloomFilterBuilder struct {
	keys map[uint64]struct{}
}

// NewBloomFilterBuilder creates a new filter builder.
func NewBloomFilterBuilder() *BloomFilterBuilder {
	return &BloomFilterBuilder{
		keys: make(map[uint64]struct{}),
	}
}

// AddKey adds a key to the builder.
func (b *BloomFilterBuilder) AddKey(key uint64) {
	b.keys[key] = struct{}{}
}

// AddRowKey hashes and adds a row key string.
func (b *BloomFilterBuilder) AddRowKey(rowKey string) {
	b.keys[HashRowKeyXXH64(rowKey)] = struct{}{}
}

// AddPrimaryKey hashes and adds a primary key.
func (b *BloomFilterBuilder) AddPrimaryKey(table string, pkValues map[string][]byte) {
	b.keys[HashPrimaryKeyXXH64(table, pkValues)] = struct{}{}
}

// Count returns the number of keys added.
func (b *BloomFilterBuilder) Count() int {
	return len(b.keys)
}

// Build creates the Bloom filter from accumulated keys.
func (b *BloomFilterBuilder) Build() *BloomFilter {
	if len(b.keys) == 0 {
		return NewBloomFilter(1)
	}

	keys := make([]uint64, 0, len(b.keys))
	for k := range b.keys {
		keys = append(keys, k)
	}

	return NewBloomFilterFromKeys(keys)
}

// Keys returns the raw keys (for conflict checking with key-based guards).
func (b *BloomFilterBuilder) Keys() []uint64 {
	keys := make([]uint64, 0, len(b.keys))
	for k := range b.keys {
		keys = append(keys, k)
	}
	return keys
}
