package db

import (
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash/v2"
	cuckoo "github.com/linvon/cuckoo-filter"
	"github.com/maxpert/marmot/telemetry"
)

const (
	// Cuckoo filter configuration
	// capacity = bucketSize × numBuckets = 4 × 250000 = 1M entries
	cuckooBucketSize      = 4
	cuckooFingerprintSize = 32     // 32-bit fingerprint = FP rate ~2.3×10⁻¹⁰
	cuckooNumBuckets      = 250000 // 1M capacity
)

// hashBufPool reduces allocations for hash-to-bytes conversion.
var hashBufPool = sync.Pool{
	New: func() any { return make([]byte, 8) },
}

// IntentFilter provides fast-path conflict detection using a Cuckoo filter.
//
// Design:
//   - Hash = XXH64(table:rowKey) for each intent
//   - Filter MISS = definitely no conflict → fast path (batch write)
//   - Filter HIT = maybe conflict → slow path (Pebble lookup)
//   - FP rate ~2.3×10⁻¹⁰ with 32-bit fingerprint
//
// Thread-safe for concurrent access.
type IntentFilter struct {
	filter    *cuckoo.Filter
	mu        sync.RWMutex
	txnHashes map[uint64]map[uint64]struct{} // txnID → set of hash values
}

// NewIntentFilter creates a new Cuckoo-based intent filter.
func NewIntentFilter() *IntentFilter {
	cf := cuckoo.NewFilter(cuckooBucketSize, cuckooFingerprintSize,
		cuckooNumBuckets, cuckoo.TableTypePacked)
	return &IntentFilter{
		filter:    cf,
		txnHashes: make(map[uint64]map[uint64]struct{}),
	}
}

// Check returns true if the hash MIGHT exist (requires slow path).
// Returns false if the hash definitely does NOT exist (fast path safe).
func (f *IntentFilter) Check(tbHash uint64) bool {
	f.mu.RLock()
	buf := hashBufPool.Get().([]byte)
	binary.LittleEndian.PutUint64(buf, tbHash)
	result := f.filter.Contain(buf)
	hashBufPool.Put(buf)
	f.mu.RUnlock()
	return result
}

// Add inserts a hash into the filter and tracks it for the transaction.
// Must be called after confirming no conflict (either fast or slow path).
func (f *IntentFilter) Add(txnID uint64, tbHash uint64) {
	f.mu.Lock()
	buf := hashBufPool.Get().([]byte)
	binary.LittleEndian.PutUint64(buf, tbHash)
	f.filter.Add(buf)
	hashBufPool.Put(buf)
	if f.txnHashes[txnID] == nil {
		f.txnHashes[txnID] = make(map[uint64]struct{})
	}
	f.txnHashes[txnID][tbHash] = struct{}{}
	size := f.filter.Size()
	txnCount := len(f.txnHashes)
	f.mu.Unlock()

	telemetry.IntentFilterSize.Set(float64(size))
	telemetry.IntentFilterTxnCount.Set(float64(txnCount))
}

// Remove deletes all hashes for a transaction from the filter.
// Called on commit/abort AFTER Pebble operations complete.
func (f *IntentFilter) Remove(txnID uint64) {
	f.mu.Lock()
	hashes, exists := f.txnHashes[txnID]
	if !exists {
		f.mu.Unlock()
		return
	}
	buf := hashBufPool.Get().([]byte)
	for h := range hashes {
		binary.LittleEndian.PutUint64(buf, h)
		f.filter.Delete(buf)
	}
	hashBufPool.Put(buf)
	delete(f.txnHashes, txnID)
	size := f.filter.Size()
	txnCount := len(f.txnHashes)
	f.mu.Unlock()

	telemetry.IntentFilterSize.Set(float64(size))
	telemetry.IntentFilterTxnCount.Set(float64(txnCount))
}

// RemoveHash removes a single hash from a transaction's tracking.
// Used for single-intent deletion (e.g., DeleteIntent).
func (f *IntentFilter) RemoveHash(txnID uint64, tbHash uint64) {
	f.mu.Lock()
	buf := hashBufPool.Get().([]byte)
	binary.LittleEndian.PutUint64(buf, tbHash)
	f.filter.Delete(buf)
	hashBufPool.Put(buf)

	// Update txnHashes tracking (O(1) with map)
	if hashes := f.txnHashes[txnID]; hashes != nil {
		delete(hashes, tbHash)
		if len(hashes) == 0 {
			delete(f.txnHashes, txnID)
		}
	}
	size := f.filter.Size()
	txnCount := len(f.txnHashes)
	f.mu.Unlock()

	telemetry.IntentFilterSize.Set(float64(size))
	telemetry.IntentFilterTxnCount.Set(float64(txnCount))
}

// Size returns current number of entries in the filter.
func (f *IntentFilter) Size() uint {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.filter.Size()
}

// TxnCount returns number of tracked transactions.
func (f *IntentFilter) TxnCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.txnHashes)
}

// ComputeIntentHash computes the hash for conflict detection.
// Combines table name and row key to avoid cross-table collisions.
func ComputeIntentHash(table, rowKey string) uint64 {
	h := xxhash.New()
	h.WriteString(table)
	h.WriteString(":")
	h.WriteString(rowKey)
	return h.Sum64()
}
