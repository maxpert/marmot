package grpc

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// makeTestGuard creates a KeySet-based guard for testing
func makeTestGuard(txnID uint64, table string, keys []uint64, ts int64) *ActiveGuard {
	keySet := make(map[uint64]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	return &ActiveGuard{
		TxnID:     txnID,
		Table:     table,
		KeySet:    keySet,
		Timestamp: hlc.Timestamp{WallTime: ts, Logical: 0, NodeID: 1},
		RowCount:  int64(len(keys)),
	}
}

func TestGuardRegistry_Register(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	guard := makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000)
	if err := r.Register(guard); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if !r.HasGuard(100) {
		t.Error("HasGuard should return true after registration")
	}

	if r.Count() != 1 {
		t.Errorf("Count = %d, want 1", r.Count())
	}
}

func TestGuardRegistry_Remove(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	r.Register(makeTestGuard(100, "users", []uint64{1, 2}, 1000))
	r.Register(makeTestGuard(100, "orders", []uint64{3, 4}, 1000))

	if r.Count() != 2 {
		t.Errorf("Count = %d, want 2", r.Count())
	}

	r.Remove(100)

	if r.HasGuard(100) {
		t.Error("HasGuard should return false after removal")
	}

	if r.Count() != 0 {
		t.Errorf("Count = %d, want 0", r.Count())
	}
}

func TestGuardRegistry_CheckConflict_NoConflict(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Transaction 100 on keys 1,2,3
	r.Register(makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000))

	// Transaction 200 on keys 10,20,30 - no overlap
	newGuard := makeTestGuard(200, "users", []uint64{10, 20, 30}, 2000)
	result := r.CheckConflict(newGuard)

	if result.HasConflict {
		t.Error("should not detect conflict for non-overlapping keys")
	}
}

func TestGuardRegistry_CheckConflict_DifferentTables(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	r.Register(makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000))

	// Same keys but different table
	newGuard := makeTestGuard(200, "orders", []uint64{1, 2, 3}, 2000)
	result := r.CheckConflict(newGuard)

	if result.HasConflict {
		t.Error("should not detect conflict for different tables")
	}
}

func TestGuardRegistry_CheckConflict_WoundWait_OlderWins(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Transaction 100 is younger (ts=2000)
	r.Register(makeTestGuard(100, "users", []uint64{1, 2, 3}, 2000))

	// Transaction 200 is older (ts=1000) and conflicts
	newGuard := makeTestGuard(200, "users", []uint64{2, 3, 4}, 1000)
	result := r.CheckConflict(newGuard)

	if !result.HasConflict {
		t.Fatal("should detect conflict for overlapping keys")
	}

	if result.ShouldWait {
		t.Error("older transaction should NOT wait (wound-wait)")
	}

	if result.ConflictingTxn != 100 {
		t.Errorf("ConflictingTxn = %d, want 100", result.ConflictingTxn)
	}
}

func TestGuardRegistry_CheckConflict_WoundWait_YoungerWaits(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Transaction 100 is older (ts=1000)
	r.Register(makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000))

	// Transaction 200 is younger (ts=2000) and conflicts
	newGuard := makeTestGuard(200, "users", []uint64{2, 3, 4}, 2000)
	result := r.CheckConflict(newGuard)

	if !result.HasConflict {
		t.Fatal("should detect conflict for overlapping keys")
	}

	if !result.ShouldWait {
		t.Error("younger transaction should wait (wound-wait)")
	}

	if result.ConflictingTxn != 100 {
		t.Errorf("ConflictingTxn = %d, want 100", result.ConflictingTxn)
	}
}

func TestGuardRegistry_CheckConflict_SameTxn(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	r.Register(makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000))

	// Same transaction ID - should not conflict with itself
	newGuard := makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000)
	result := r.CheckConflict(newGuard)

	if result.HasConflict {
		t.Error("transaction should not conflict with itself")
	}
}

func TestGuardRegistry_RefreshTTL(t *testing.T) {
	r := NewGuardRegistry(1 * time.Second)
	defer r.Stop()

	guard := makeTestGuard(100, "users", []uint64{1}, 1000)
	r.Register(guard)

	originalExpiry := guard.ExpiresAt

	time.Sleep(100 * time.Millisecond)
	r.RefreshTTL(100)

	guards := r.GetAllGuards(100)
	if len(guards) != 1 {
		t.Fatal("should have 1 guard")
	}

	if !guards[0].ExpiresAt.After(originalExpiry) {
		t.Error("ExpiresAt should be extended after RefreshTTL")
	}
}

func TestGuardRegistry_GetGuard(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	r.Register(makeTestGuard(100, "users", []uint64{1}, 1000))
	r.Register(makeTestGuard(100, "orders", []uint64{2}, 1000))

	guard := r.GetGuard(100, "users")
	if guard == nil {
		t.Fatal("GetGuard should return guard")
	}
	if guard.Table != "users" {
		t.Errorf("guard.Table = %s, want users", guard.Table)
	}

	guard = r.GetGuard(100, "products")
	if guard != nil {
		t.Error("GetGuard should return nil for non-existent table")
	}

	guard = r.GetGuard(999, "users")
	if guard != nil {
		t.Error("GetGuard should return nil for non-existent txn")
	}
}

func TestGuardRegistry_GetAllGuards(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	r.Register(makeTestGuard(100, "users", []uint64{1}, 1000))
	r.Register(makeTestGuard(100, "orders", []uint64{2}, 1000))
	r.Register(makeTestGuard(200, "users", []uint64{3}, 2000))

	guards := r.GetAllGuards(100)
	if len(guards) != 2 {
		t.Errorf("GetAllGuards(100) returned %d guards, want 2", len(guards))
	}

	guards = r.GetAllGuards(200)
	if len(guards) != 1 {
		t.Errorf("GetAllGuards(200) returned %d guards, want 1", len(guards))
	}

	guards = r.GetAllGuards(999)
	if len(guards) != 0 {
		t.Errorf("GetAllGuards(999) returned %d guards, want 0", len(guards))
	}
}

func TestGuardRegistry_Stats(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	r.Register(makeTestGuard(100, "users", []uint64{1}, 1000))
	r.Register(makeTestGuard(100, "orders", []uint64{2}, 1000))
	r.Register(makeTestGuard(200, "products", []uint64{3}, 2000))

	stats := r.Stats()

	if stats.TotalGuards != 3 {
		t.Errorf("TotalGuards = %d, want 3", stats.TotalGuards)
	}

	if stats.TotalTxns != 2 {
		t.Errorf("TotalTxns = %d, want 2", stats.TotalTxns)
	}

	if stats.TableCounts["users"] != 1 {
		t.Errorf("TableCounts[users] = %d, want 1", stats.TableCounts["users"])
	}
}

func TestGuardRegistry_CleanupExpired(t *testing.T) {
	r := &GuardRegistry{
		guards:    make(map[string][]*ActiveGuard),
		byTxn:     make(map[uint64][]*ActiveGuard),
		intentTTL: 100 * time.Millisecond,
		stopCh:    make(chan struct{}),
	}
	defer r.Stop()

	guard := makeTestGuard(100, "users", []uint64{1}, 1000)
	guard.ExpiresAt = time.Now().Add(-1 * time.Second) // Already expired
	r.guards["users"] = append(r.guards["users"], guard)
	r.byTxn[100] = append(r.byTxn[100], guard)

	r.cleanupExpired()

	if r.HasGuard(100) {
		t.Error("expired guard should be cleaned up")
	}

	if r.Count() != 0 {
		t.Errorf("Count = %d, want 0", r.Count())
	}
}

func TestGuardRegistry_MultipleTablesConflict(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Transaction updates multiple tables
	r.Register(makeTestGuard(100, "users", []uint64{1, 2}, 1000))
	r.Register(makeTestGuard(100, "accounts", []uint64{10, 20}, 1000))

	// New transaction conflicts on users table
	usersGuard := makeTestGuard(200, "users", []uint64{2, 3}, 2000)
	result := r.CheckConflict(usersGuard)

	if !result.HasConflict {
		t.Error("should detect conflict on users table")
	}

	// New transaction doesn't conflict on products table
	productsGuard := makeTestGuard(200, "products", []uint64{100, 200}, 2000)
	result = r.CheckConflict(productsGuard)

	if result.HasConflict {
		t.Error("should not detect conflict on products table")
	}
}

// Test KeySetFromSlice helper
func TestKeySetFromSlice(t *testing.T) {
	hashes := []uint64{1, 2, 3, 4, 5}
	keySet := KeySetFromSlice(hashes)

	if len(keySet) != 5 {
		t.Errorf("len(keySet) = %d, want 5", len(keySet))
	}

	for _, h := range hashes {
		if _, ok := keySet[h]; !ok {
			t.Errorf("keySet missing hash %d", h)
		}
	}
}

func TestKeySetFromSlice_Empty(t *testing.T) {
	keySet := KeySetFromSlice(nil)
	if len(keySet) != 0 {
		t.Errorf("len(keySet) = %d, want 0", len(keySet))
	}

	keySet = KeySetFromSlice([]uint64{})
	if len(keySet) != 0 {
		t.Errorf("len(keySet) = %d, want 0", len(keySet))
	}
}

// Test hasIntersection helper
func TestHasIntersection(t *testing.T) {
	a := map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	b := map[uint64]struct{}{3: {}, 4: {}, 5: {}}

	if !hasIntersection(a, b) {
		t.Error("should detect intersection at key 3")
	}
}

func TestHasIntersection_NoOverlap(t *testing.T) {
	a := map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	b := map[uint64]struct{}{4: {}, 5: {}, 6: {}}

	if hasIntersection(a, b) {
		t.Error("should not detect intersection for non-overlapping sets")
	}
}

func TestHasIntersection_Empty(t *testing.T) {
	a := map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	b := map[uint64]struct{}{}

	if hasIntersection(a, b) {
		t.Error("should not detect intersection with empty set")
	}

	if hasIntersection(b, a) {
		t.Error("should not detect intersection with empty set (reversed)")
	}
}

func TestHasIntersection_BothEmpty(t *testing.T) {
	a := map[uint64]struct{}{}
	b := map[uint64]struct{}{}

	if hasIntersection(a, b) {
		t.Error("should not detect intersection between empty sets")
	}
}

// Test exact conflict detection (no false positives)
func TestExactConflictDetection_NoFalsePositives(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register guard with specific keys
	r.Register(makeTestGuard(100, "users", []uint64{1000, 2000, 3000}, 1000))

	// Test many non-overlapping keys - none should conflict
	for i := uint64(1); i < 100; i++ {
		// These keys will never overlap with 1000, 2000, 3000
		keys := []uint64{i, i + 100, i + 200}
		guard := makeTestGuard(200, "users", keys, 2000)
		result := r.CheckConflict(guard)

		if result.HasConflict {
			t.Errorf("false positive detected for keys %v", keys)
		}
	}

	// Now test overlapping key - must detect
	guard := makeTestGuard(200, "users", []uint64{1000, 5000, 6000}, 2000)
	result := r.CheckConflict(guard)

	if !result.HasConflict {
		t.Error("should detect conflict for overlapping key 1000")
	}
}

// Benchmarks

func BenchmarkCheckConflict_NoConflict(b *testing.B) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register 100 guards
	for i := uint64(0); i < 100; i++ {
		keys := []uint64{i * 1000, i*1000 + 1, i*1000 + 2}
		r.Register(makeTestGuard(i, "users", keys, int64(i)))
	}

	// Check non-conflicting guard
	guard := makeTestGuard(999, "users", []uint64{999999, 999998, 999997}, 999)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.CheckConflict(guard)
	}
}

func BenchmarkCheckConflict_WithConflict(b *testing.B) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register 100 guards
	for i := uint64(0); i < 100; i++ {
		keys := []uint64{i * 1000, i*1000 + 1, i*1000 + 2}
		r.Register(makeTestGuard(i, "users", keys, int64(i)))
	}

	// Check conflicting guard (conflicts with first guard)
	guard := makeTestGuard(999, "users", []uint64{0, 1, 2}, 999)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.CheckConflict(guard)
	}
}

func BenchmarkRegister(b *testing.B) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	guards := make([]*ActiveGuard, b.N)
	for i := 0; i < b.N; i++ {
		guards[i] = makeTestGuard(uint64(i), "users", []uint64{uint64(i)}, int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Register(guards[i])
	}
}

func BenchmarkHasIntersection_SmallSets(b *testing.B) {
	a := make(map[uint64]struct{}, 10)
	c := make(map[uint64]struct{}, 10)
	for i := uint64(0); i < 10; i++ {
		a[i] = struct{}{}
		c[i+100] = struct{}{} // Non-overlapping
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasIntersection(a, c)
	}
}

func BenchmarkHasIntersection_LargeSets(b *testing.B) {
	a := make(map[uint64]struct{}, 10000)
	c := make(map[uint64]struct{}, 10000)
	for i := uint64(0); i < 10000; i++ {
		a[i] = struct{}{}
		c[i+100000] = struct{}{} // Non-overlapping
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasIntersection(a, c)
	}
}
