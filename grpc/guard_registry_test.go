package grpc

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/filter"
)

// makeTestGuard creates a key-based guard for testing
func makeTestGuard(txnID uint64, table string, keys []uint64, ts int64) *ActiveGuard {
	f := filter.NewBloomFilterFromKeys(keys)
	return &ActiveGuard{
		TxnID:        txnID,
		Table:        table,
		Filter:       f,
		Keys:         keys,
		IsFilterOnly: false,
		Timestamp:    hlc.Timestamp{WallTime: ts, Logical: 0, NodeID: 1},
		RowCount:     int64(len(keys)),
	}
}

// makeFilterOnlyGuard creates a filter-only guard (no keys) for testing
func makeFilterOnlyGuard(txnID uint64, table string, keys []uint64, ts int64) *ActiveGuard {
	f := filter.NewBloomFilterFromKeys(keys)
	return &ActiveGuard{
		TxnID:        txnID,
		Table:        table,
		Filter:       f,
		Keys:         nil, // No keys for filter-only
		IsFilterOnly: true,
		Timestamp:    hlc.Timestamp{WallTime: ts, Logical: 0, NodeID: 1},
		RowCount:     int64(len(keys)),
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

// Filter-only guard tests

func TestGuardRegistry_FilterOnly_SinglePerTable(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register first filter-only guard
	guard1 := makeFilterOnlyGuard(100, "users", []uint64{1, 2, 3}, 1000)
	if err := r.Register(guard1); err != nil {
		t.Fatalf("First filter-only register failed: %v", err)
	}

	if !r.HasActiveFilterOnlyGuard("users") {
		t.Error("Should have active filter-only guard for users")
	}

	// Try to register second filter-only guard - should fail
	guard2 := makeFilterOnlyGuard(200, "users", []uint64{10, 20, 30}, 2000)
	err := r.Register(guard2)
	if err == nil {
		t.Error("Second filter-only guard should fail to register")
	}
}

func TestGuardRegistry_FilterOnly_DifferentTables(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register filter-only guard for users
	guard1 := makeFilterOnlyGuard(100, "users", []uint64{1, 2, 3}, 1000)
	if err := r.Register(guard1); err != nil {
		t.Fatalf("Register users failed: %v", err)
	}

	// Register filter-only guard for orders - should succeed (different table)
	guard2 := makeFilterOnlyGuard(200, "orders", []uint64{10, 20, 30}, 2000)
	if err := r.Register(guard2); err != nil {
		t.Fatalf("Register orders failed: %v", err)
	}

	if !r.HasActiveFilterOnlyGuard("users") {
		t.Error("Should have active filter-only guard for users")
	}
	if !r.HasActiveFilterOnlyGuard("orders") {
		t.Error("Should have active filter-only guard for orders")
	}
}

func TestGuardRegistry_FilterOnly_CheckConflict_WoundWait(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register filter-only guard (older)
	guard1 := makeFilterOnlyGuard(100, "users", []uint64{1, 2, 3}, 1000)
	r.Register(guard1)

	// New filter-only guard (younger) should conflict
	guard2 := makeFilterOnlyGuard(200, "users", []uint64{10, 20, 30}, 2000)
	result := r.CheckConflict(guard2)

	if !result.HasConflict {
		t.Error("Younger filter-only should conflict with older")
	}
	if !result.ShouldWait {
		t.Error("Younger should wait for older")
	}
}

func TestGuardRegistry_FilterOnly_KeyBasedCanProbe(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register filter-only guard with keys 1,2,3
	filterOnlyGuard := makeFilterOnlyGuard(100, "users", []uint64{1, 2, 3}, 1000)
	r.Register(filterOnlyGuard)

	// Key-based guard with non-overlapping keys should succeed
	keyGuard1 := makeTestGuard(200, "users", []uint64{10, 20, 30}, 2000)
	result := r.CheckConflict(keyGuard1)
	if result.HasConflict {
		t.Error("Non-overlapping key-based guard should not conflict")
	}

	// Key-based guard with overlapping keys should conflict
	keyGuard2 := makeTestGuard(300, "users", []uint64{2, 3, 4}, 3000)
	result = r.CheckConflict(keyGuard2)
	if !result.HasConflict {
		t.Error("Overlapping key-based guard should conflict with filter-only")
	}
}

func TestGuardRegistry_FilterOnly_ProbesKeyBasedGuards(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register key-based guard with keys 1,2,3
	keyGuard := makeTestGuard(100, "users", []uint64{1, 2, 3}, 1000)
	r.Register(keyGuard)

	// Filter-only guard with non-overlapping keys should succeed
	filterGuard1 := makeFilterOnlyGuard(200, "users", []uint64{10, 20, 30}, 2000)
	result := r.CheckConflict(filterGuard1)
	if result.HasConflict {
		t.Error("Non-overlapping filter-only guard should not conflict")
	}

	// Filter-only guard with overlapping keys should conflict
	filterGuard2 := makeFilterOnlyGuard(300, "users", []uint64{2, 3, 4}, 3000)
	result = r.CheckConflict(filterGuard2)
	if !result.HasConflict {
		t.Error("Overlapping filter-only guard should conflict with key-based")
	}
}

func TestGuardRegistry_MarkComplete_ReleasesFilterOnlySlot(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register filter-only guard
	guard1 := makeFilterOnlyGuard(100, "users", []uint64{1, 2, 3}, 1000)
	r.Register(guard1)

	if !r.HasActiveFilterOnlyGuard("users") {
		t.Error("Should have active filter-only guard")
	}

	// Mark complete
	r.MarkComplete(100)

	if r.HasActiveFilterOnlyGuard("users") {
		t.Error("MarkComplete should release filter-only slot")
	}

	// Now another filter-only guard can register
	guard2 := makeFilterOnlyGuard(200, "users", []uint64{10, 20, 30}, 2000)
	if err := r.Register(guard2); err != nil {
		t.Fatalf("Should be able to register after MarkComplete: %v", err)
	}
}

func TestGuardRegistry_Remove_ReleasesFilterOnlySlot(t *testing.T) {
	r := NewGuardRegistry(60 * time.Second)
	defer r.Stop()

	// Register filter-only guard
	guard1 := makeFilterOnlyGuard(100, "users", []uint64{1, 2, 3}, 1000)
	r.Register(guard1)

	// Remove
	r.Remove(100)

	if r.HasActiveFilterOnlyGuard("users") {
		t.Error("Remove should release filter-only slot")
	}

	// Now another filter-only guard can register
	guard2 := makeFilterOnlyGuard(200, "users", []uint64{10, 20, 30}, 2000)
	if err := r.Register(guard2); err != nil {
		t.Fatalf("Should be able to register after Remove: %v", err)
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
	r.Register(makeFilterOnlyGuard(200, "products", []uint64{3}, 2000))

	stats := r.Stats()

	if stats.TotalGuards != 3 {
		t.Errorf("TotalGuards = %d, want 3", stats.TotalGuards)
	}

	if stats.TotalTxns != 2 {
		t.Errorf("TotalTxns = %d, want 2", stats.TotalTxns)
	}

	if stats.FilterOnlyGuards != 1 {
		t.Errorf("FilterOnlyGuards = %d, want 1", stats.FilterOnlyGuards)
	}

	if stats.ActiveFilterOnlyCount != 1 {
		t.Errorf("ActiveFilterOnlyCount = %d, want 1", stats.ActiveFilterOnlyCount)
	}

	if stats.TableCounts["users"] != 1 {
		t.Errorf("TableCounts[users] = %d, want 1", stats.TableCounts["users"])
	}
}

func TestGuardRegistry_CleanupExpired(t *testing.T) {
	r := &GuardRegistry{
		guards:                make(map[string][]*ActiveGuard),
		byTxn:                 make(map[uint64][]*ActiveGuard),
		activeFilterOnlyGuard: make(map[string]*ActiveGuard),
		intentTTL:             100 * time.Millisecond,
		stopCh:                make(chan struct{}),
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

func TestGuardRegistry_CleanupExpired_ReleasesFilterOnlySlot(t *testing.T) {
	r := &GuardRegistry{
		guards:                make(map[string][]*ActiveGuard),
		byTxn:                 make(map[uint64][]*ActiveGuard),
		activeFilterOnlyGuard: make(map[string]*ActiveGuard),
		intentTTL:             100 * time.Millisecond,
		stopCh:                make(chan struct{}),
	}
	defer r.Stop()

	guard := makeFilterOnlyGuard(100, "users", []uint64{1}, 1000)
	guard.ExpiresAt = time.Now().Add(-1 * time.Second) // Already expired
	r.guards["users"] = append(r.guards["users"], guard)
	r.byTxn[100] = append(r.byTxn[100], guard)
	r.activeFilterOnlyGuard["users"] = guard

	r.cleanupExpired()

	if r.HasActiveFilterOnlyGuard("users") {
		t.Error("expired filter-only guard slot should be released")
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
