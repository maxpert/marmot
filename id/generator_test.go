package id

import (
	"sync"
	"testing"

	"github.com/maxpert/marmot/hlc"
)

func TestHLCGenerator_NextIDs_Uniqueness(t *testing.T) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	seen := make(map[uint64]bool)
	const iterations = 10000

	for i := 0; i < iterations; i++ {
		id := oneID(gen)
		if seen[id] {
			t.Fatalf("duplicate ID generated at iteration %d: %d", i, id)
		}
		seen[id] = true
	}
}

func TestHLCGenerator_NextIDs_Monotonic(t *testing.T) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	var prev uint64
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		id := oneID(gen)
		if id <= prev {
			t.Fatalf("non-monotonic ID at iteration %d: prev=%d, curr=%d", i, prev, id)
		}
		prev = id
	}
}

func TestHLCGenerator_NextIDs_Concurrent(t *testing.T) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	const goroutines = 10
	const idsPerGoroutine = 1000

	var wg sync.WaitGroup
	idsChan := make(chan uint64, goroutines*idsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < idsPerGoroutine; i++ {
				idsChan <- oneID(gen)
			}
		}()
	}

	wg.Wait()
	close(idsChan)

	seen := make(map[uint64]bool)
	for id := range idsChan {
		if seen[id] {
			t.Fatalf("duplicate ID in concurrent test: %d", id)
		}
		seen[id] = true
	}

	if len(seen) != goroutines*idsPerGoroutine {
		t.Fatalf("expected %d unique IDs, got %d", goroutines*idsPerGoroutine, len(seen))
	}
}

func TestHLCGenerator_DifferentNodes(t *testing.T) {
	clock1 := hlc.NewClock(1)
	clock2 := hlc.NewClock(2)
	gen1 := NewHLCGenerator(clock1)
	gen2 := NewHLCGenerator(clock2)

	id1 := oneID(gen1)
	id2 := oneID(gen2)

	if id1 == id2 {
		t.Fatalf("IDs from different nodes should differ: %d == %d", id1, id2)
	}

	// Extract node IDs from generated IDs (bits 16-21)
	nodeID1 := (id1 >> 16) & 0x3F
	nodeID2 := (id2 >> 16) & 0x3F

	if nodeID1 != 1 {
		t.Errorf("expected node ID 1 in id1, got %d", nodeID1)
	}
	if nodeID2 != 2 {
		t.Errorf("expected node ID 2 in id2, got %d", nodeID2)
	}
}

func BenchmarkHLCGenerator_NextIDs(b *testing.B) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oneID(gen)
	}
}

func BenchmarkHLCGenerator_NextIDs_Parallel(b *testing.B) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			oneID(gen)
		}
	})
}

func TestCompactGenerator_NextIDs_Uniqueness(t *testing.T) {
	gen := NewCompactGenerator(1)

	seen := make(map[uint64]bool)
	const iterations = 10000

	for i := 0; i < iterations; i++ {
		id := oneID(gen)
		if seen[id] {
			t.Fatalf("duplicate ID generated at iteration %d: %d", i, id)
		}
		seen[id] = true
	}
}

func TestCompactGenerator_NextIDs_Monotonic(t *testing.T) {
	gen := NewCompactGenerator(1)

	var prev uint64
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		id := oneID(gen)
		if id <= prev {
			t.Fatalf("non-monotonic ID at iteration %d: prev=%d, curr=%d", i, prev, id)
		}
		prev = id
	}
}

func TestCompactGenerator_NextIDs_Concurrent(t *testing.T) {
	gen := NewCompactGenerator(1)

	const goroutines = 10
	const idsPerGoroutine = 1000

	var wg sync.WaitGroup
	idsChan := make(chan uint64, goroutines*idsPerGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < idsPerGoroutine; i++ {
				idsChan <- oneID(gen)
			}
		}()
	}

	wg.Wait()
	close(idsChan)

	seen := make(map[uint64]bool)
	for id := range idsChan {
		if seen[id] {
			t.Fatalf("duplicate ID in concurrent test: %d", id)
		}
		seen[id] = true
	}

	if len(seen) != goroutines*idsPerGoroutine {
		t.Fatalf("expected %d unique IDs, got %d", goroutines*idsPerGoroutine, len(seen))
	}
}

func TestCompactGenerator_DifferentNodes(t *testing.T) {
	gen1 := NewCompactGenerator(1)
	gen2 := NewCompactGenerator(2)

	id1 := oneID(gen1)
	id2 := oneID(gen2)

	if id1 == id2 {
		t.Fatalf("IDs from different nodes should differ: %d == %d", id1, id2)
	}

	// Extract node IDs from generated IDs (bits 6-11)
	nodeID1 := (id1 >> CompactNodeShift) & CompactNodeMask
	nodeID2 := (id2 >> CompactNodeShift) & CompactNodeMask

	if nodeID1 != 1 {
		t.Errorf("expected node ID 1 in id1, got %d", nodeID1)
	}
	if nodeID2 != 2 {
		t.Errorf("expected node ID 2 in id2, got %d", nodeID2)
	}
}

func TestCompactGenerator_MaxValue(t *testing.T) {
	const maxSafeInt = (1 << 53) - 1 // 9007199254740991

	gen := NewCompactGenerator(63) // Max node ID

	for i := 0; i < 10000; i++ {
		id := oneID(gen)
		if id > maxSafeInt {
			t.Fatalf("ID %d exceeds MAX_SAFE_INTEGER %d at iteration %d", id, maxSafeInt, i)
		}
	}
}

func TestCompactGenerator_SequenceOverflow(t *testing.T) {
	gen := NewCompactGenerator(1)

	// Generate more than CompactSeqMax (63) IDs in a tight loop
	// This should trigger sequence overflow and wait for next millisecond
	const iterations = 100

	seen := make(map[uint64]bool)
	for i := 0; i < iterations; i++ {
		id := oneID(gen)
		if seen[id] {
			t.Fatalf("duplicate ID at iteration %d: %d", i, id)
		}
		seen[id] = true
	}

	if len(seen) != iterations {
		t.Fatalf("expected %d unique IDs, got %d", iterations, len(seen))
	}
}

func TestCompactGenerator_NodeIDMasking(t *testing.T) {
	// Test that node IDs > 63 are properly masked
	gen := NewCompactGenerator(127) // Should be masked to 63

	id := oneID(gen)
	nodeID := (id >> CompactNodeShift) & CompactNodeMask

	if nodeID != 63 {
		t.Errorf("expected node ID 63 (masked from 127), got %d", nodeID)
	}
}

func BenchmarkCompactGenerator_NextIDs(b *testing.B) {
	gen := NewCompactGenerator(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		oneID(gen)
	}
}

func BenchmarkCompactGenerator_NextIDs_Parallel(b *testing.B) {
	gen := NewCompactGenerator(1)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			oneID(gen)
		}
	})
}

// oneID mints a single id. Neither generator here can fail, so an error is a
// broken invariant; it panics rather than calling t.Fatal so that it fails
// the test from any goroutine.
func oneID(g Generator) uint64 {
	var ids [1]uint64
	if err := g.NextIDs(ids[:]); err != nil {
		panic(err)
	}
	return ids[0]
}

// TestGenerators_NextIDsBatchIsAscendingAndDisjoint pins the multi-row form:
// one call returns ascending ids, and batches minted concurrently never share
// an id.
func TestGenerators_NextIDsBatchIsAscendingAndDisjoint(t *testing.T) {
	gens := map[string]Generator{
		"hlc":     NewHLCGenerator(hlc.NewClock(1)),
		"compact": NewCompactGenerator(1),
	}
	for name, gen := range gens {
		t.Run(name, func(t *testing.T) {
			const goroutines, batches, batchSize = 8, 50, 7
			var mu sync.Mutex
			seen := make(map[uint64]bool, goroutines*batches*batchSize)
			var wg sync.WaitGroup
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for b := 0; b < batches; b++ {
						ids := make([]uint64, batchSize)
						if err := gen.NextIDs(ids); err != nil {
							t.Errorf("NextIDs: %v", err)
							return
						}
						mu.Lock()
						for i, id := range ids {
							if i > 0 && id <= ids[i-1] {
								t.Errorf("batch not ascending: %v", ids)
							}
							if seen[id] {
								t.Errorf("id %d minted twice", id)
							}
							seen[id] = true
						}
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			if len(seen) != goroutines*batches*batchSize {
				t.Fatalf("minted %d distinct ids, want %d", len(seen), goroutines*batches*batchSize)
			}
		})
	}
}
