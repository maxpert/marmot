package id

import (
	"sync"
	"testing"

	"github.com/maxpert/marmot/hlc"
)

func TestHLCGenerator_NextID_Uniqueness(t *testing.T) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	seen := make(map[uint64]bool)
	const iterations = 10000

	for i := 0; i < iterations; i++ {
		id := gen.NextID()
		if seen[id] {
			t.Fatalf("duplicate ID generated at iteration %d: %d", i, id)
		}
		seen[id] = true
	}
}

func TestHLCGenerator_NextID_Monotonic(t *testing.T) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	var prev uint64
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		id := gen.NextID()
		if id <= prev {
			t.Fatalf("non-monotonic ID at iteration %d: prev=%d, curr=%d", i, prev, id)
		}
		prev = id
	}
}

func TestHLCGenerator_NextID_Concurrent(t *testing.T) {
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
				idsChan <- gen.NextID()
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

	id1 := gen1.NextID()
	id2 := gen2.NextID()

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

func BenchmarkHLCGenerator_NextID(b *testing.B) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gen.NextID()
	}
}

func BenchmarkHLCGenerator_NextID_Parallel(b *testing.B) {
	clock := hlc.NewClock(1)
	gen := NewHLCGenerator(clock)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			gen.NextID()
		}
	})
}
