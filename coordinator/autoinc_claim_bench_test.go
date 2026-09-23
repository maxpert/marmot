package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
)

// BenchmarkClaimRange measures one uncontested claim end to end through the
// coordinator's 2PC against an in-process fake participant, so what it times
// is the claim's own work - building the statement, encoding the payload,
// pinning quorum, running PREPARE and COMMIT - and not a network.
//
// It is the allocation budget for the range allocator, which amortises
// one ClaimRange over a whole range of ids, so this cost is paid once per
// range, not once per insert.
func BenchmarkClaimRange(b *testing.B) {
	InitTestTelemetry()

	fake := newClaimStepReplicator()
	nodeProvider := newMockNodeProvider([]uint64{1})
	wc := NewWriteCoordinator(1, nodeProvider, fake, fake, 200*time.Millisecond, hlc.NewClock(1))
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := wc.ClaimRange(ctx, "testdb", "orders", uint64(i), 50); err != nil {
			b.Fatalf("ClaimRange: %v", err)
		}
	}
}
