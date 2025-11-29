package main

import (
	"context"
	"fmt"
	"time"
)

// reportProgress prints real-time progress every second.
func reportProgress(ctx context.Context, stats *Stats, workload string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSnapshot Snapshot
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := stats.GetSnapshot()
			elapsed := time.Since(startTime)

			// Calculate ops/sec since last snapshot
			currentTotal := snapshot.ReadOps + snapshot.UpdateOps + snapshot.InsertOps + snapshot.DeleteOps + snapshot.UpsertOps
			lastTotal := lastSnapshot.ReadOps + lastSnapshot.UpdateOps + lastSnapshot.InsertOps + lastSnapshot.DeleteOps + lastSnapshot.UpsertOps
			opsSec := currentTotal - lastTotal

			// Calculate cumulative throughput
			cumThroughput := float64(currentTotal) / elapsed.Seconds()

			fmt.Printf("[%5.0fs] ops/sec: %6d | total: %8d | errors: %4d | retries: %4d | throughput: %.1f ops/sec\n",
				elapsed.Seconds(),
				opsSec,
				currentTotal,
				snapshot.Errors,
				snapshot.Retries,
				cumThroughput,
			)

			lastSnapshot = snapshot
		}
	}
}
