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

			// Build output with optional tx stats
			if snapshot.TxCount > 0 {
				txSec := snapshot.TxCount - lastSnapshot.TxCount
				cumTxThroughput := float64(snapshot.TxCount) / elapsed.Seconds()
				fmt.Printf("[%5.0fs] ops/sec: %6d | tx/sec: %5d | total: %8d | tx: %6d | errors: %4d | retries: %4d | throughput: %.1f ops/sec | %.1f tx/sec\n",
					elapsed.Seconds(),
					opsSec,
					txSec,
					currentTotal,
					snapshot.TxCount,
					snapshot.Errors+snapshot.TxErrors,
					snapshot.Retries+snapshot.TxRetries,
					cumThroughput,
					cumTxThroughput,
				)
			} else {
				fmt.Printf("[%5.0fs] ops/sec: %6d | total: %8d | errors: %4d | retries: %4d | throughput: %.1f ops/sec\n",
					elapsed.Seconds(),
					opsSec,
					currentTotal,
					snapshot.Errors,
					snapshot.Retries,
					cumThroughput,
				)
			}

			lastSnapshot = snapshot
		}
	}
}
