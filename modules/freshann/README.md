# freshann

`freshann` is a standalone Go submodule for Pebble-backed ANN indexing.

## Implemented

- Pluggable `Engine` / `Index` APIs
- Pebble idempotent apply tracking via `(txn_id, seq_id)` and `WaitApplied`
- Internal doc-ID mapping (`externalID <-> docID`) for compact query paths
- Partition/tag filter postings and mutation-safe delete handling
- Search tuning controls (`EfSearch`, `Beam`, `CandidateBudget`, `RerankK`, worker controls)
- Pluggable budget policies (`adaptive` and `fixed`) resolved in-library for query-time tuning
- Optional exact fallback gate (disabled by default)
- Verification with deep graph/reference checks
- Core-6 ANN-Bench benchmark harness with `QPS@Recall>=0.90` reporting

## Key types

`IndexSpec` now includes:

- `StorageSpec`
  - `PebbleCacheBytes`
  - `BloomBitsPerKey`
  - `VectorBlockSize`
  - `GraphPageSize`
  - `PostingChunkSize`
  - `EnableSSTIngest`
- `SearchDefaults`
  - `EfSearch`
  - `Beam`
  - `CandidateBudget`
  - `RerankK`
  - `ShardWorkers`
  - `TargetRecall`
  - `BudgetScale`
  - `AllowExactFallback`
- `BudgetPolicy`
  - `Mode` (`adaptive`, `fixed`)
  - `TargetRecall`
  - min/max clamp ranges for `EfSearch`, `Beam`, `CandidateBudget`, `RerankK`

`SearchRequest` supports per-query `Tuning` overrides.

`IndexStats` includes:

- `FallbackScans`
- `GraphPageReads`
- `VectorBlockReads`
- `QPS90Last`
- `RecallAt10Last`

## Quick start

```go
eng, _ := freshann.NewEngine(freshann.EngineOptions{RootDir: "/tmp/freshann"})
_, _ = eng.CreateIndex(context.Background(), freshann.IndexSpec{
    ID: "items_idx",
    Dim: 768,
    Metric: freshann.MetricCosine,
    DurabilityMode: freshann.DurabilityPeriodic,
    BudgetPolicy: freshann.BudgetPolicySpec{
        Mode: freshann.BudgetPolicyAdaptive,
        TargetRecall: 0.90,
    },
    SearchDefaults: freshann.SearchTuning{
        // Leave these zero to use adaptive library defaults from dim+metric.
        TargetRecall: 0.90,
    },
})
idx, _ := eng.OpenIndex(context.Background(), "items_idx")

_, _ = idx.Upsert(context.Background(), freshann.Mutation{
    TxnID: 1,
    SeqID: 1,
    ExternalID: []byte("row-1"),
    VectorFP32: make([]float32, 768),
})

res, _ := idx.Search(context.Background(), freshann.SearchRequest{
    VectorFP32: make([]float32, 768),
    TopK: 10,
    Tuning: freshann.SearchTuning{
        BudgetScale: 1.1, // planner-driven per-query adjustment
    },
})
_ = res
```

## Tests

```bash
go test ./...
```

## Core-6 benchmark run

```bash
./test/bench/run_standard_benchmarks.sh
```

All benchmark artifacts are written under `/tmp/freshann-bench-data` by default.
