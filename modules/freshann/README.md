# freshann

`freshann` is a standalone Go submodule for Pebble-backed ANN indexing.

## Implemented

- Pluggable `Engine` / `Index` APIs
- Hard-cut V2 index format (`FormatVersion=2`) with explicit rejection of older formats
- Pebble idempotent apply tracking via `(txn_id, seq_id)` and `WaitApplied`
- Internal doc-ID mapping (`externalID <-> docID`) for compact query paths
- V2 split storage path: vector payloads (`vec/v2/<docID>`) and msgpack metadata (`meta/v2/<docID>`)
- Uint64 graph implementation (`pkg/graphv2`) with binary page persistence (`graph/v2/page/*`)
- Partition/tag filter postings and mutation-safe delete handling
- Search tuning controls (`EfSearch`, `Beam`, `CandidateBudget`, `RerankK`, worker controls)
- Pluggable budget policies (`adaptive` and `fixed`) resolved in-library for query-time tuning
- Optional exact fallback gate (disabled by default)
- Query diagnostics and resolved tuning in debug search responses
- Verification with deep graph/reference checks
- Core-6 ANN-Bench benchmark harness with `QPS@Recall>=0.90` reporting

## Key types

`IndexSpec` now includes:

- `FormatVersion` (`2` required)
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
`SearchRequest.Debug=true` returns resolved tuning and diagnostics in `SearchResult`.

`IndexStats` includes:

- `FallbackScans`
- `GraphPageReads`
- `VectorBlockReads`
- `QueryCount`
- `AvgCandidates`
- `AvgRerank`
- `VectorCacheHitRatio`
- `LastResolvedTuning`
- `QPS90Last`
- `RecallAt10Last`

## Quick start

```go
eng, _ := freshann.NewEngine(freshann.EngineOptions{RootDir: "/tmp/freshann"})
_, _ = eng.CreateIndex(context.Background(), freshann.IndexSpec{
    FormatVersion: 2,
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

## Core-6 comparison snapshot (excluding Milvus)

Snapshot date: 2026-02-22.

FreshANN numbers below are from local runs with:

- `topK=10`
- `base_count=20000`
- `query_count=200`
- `query_workers=4`
- `shard_workers=4`

Reference engine numbers are pulled from ANN-Bench dataset pages (`QPS@recall>=0.90`, `k=10`) and intentionally exclude Milvus in this section.

| Dataset | FreshANN recall@10 | FreshANN QPS@>=0.90 | pgvector | Qdrant | Weaviate | HNSWLib | HNSW(Faiss) | Faiss-IVF | RediSearch |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| glove-100-angular | 0.9500 | 1200.11 | 68.62 | 196.57 | 455.94 | 780.81 | 1151.54 | 483.91 | 1264.28 |
| sift-128-euclidean | 0.9955 | 1550.40 | 48.15 | 1101.41 | 1219.02 | 7934.70 | 3920.19 | 1421.66 | 3779.68 |
| fashion-mnist-784-euclidean | 1.0000 | 727.09 | 206.64 | 1170.26 | 1175.47 | 8178.48 | 6063.26 | 2711.60 | 4067.18 |
| nytimes-256-angular | 0.9035 | 1179.53 | 20.78 | 326.03 | 791.41 | 1545.87 | 618.36 | 0.00 | 0.00 |
| gist-960-euclidean | 0.9470 | 297.20 | 10.31 | n/a | 313.24 | 543.72 | 279.56 | 14.50 | n/a |
| glove-25-angular | 0.9970 | 3229.64 | 224.04 | 1525.69 | 1341.85 | 4164.03 | 6197.04 | 4078.53 | 5210.73 |

Comparison report sources:

- Local: `/tmp/freshann-bench-data/reports/core6-comprehensive-table.json`
- Internet matrix: `/tmp/freshann-bench-data/reports/internet-comparison-matrix.json`
- ANN-Bench pages:
  - <https://ann-benchmarks.com/glove-100-angular_10_angular.html>
  - <https://ann-benchmarks.com/sift-128-euclidean_10_euclidean.html>
  - <https://ann-benchmarks.com/fashion-mnist-784-euclidean_10_euclidean.html>
  - <https://ann-benchmarks.com/nytimes-256-angular_10_angular.html>
  - <https://ann-benchmarks.com/gist-960-euclidean_10_euclidean.html>
  - <https://ann-benchmarks.com/glove-25-angular_10_angular.html>

Important caveat: this is a directional comparison, not a strict apples-to-apples match, because local FreshANN runs and internet-reported engines do not share identical hardware or corpus size.

Planned follow-up benchmark track: run FreshANN against the same dataset families commonly used in Milvus DISKANN writeups (for example SIFT1M/GIST1M/DEEP1M-style corpus tracks) for tighter comparison parity.
