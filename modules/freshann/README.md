# freshann

`freshann` is a standalone Go submodule for disk-first vector indexing inspired by DiskANN/FreshDiskANN patterns.

## Implemented in this module

- Pluggable `Engine`/`Index` APIs
- Pebble-backed metadata + idempotent mutation apply tracking (`txn_id`, `seq_id`)
- Direct Pebble apply before ack (no intermediate mutation queue)
- `WaitApplied` API for explicit apply confirmation
- One physical index per table/index ID
- Segment snapshots + atomic manifest updates
- Pebble-backed graph persistence and graph-assisted search candidate generation
- Partition + tag filter posting indexes backed by Roaring bitmaps
- Cosine/dot/euclidean scoring with exact rerank
- Verification (`Verify`) with deep graph/reference checks
- Benchmark harness primitives and reproducible baseline docker stack for Milvus/Qdrant/pgvector

## Quick start

```go
eng, _ := freshann.NewEngine(freshann.EngineOptions{RootDir: "/tmp/freshann"})
_, _ = eng.CreateIndex(context.Background(), freshann.IndexSpec{
    ID: "my_table_idx",
    Dim: 768,
    Metric: freshann.MetricCosine,
    DurabilityMode: freshann.DurabilityPeriodic,
})
idx, _ := eng.OpenIndex(context.Background(), "my_table_idx")

tok, _ := idx.Upsert(context.Background(), freshann.Mutation{
    TxnID: 1,
    SeqID: 1,
    ExternalID: []byte("row-1"),
    VectorFP32: make([]float32, 768),
    PartitionKey: "tenant-1",
    Tags: map[string]string{"lang": "en"},
})

_ = idx.WaitApplied(context.Background(), tok)
res, _ := idx.Search(context.Background(), freshann.SearchRequest{
    VectorFP32: make([]float32, 768),
    TopK: 10,
    PartitionKey: "tenant-1",
    Tags: map[string]string{"lang": "en"},
})
_ = res
```

## Run tests

```bash
go test ./...
```

## Run local baseline stack

```bash
./test/bench/run_local_baselines.sh
```
