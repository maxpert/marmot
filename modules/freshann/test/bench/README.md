# Baseline Bench Harness

This folder contains reproducible local baseline infrastructure for:

- Milvus
- Qdrant
- pgvector

## Bring up baselines

```bash
docker compose -f test/bench/docker-compose.baselines.yml up -d
```

## Planned benchmark workflow

1. Load shared datasets into all systems and freshann.
2. Execute standardized query sets.
3. Emit recall/latency/QPS reports.
4. Compare with fixed tier thresholds.

## Freshann standard dataset harness (no Docker required)

Runs on:

- ANN-Benchmarks `glove-100-angular`
- Big-ANN `bigann-10m` (configurable subset)

Persistent paths (default):

- datasets: `/tmp/freshann-bench-data/datasets`
- indexes: `/tmp/freshann-bench-data/indexes`
- reports: `/tmp/freshann-bench-data/reports`

Run:

```bash
./test/bench/run_standard_benchmarks.sh
```

Tune subset sizes:

```bash
ANN_BASE_COUNT=100000 ANN_QUERY_COUNT=2000 BIGANN_BASE_COUNT=250000 BIGANN_QUERY_COUNT=2000 ./test/bench/run_standard_benchmarks.sh
```
