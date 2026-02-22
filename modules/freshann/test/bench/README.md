# Benchmark Harness

## External baseline stack (optional)

This folder contains reproducible local baseline infrastructure for:

- Milvus
- Qdrant
- pgvector

Bring up baselines:

```bash
docker compose -f test/bench/docker-compose.baselines.yml up -d
```

## Freshann Core-6 ANN-Bench harness

The standard run targets ANN-Bench Core-6:

- `glove-100-angular`
- `sift-128-euclidean`
- `fashion-mnist-784-euclidean`
- `nytimes-256-angular`
- `gist-960-euclidean`
- `glove-25-angular`

Persistent paths (default):

- datasets: `/tmp/freshann-bench-data/datasets`
- indexes: `/tmp/freshann-bench-data/indexes`
- reports: `/tmp/freshann-bench-data/reports`

Run:

```bash
./test/bench/run_standard_benchmarks.sh
```

Common tuning controls:

```bash
BASE_COUNT=50000 QUERY_COUNT=1000 EFSEARCH_LIST=96,160 BEAM_LIST=8,16 CANDIDATE_BUDGET_LIST=512,1024 RERANK_LIST=64,128 ./test/bench/run_standard_benchmarks.sh
```

Default tuning mode is `auto`, driven by the in-library adaptive budget policy (`pkg/budget`) and its sweep grid generator. Override with explicit lists via env vars above.

Baseline freeze / compare:

```bash
FREEZE_BASELINE=1 ./test/bench/run_standard_benchmarks.sh
```

Outputs include per-dataset reports plus:

- `/tmp/freshann-bench-data/reports/baseline-core6.json`
- `/tmp/freshann-bench-data/reports/core6-comparison-matrix.json`
- `/tmp/freshann-bench-data/reports/internet-comparison-matrix.json`
- `/tmp/freshann-bench-data/reports/core6-comprehensive-table.json`
- `/tmp/freshann-bench-data/reports/core6-comprehensive-table.md`
