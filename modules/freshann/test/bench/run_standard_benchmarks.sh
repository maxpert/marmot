#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

DATA_ROOT="${DATA_ROOT:-/tmp/freshann-bench-data}"
DATASET_ROOT="${DATASET_ROOT:-$DATA_ROOT/datasets}"
INDEX_ROOT="${INDEX_ROOT:-$DATA_ROOT/indexes}"
REPORT_ROOT="${REPORT_ROOT:-$DATA_ROOT/reports}"

ANN_BASE_COUNT="${ANN_BASE_COUNT:-50000}"
ANN_QUERY_COUNT="${ANN_QUERY_COUNT:-1000}"
BIGANN_BASE_COUNT="${BIGANN_BASE_COUNT:-100000}"
BIGANN_QUERY_COUNT="${BIGANN_QUERY_COUNT:-1000}"
TOPK="${TOPK:-10}"

mkdir -p "$DATASET_ROOT" "$INDEX_ROOT" "$REPORT_ROOT"
VENV_DIR="${VENV_DIR:-$DATA_ROOT/venv}"

echo "[freshann] ensuring python deps for dataset prep"
if [[ ! -x "$VENV_DIR/bin/python3" ]]; then
  python3 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/python3" -m pip install --upgrade pip >/dev/null
"$VENV_DIR/bin/python3" -m pip install numpy h5py >/dev/null

echo "[freshann] downloading + preparing datasets in $DATASET_ROOT"
"$VENV_DIR/bin/python3" test/bench/download_standard_datasets.py \
  --root "$DATASET_ROOT" \
  --ann-base-count "$ANN_BASE_COUNT" \
  --ann-query-count "$ANN_QUERY_COUNT" \
  --bigann-base-count "$BIGANN_BASE_COUNT" \
  --bigann-query-count "$BIGANN_QUERY_COUNT"

echo "[freshann] running ANN-Bench (glove-100-angular)"
go run ./cmd/datasetbench \
  --dataset-dir "$DATASET_ROOT/annbench-glove-100-angular" \
  --index-root "$INDEX_ROOT" \
  --topk "$TOPK" \
  --out "$REPORT_ROOT/annbench-glove-100-angular.json"

echo "[freshann] running Big-ANN (bigann-10m subset)"
go run ./cmd/datasetbench \
  --dataset-dir "$DATASET_ROOT/bigann-10m" \
  --index-root "$INDEX_ROOT" \
  --topk "$TOPK" \
  --out "$REPORT_ROOT/bigann-10m.json"

echo "[freshann] reports:"
ls -1 "$REPORT_ROOT"/*.json
