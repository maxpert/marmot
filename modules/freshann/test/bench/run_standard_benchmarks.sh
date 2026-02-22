#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

DATA_ROOT="${DATA_ROOT:-/tmp/freshann-bench-data}"
DATASET_ROOT="${DATASET_ROOT:-$DATA_ROOT/datasets}"
INDEX_ROOT="${INDEX_ROOT:-$DATA_ROOT/indexes}"
REPORT_ROOT="${REPORT_ROOT:-$DATA_ROOT/reports}"

BASE_COUNT="${BASE_COUNT:-0}"
QUERY_COUNT="${QUERY_COUNT:-0}"
TOPK="${TOPK:-10}"
QUERY_WORKERS="${QUERY_WORKERS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
SHARD_WORKERS="${SHARD_WORKERS:-4}"
ALLOW_EXACT_FALLBACK="${ALLOW_EXACT_FALLBACK:-0}"
EFSEARCH_LIST="${EFSEARCH_LIST:-auto}"
BEAM_LIST="${BEAM_LIST:-auto}"
CANDIDATE_BUDGET_LIST="${CANDIDATE_BUDGET_LIST:-auto}"
RERANK_LIST="${RERANK_LIST:-auto}"

BASELINE_REPORT="${BASELINE_REPORT:-$REPORT_ROOT/baseline-core6.json}"
MATRIX_REPORT="${MATRIX_REPORT:-$REPORT_ROOT/core6-comparison-matrix.json}"
FREEZE_BASELINE="${FREEZE_BASELINE:-0}"

mkdir -p "$DATASET_ROOT" "$INDEX_ROOT" "$REPORT_ROOT"
VENV_DIR="${VENV_DIR:-$DATA_ROOT/venv}"

echo "[freshann] ensuring python deps for dataset prep"
if [[ ! -x "$VENV_DIR/bin/python3" ]]; then
  python3 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/python3" -m pip install --upgrade pip >/dev/null
"$VENV_DIR/bin/python3" -m pip install numpy h5py >/dev/null

echo "[freshann] downloading + preparing ANN-Bench Core-6 datasets in $DATASET_ROOT"
"$VENV_DIR/bin/python3" test/bench/download_standard_datasets.py \
  --root "$DATASET_ROOT" \
  --base-count "$BASE_COUNT" \
  --query-count "$QUERY_COUNT"

datasets=(
  glove-100-angular
  sift-128-euclidean
  fashion-mnist-784-euclidean
  nytimes-256-angular
  gist-960-euclidean
  glove-25-angular
)

for ds in "${datasets[@]}"; do
  echo "[freshann] running dataset: $ds"
  out="$REPORT_ROOT/$ds.json"
  go run ./cmd/datasetbench \
    --dataset-dir "$DATASET_ROOT/$ds" \
    --index-root "$INDEX_ROOT" \
    --topk "$TOPK" \
    --query-workers "$QUERY_WORKERS" \
    --shard-workers "$SHARD_WORKERS" \
    --efsearch-list "$EFSEARCH_LIST" \
    --beam-list "$BEAM_LIST" \
    --candidate-budget-list "$CANDIDATE_BUDGET_LIST" \
    --rerank-list "$RERANK_LIST" \
    --allow-exact-fallback=$([[ "$ALLOW_EXACT_FALLBACK" == "1" ]] && echo true || echo false) \
    --out "$out"
done

echo "[freshann] building comparison matrix"
python3 - "$REPORT_ROOT" "$BASELINE_REPORT" "$MATRIX_REPORT" "$FREEZE_BASELINE" <<'PY'
import json
import os
import sys
from pathlib import Path

report_root = Path(sys.argv[1])
baseline_path = Path(sys.argv[2])
matrix_path = Path(sys.argv[3])
freeze_baseline = sys.argv[4] == "1"

datasets = [
    "glove-100-angular",
    "sift-128-euclidean",
    "fashion-mnist-784-euclidean",
    "nytimes-256-angular",
    "gist-960-euclidean",
    "glove-25-angular",
]

current = {}
for ds in datasets:
    p = report_root / f"{ds}.json"
    if not p.exists():
        continue
    with p.open() as f:
        current[ds] = json.load(f)

if freeze_baseline or not baseline_path.exists():
    baseline = {ds: current.get(ds) for ds in datasets if ds in current}
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(json.dumps(baseline, indent=2))
else:
    with baseline_path.open() as f:
        baseline = json.load(f)

rows = []
for ds in datasets:
    cur = current.get(ds)
    base = baseline.get(ds)
    if not cur:
        continue
    row = {
        "dataset": ds,
        "recall_at_k": cur.get("recall_at_k", 0.0),
        "qps": cur.get("qps", 0.0),
        "qps_at_recall_90": cur.get("qps_at_recall_90", 0.0),
        "p95_ms": cur.get("p95_ms", 0.0),
        "pass_recall_090": cur.get("recall_at_k", 0.0) >= 0.90,
        "baseline_qps_at_recall_90": (base or {}).get("qps_at_recall_90", 0.0),
        "improved_vs_baseline": cur.get("qps_at_recall_90", 0.0) > ((base or {}).get("qps_at_recall_90", 0.0)),
    }
    rows.append(row)

matrix = {
    "datasets": rows,
    "all_pass_recall_090": all(r["pass_recall_090"] for r in rows) if rows else False,
    "all_improved_vs_baseline": all(r["improved_vs_baseline"] for r in rows) if rows else False,
}

matrix_path.write_text(json.dumps(matrix, indent=2))
print(json.dumps(matrix, indent=2))
PY

echo "[freshann] building internet comparison matrix from ANN-Bench pages"
python3 test/bench/build_internet_comparison.py \
  --report-root "$REPORT_ROOT" \
  --out "$REPORT_ROOT/internet-comparison-matrix.json"

echo "[freshann] building comprehensive table"
python3 test/bench/build_comprehensive_table.py \
  --report-root "$REPORT_ROOT" \
  --internet "$REPORT_ROOT/internet-comparison-matrix.json" \
  --out-json "$REPORT_ROOT/core6-comprehensive-table.json" \
  --out-md "$REPORT_ROOT/core6-comprehensive-table.md"

echo "[freshann] reports:"
ls -1 "$REPORT_ROOT"/*.json
