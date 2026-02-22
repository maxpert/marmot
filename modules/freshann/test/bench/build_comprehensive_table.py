#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


CORE6 = [
    "glove-100-angular",
    "sift-128-euclidean",
    "fashion-mnist-784-euclidean",
    "nytimes-256-angular",
    "gist-960-euclidean",
    "glove-25-angular",
]


def load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text())


def fmt(v, nd=2):
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, int):
        return str(v)
    return f"{float(v):.{nd}f}"


def build_rows(report_root: Path, internet_path: Path):
    internet = load_json(internet_path) or {"rows": []}
    internet_map = {r["dataset"]: r for r in internet.get("rows", [])}
    rows = []
    for ds in CORE6:
        r = load_json(report_root / f"{ds}.json")
        if not r:
            continue
        ir = internet_map.get(ds, {})
        pg = ir.get("pgvector_qps_at_recall_90")
        qdrant = ir.get("qdrant_qps_at_recall_90")
        fresh_qps90 = r.get("qps_at_recall_90", 0.0)
        ratio_pg = None
        if pg and pg > 0:
            ratio_pg = fresh_qps90 / pg
        rows.append(
            {
                "dataset": ds,
                "metric": r.get("metric"),
                "dim": r.get("dim"),
                "vectors": r.get("base_count"),
                "queries": r.get("query_count"),
                "build_seconds": r.get("build_seconds"),
                "query_seconds": r.get("query_seconds"),
                "recall_at_10": r.get("recall_at_k"),
                "qps": r.get("qps"),
                "qps_at_recall_90": fresh_qps90,
                "p95_ms": r.get("p95_ms"),
                "peak_heap_alloc_mb": (r.get("memory") or {}).get("peak_heap_alloc_mb"),
                "peak_heap_inuse_mb": (r.get("memory") or {}).get("peak_heap_inuse_mb"),
                "peak_sys_mb": (r.get("memory") or {}).get("peak_sys_mb"),
                "total_alloc_mb": (r.get("memory") or {}).get("total_alloc_mb"),
                "num_gc": (r.get("memory") or {}).get("num_gc"),
                "gc_pause_total_ms": (r.get("memory") or {}).get("gc_pause_total_ms"),
                "best_tuning": r.get("best_tuning"),
                "pgvector_qps_at_recall_90": pg,
                "qdrant_qps_at_recall_90": qdrant,
                "ratio_vs_pgvector_qps90": ratio_pg,
                "passes_recall_090": (r.get("recall_at_k", 0.0) or 0.0) >= 0.90,
            }
        )
    return rows


def to_markdown(rows):
    header = [
        "dataset",
        "metric",
        "dim",
        "vectors",
        "queries",
        "build_s",
        "query_s",
        "recall@10",
        "qps",
        "qps@>=0.90",
        "p95_ms",
        "peak_heap_mb",
        "peak_sys_mb",
        "total_alloc_mb",
        "num_gc",
        "gc_pause_ms",
        "pg_qps90",
        "qdrant_qps90",
        "fresh/pg",
        "pass_0.90",
    ]
    lines = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join(["---"] * len(header)) + "|")
    for r in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(r["dataset"]),
                    str(r.get("metric", "-")),
                    fmt(r.get("dim"), 0),
                    fmt(r.get("vectors"), 0),
                    fmt(r.get("queries"), 0),
                    fmt(r.get("build_seconds")),
                    fmt(r.get("query_seconds")),
                    fmt(r.get("recall_at_10"), 4),
                    fmt(r.get("qps")),
                    fmt(r.get("qps_at_recall_90")),
                    fmt(r.get("p95_ms")),
                    fmt(r.get("peak_heap_alloc_mb")),
                    fmt(r.get("peak_sys_mb")),
                    fmt(r.get("total_alloc_mb")),
                    fmt(r.get("num_gc"), 0),
                    fmt(r.get("gc_pause_total_ms")),
                    fmt(r.get("pgvector_qps_at_recall_90")),
                    fmt(r.get("qdrant_qps_at_recall_90")),
                    fmt(r.get("ratio_vs_pgvector_qps90")),
                    fmt(r.get("passes_recall_090")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report-root", default="/tmp/freshann-bench-data/reports")
    p.add_argument("--internet", default="/tmp/freshann-bench-data/reports/internet-comparison-matrix.json")
    p.add_argument("--out-json", default="/tmp/freshann-bench-data/reports/core6-comprehensive-table.json")
    p.add_argument("--out-md", default="/tmp/freshann-bench-data/reports/core6-comprehensive-table.md")
    args = p.parse_args()

    report_root = Path(args.report_root)
    rows = build_rows(report_root, Path(args.internet))
    payload = {"rows": rows, "all_pass_recall_090": all(r["passes_recall_090"] for r in rows) if rows else False}
    md = to_markdown(rows)

    Path(args.out_json).write_text(json.dumps(payload, indent=2))
    Path(args.out_md).write_text(md)
    print(md)


if __name__ == "__main__":
    main()
