#!/usr/bin/env python3
import argparse
import json
import re
import urllib.request
from pathlib import Path

DATASETS = [
    ("glove-100-angular", "https://ann-benchmarks.com/glove-100-angular_10_angular.html"),
    ("sift-128-euclidean", "https://ann-benchmarks.com/sift-128-euclidean_10_euclidean.html"),
    ("fashion-mnist-784-euclidean", "https://ann-benchmarks.com/fashion-mnist-784-euclidean_10_euclidean.html"),
    ("nytimes-256-angular", "https://ann-benchmarks.com/nytimes-256-angular_10_angular.html"),
    ("gist-960-euclidean", "https://ann-benchmarks.com/gist-960-euclidean_10_euclidean.html"),
    ("glove-25-angular", "https://ann-benchmarks.com/glove-25-angular_10_angular.html"),
]

ENGINES = [
    "qdrant",
    "pgvector",
    "weaviate",
    "redisearch",
    "hnswlib",
    "hnsw(faiss)",
    "faiss-ivf",
]


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as r:
        return r.read().decode("utf-8", errors="ignore")


def qps90_for_label(html: str, label: str):
    pat = re.compile(r"label:\s*\"" + re.escape(label) + r"\".*?data:\s*\[(.*?)\]\s*\}", re.S)
    m = pat.search(html)
    if not m:
        return None
    data = m.group(1)
    pts = re.findall(r"\{\s*x:\s*([0-9\.eE+-]+)\s*,\s*y:\s*([0-9\.eE+-]+)", data)
    vals = [float(y) for x, y in pts if float(x) >= 0.90]
    if not vals:
        return 0.0
    return max(vals)


def main() -> None:
    p = argparse.ArgumentParser(description="Build internet comparison matrix from ANN-Bench pages + local freshann reports")
    p.add_argument("--report-root", default="/tmp/freshann-bench-data/reports")
    p.add_argument("--out", default="/tmp/freshann-bench-data/reports/internet-comparison-matrix.json")
    args = p.parse_args()

    report_root = Path(args.report_root)
    ours = {}
    for ds, _ in DATASETS:
        path = report_root / f"{ds}.json"
        if path.exists():
            ours[ds] = json.loads(path.read_text())

    rows = []
    for ds, url in DATASETS:
        html = fetch(url)
        row = {
            "dataset": ds,
            "freshann_qps_at_recall_90": ours.get(ds, {}).get("qps_at_recall_90", 0.0),
            "freshann_recall_at_k": ours.get(ds, {}).get("recall_at_k", 0.0),
            "source_url": url,
        }
        for engine in ENGINES:
            row[f"{engine}_qps_at_recall_90"] = qps90_for_label(html, engine)
        rows.append(row)

    out = {
        "source": "ann-benchmarks.com dataset detail pages (first series occurrence per engine)",
        "rows": rows,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
