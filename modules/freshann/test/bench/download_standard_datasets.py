#!/usr/bin/env python3
import argparse
import json
import os
import struct
import urllib.request
from pathlib import Path
from typing import Optional


def download(url: str, dst: Path, max_size: Optional[int] = None) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and dst.stat().st_size > 0:
        print(f"[download] exists: {dst}")
        return
    print(f"[download] {url} -> {dst}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; freshann-bench/1.0)"})
    with urllib.request.urlopen(req) as resp, open(dst, "wb") as out:
        chunk = 1024 * 1024
        total = 0
        while True:
            b = resp.read(chunk)
            if not b:
                break
            if max_size is not None and total + len(b) > max_size:
                keep = max_size - total
                if keep > 0:
                    out.write(b[:keep])
                    total += keep
                break
            out.write(b)
            total += len(b)
    print(f"[download] done bytes={dst.stat().st_size}")


def patch_u8bin_header_count(path: Path, count: int) -> None:
    with open(path, "r+b") as f:
        f.seek(0)
        f.write(struct.pack("<I", count))


def write_f32bin(path: Path, arr) -> None:
    n, d = arr.shape
    with open(path, "wb") as f:
        f.write(struct.pack("<II", n, d))
        arr.astype("float32").tofile(f)


def write_i32bin(path: Path, arr) -> None:
    n, d = arr.shape
    with open(path, "wb") as f:
        f.write(struct.pack("<II", n, d))
        arr.astype("int32").tofile(f)


def prepare_annbench(root: Path, ann_base_count: int, ann_query_count: int) -> None:
    import h5py
    import numpy as np

    ds_dir = root / "annbench-glove-100-angular"
    ds_dir.mkdir(parents=True, exist_ok=True)
    h5_path = ds_dir / "glove-100-angular.hdf5"
    download("https://ann-benchmarks.com/glove-100-angular.hdf5", h5_path)

    with h5py.File(h5_path, "r") as h:
        train = h["train"]
        test = h["test"]
        neighbors = h["neighbors"]

        base_n = min(train.shape[0], ann_base_count)
        query_n = min(test.shape[0], ann_query_count)
        base = np.asarray(train[:base_n], dtype=np.float32)
        queries = np.asarray(test[:query_n], dtype=np.float32)

        write_f32bin(ds_dir / "base.f32bin", base)
        write_f32bin(ds_dir / "queries.f32bin", queries)

        has_official_gt = False
        if base_n == train.shape[0]:
            gt = np.asarray(neighbors[:query_n, :100], dtype=np.int32)
            write_i32bin(ds_dir / "gt.i32bin", gt)
            has_official_gt = True

        manifest = {
            "dataset_name": "annbench-glove-100-angular",
            "metric": "cosine",
            "base_count": int(base_n),
            "query_count": int(query_n),
            "has_official_gt": has_official_gt,
            "original_base_all": int(train.shape[0]),
        }
        (ds_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[annbench] prepared at {ds_dir}")


def read_u8bin(path: Path, n_limit: Optional[int] = None):
    import numpy as np

    with open(path, "rb") as f:
        n, d = struct.unpack("<II", f.read(8))
    if n_limit is not None:
        n = min(n, n_limit)
    data = np.memmap(path, dtype=np.uint8, mode="r", offset=8, shape=(n, d))
    return np.asarray(data, dtype=np.float32)


def read_knn_ids(path: Path, q_limit: Optional[int] = None, k_limit: Optional[int] = None):
    import numpy as np

    with open(path, "rb") as f:
        nq, k = struct.unpack("<II", f.read(8))
        if q_limit is not None:
            nq = min(nq, q_limit)
        ids = np.fromfile(f, dtype=np.int32, count=nq * k).reshape(nq, k)
        # skip distances in file; not needed
    if k_limit is not None and k_limit < ids.shape[1]:
        ids = ids[:, :k_limit]
    return ids


def prepare_bigann(root: Path, bigann_base_count: int, bigann_query_count: int) -> None:
    ds_dir = root / "bigann-10m"
    ds_dir.mkdir(parents=True, exist_ok=True)

    base_raw = ds_dir / f"base.crop_{bigann_base_count}.u8bin"
    query_raw = ds_dir / "query.public.10K.u8bin"
    gt_raw = ds_dir / "GT_10M.bigann-10M"

    d = 128
    max_size = 8 + bigann_base_count * d
    download("https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/base.1B.u8bin", base_raw, max_size=max_size)
    patch_u8bin_header_count(base_raw, bigann_base_count)
    download("https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.public.10K.u8bin", query_raw)
    download("https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/GT_10M/bigann-10M", gt_raw)

    base = read_u8bin(base_raw, bigann_base_count)
    queries = read_u8bin(query_raw, bigann_query_count)
    write_f32bin(ds_dir / "base.f32bin", base)
    write_f32bin(ds_dir / "queries.f32bin", queries)

    # BigANN official GT is Euclidean; freshann harness currently benchmarks cosine/dot.
    # Keep GT downloaded for future use, but mark as not directly comparable in this harness.
    has_official_gt = False

    manifest = {
        "dataset_name": "bigann-10m",
        "metric": "cosine",
        "base_count": int(bigann_base_count),
        "query_count": int(bigann_query_count),
        "has_official_gt": has_official_gt,
        "original_base_all": 10_000_000,
    }
    (ds_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[bigann] prepared at {ds_dir}")


def main() -> None:
    p = argparse.ArgumentParser(description="Download and prepare ANN-Benchmarks + Big-ANN datasets for freshann benchmark harness")
    p.add_argument("--root", default="/tmp/freshann-bench-data/datasets")
    p.add_argument("--ann-base-count", type=int, default=50_000)
    p.add_argument("--ann-query-count", type=int, default=1_000)
    p.add_argument("--bigann-base-count", type=int, default=100_000)
    p.add_argument("--bigann-query-count", type=int, default=1_000)
    args = p.parse_args()

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)

    try:
        prepare_annbench(root, args.ann_base_count, args.ann_query_count)
        prepare_bigann(root, args.bigann_base_count, args.bigann_query_count)
    except ModuleNotFoundError as e:
        print(
            "missing python dependency: "
            f"{e}. Install with: python3 -m pip install --user numpy h5py"
        )
        raise


if __name__ == "__main__":
    main()
