#!/usr/bin/env python3
"""
Download and convert ANN benchmark datasets for HD-Index benchmarking.

Supports:
  sift-128             SIFT1M (TEXMEX corpus), 128-dim, euclidean
  dbpedia-openai-1536  DBPedia OpenAI embeddings, 1536-dim, cosine

Output layout under --output-dir/{dataset}/:
  train.fvecs        base vectors
  test.fvecs         query vectors
  groundtruth.ivecs  k-NN indices (depth k)
  metadata.json      dataset statistics
"""

import argparse
import json
import os
import shutil
import struct
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Dependency checks — fail fast with clear instructions
# ---------------------------------------------------------------------------

def _require_numpy():
    try:
        import numpy as np
        return np
    except ImportError:
        print("numpy is required. Install with:  pip install numpy", file=sys.stderr)
        sys.exit(1)


def _require_h5py():
    try:
        import h5py
        return h5py
    except ImportError:
        print("h5py is required. Install with:  pip install h5py", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Binary I/O helpers
# ---------------------------------------------------------------------------

def write_fvecs(path: Path, data) -> None:
    """Write float32 vectors in fvecs format: [dim:int32][v0:float32]...[vN:float32] per row."""
    np = _require_numpy()
    data = np.asarray(data, dtype=np.float32)
    n, dim = data.shape
    with open(path, "wb") as f:
        for i in range(n):
            f.write(struct.pack("<i", dim))
            f.write(data[i].tobytes())


def write_ivecs(path: Path, data) -> None:
    """Write int32 vectors in ivecs format: [k:int32][id0:int32]...[idK:int32] per row."""
    np = _require_numpy()
    data = np.asarray(data, dtype=np.int32)
    n, k = data.shape
    with open(path, "wb") as f:
        for i in range(n):
            f.write(struct.pack("<i", k))
            f.write(data[i].tobytes())


def read_fvecs(path: Path):
    """Read fvecs file and return numpy array of shape (n, dim)."""
    np = _require_numpy()
    vectors = []
    with open(path, "rb") as f:
        while True:
            header = f.read(4)
            if not header:
                break
            dim = struct.unpack("<i", header)[0]
            raw = f.read(dim * 4)
            if len(raw) != dim * 4:
                raise ValueError(f"Truncated fvecs data in {path}")
            vectors.append(np.frombuffer(raw, dtype=np.float32).copy())
    return np.stack(vectors) if vectors else np.empty((0, 0), dtype=np.float32)


def read_ivecs(path: Path):
    """Read ivecs file and return numpy array of shape (n, k)."""
    np = _require_numpy()
    vectors = []
    with open(path, "rb") as f:
        while True:
            header = f.read(4)
            if not header:
                break
            k = struct.unpack("<i", header)[0]
            raw = f.read(k * 4)
            if len(raw) != k * 4:
                raise ValueError(f"Truncated ivecs data in {path}")
            vectors.append(np.frombuffer(raw, dtype=np.int32).copy())
    return np.stack(vectors) if vectors else np.empty((0, 0), dtype=np.int32)


def write_metadata(directory: Path, meta: dict) -> None:
    with open(directory / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  metadata written to {directory / 'metadata.json'}")


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def _try_tqdm():
    try:
        from tqdm import tqdm
        return tqdm
    except ImportError:
        return None


def download_file(url: str, dest_path: Path) -> None:
    """Download url to dest_path with progress reporting."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_name(dest_path.name + ".part")
    tmp_path.unlink(missing_ok=True)
    tqdm = _try_tqdm()

    print(f"  Downloading {url}")
    print(f"  -> {dest_path}")

    try:
        if tqdm is not None:
            _download_with_tqdm(url, tmp_path, tqdm)
        else:
            _download_simple(url, tmp_path)
        os.replace(tmp_path, dest_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _download_with_tqdm(url: str, dest_path: Path, tqdm_cls) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "fetch_benchmark_data/1.0"})
    with urllib.request.urlopen(req) as resp:
        total = int(resp.headers.get("Content-Length", 0)) or None
        with tqdm_cls(total=total, unit="B", unit_scale=True, desc=dest_path.name) as bar:
            with open(dest_path, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)  # 1 MB
                    if not chunk:
                        break
                    out.write(chunk)
                    bar.update(len(chunk))


def _download_simple(url: str, dest_path: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "fetch_benchmark_data/1.0"})
    with urllib.request.urlopen(req) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest_path, "wb") as out:
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  {pct:3d}%  {downloaded // (1 << 20)} / {total // (1 << 20)} MB", end="", flush=True)
    print()


# ---------------------------------------------------------------------------
# Disk space guard
# ---------------------------------------------------------------------------

def _check_disk_space(path: Path, required_bytes: int) -> None:
    usage = shutil.disk_usage(path)
    if usage.free < required_bytes:
        free_mb = usage.free // (1 << 20)
        need_mb = required_bytes // (1 << 20)
        raise RuntimeError(
            f"Insufficient disk space at {path}: need ~{need_mb} MB, only {free_mb} MB free"
        )


# ---------------------------------------------------------------------------
# Ground truth computation
# ---------------------------------------------------------------------------

def compute_ground_truth(train, test, metric: str, k: int):
    """
    Brute-force k-NN ground truth.

    metric: "euclidean" — L2 distance
            "angular" or "cosine" — cosine distance (1 - cosine_similarity)
    Returns int32 array of shape (len(test), k) with 0-based train indices.
    """
    np = _require_numpy()
    train = np.asarray(train, dtype=np.float32)
    test = np.asarray(test, dtype=np.float32)

    n_test = len(test)
    results = np.empty((n_test, k), dtype=np.int32)

    if metric in ("angular", "cosine"):
        # Normalise once; then L2 on unit vectors == sqrt(2 - 2*cos) which preserves order.
        train_norms = np.linalg.norm(train, axis=1, keepdims=True)
        train_norms = np.where(train_norms == 0, 1.0, train_norms)
        train_n = train / train_norms

        test_norms = np.linalg.norm(test, axis=1, keepdims=True)
        test_norms = np.where(test_norms == 0, 1.0, test_norms)
        test_n = test / test_norms

    batch_size = 100
    n_batches = (n_test + batch_size - 1) // batch_size

    for b in range(n_batches):
        lo = b * batch_size
        hi = min(lo + batch_size, n_test)
        print(f"\r  Ground truth: batch {b + 1}/{n_batches} (queries {lo}-{hi})", end="", flush=True)

        if metric in ("angular", "cosine"):
            q = test_n[lo:hi]
            # dot product on unit vectors = cosine similarity; higher is closer
            sims = q @ train_n.T  # (batch, n_train)
            # We want largest similarity = smallest cosine distance
            top_k = np.argpartition(-sims, kth=min(k, sims.shape[1] - 1), axis=1)[:, :k]
            # Sort within the top-k by descending similarity
            rows = np.arange(hi - lo)[:, None]
            order = np.argsort(-sims[rows, top_k], axis=1)
            results[lo:hi] = top_k[rows, order]
        else:
            # Euclidean: ||q - p||^2 = ||q||^2 + ||p||^2 - 2*q.p
            q = test[lo:hi]
            q_sq = np.sum(q ** 2, axis=1, keepdims=True)          # (batch, 1)
            t_sq = np.sum(train ** 2, axis=1)[np.newaxis, :]       # (1, n_train)
            dists_sq = q_sq + t_sq - 2.0 * (q @ train.T)          # (batch, n_train)
            top_k = np.argpartition(dists_sq, kth=min(k, dists_sq.shape[1] - 1), axis=1)[:, :k]
            rows = np.arange(hi - lo)[:, None]
            order = np.argsort(dists_sq[rows, top_k], axis=1)
            results[lo:hi] = top_k[rows, order]

    print()  # newline after progress
    return results


# ---------------------------------------------------------------------------
# Dataset: SIFT-128
# ---------------------------------------------------------------------------

_SIFT_URL = "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz"
_SIFT_REQUIRED_BYTES = 700 * (1 << 20)  # ~700 MB after extraction


def random_subset_indices(np, n: int, subset: int, seed: int):
    if subset <= 0 or subset >= n:
        return None
    rng = np.random.default_rng(seed)
    return rng.choice(n, size=subset, replace=False)


def fetch_sift128(output_dir: Path, subset: int, k: int, force: bool, subset_seed: int) -> None:
    dataset_dir = output_dir / "sift-128"
    if _output_exists(dataset_dir) and not force:
        print(f"[sift-128] Output already exists at {dataset_dir}. Use --force to re-generate.")
        return

    cache_dir = output_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    _check_disk_space(output_dir, _SIFT_REQUIRED_BYTES)

    archive = cache_dir / "sift.tar.gz"
    if not archive.exists() or force:
        try:
            download_file(_SIFT_URL, archive)
        except Exception as exc:
            archive.unlink(missing_ok=True)
            raise RuntimeError(f"Failed to download SIFT dataset: {exc}") from exc
    else:
        print(f"[sift-128] Using cached archive at {archive}")

    with tempfile.TemporaryDirectory() as tmpdir:
        print("[sift-128] Extracting archive ...")
        with tarfile.open(archive, "r:gz") as tf:
            tf.extractall(tmpdir)

        tmp_path = Path(tmpdir)
        # TEXMEX archive unpacks into a 'sift/' subdirectory
        sift_dir = tmp_path / "sift"
        if not sift_dir.exists():
            raise RuntimeError(f"Expected sift/ subdirectory inside archive, found: {list(tmp_path.iterdir())}")

        print("[sift-128] Reading native fvecs/ivecs files ...")
        train = read_fvecs(sift_dir / "sift_base.fvecs")
        test = read_fvecs(sift_dir / "sift_query.fvecs")
        gt_native = read_ivecs(sift_dir / "sift_groundtruth.ivecs")
    source_n_train = len(train)

    _validate_dims(train, test, expected_dim=128, name="sift-128")

    np = _require_numpy()
    subset_indices = random_subset_indices(np, len(train), subset, subset_seed)
    if subset_indices is not None:
        print(f"[sift-128] Randomly selecting {subset} train vectors with seed {subset_seed}, recomputing ground truth ...")
        train = train[subset_indices]
        gt = compute_ground_truth(train, test, "euclidean", k)
    else:
        subset = len(train)
        # Native ground truth has 100 neighbours; truncate/pad to k
        gt = _adapt_groundtruth(gt_native, k, len(train))

    dataset_dir.mkdir(parents=True, exist_ok=True)
    print("[sift-128] Writing output files ...")
    write_fvecs(dataset_dir / "train.fvecs", train)
    write_fvecs(dataset_dir / "test.fvecs", test)
    write_ivecs(dataset_dir / "groundtruth.ivecs", gt)
    write_metadata(dataset_dir, {
        "n_train": len(train),
        "n_test": len(test),
        "dim": 128,
        "metric": "euclidean",
        "k": k,
        "subset_method": "random_without_replacement" if subset_indices is not None else "full",
        "subset_seed": subset_seed if subset_indices is not None else None,
        "source_n_train": source_n_train,
    })
    print(f"[sift-128] Done. Output at {dataset_dir}")


# ---------------------------------------------------------------------------
# Dataset: DBPedia OpenAI-1536
# ---------------------------------------------------------------------------

_DBPEDIA_URL = (
    "https://storage.googleapis.com/ann-datasets/ann-benchmarks/"
    "dbpedia-openai-1000k-angular.hdf5"
)
_DBPEDIA_REQUIRED_BYTES = 7 * (1 << 30)  # ~7 GB


def fetch_dbpedia1536(output_dir: Path, subset: int, k: int, force: bool, subset_seed: int) -> None:
    h5py = _require_h5py()
    dataset_dir = output_dir / "dbpedia-openai-1536"
    if _output_exists(dataset_dir) and not force:
        print(f"[dbpedia-openai-1536] Output already exists at {dataset_dir}. Use --force to re-generate.")
        return

    cache_dir = output_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    _check_disk_space(output_dir, _DBPEDIA_REQUIRED_BYTES)

    hdf5_file = cache_dir / "dbpedia-openai-1536.hdf5"
    if not hdf5_file.exists() or force:
        try:
            download_file(_DBPEDIA_URL, hdf5_file)
        except Exception as exc:
            hdf5_file.unlink(missing_ok=True)
            raise RuntimeError(f"Failed to download DBPedia dataset: {exc}") from exc
    else:
        print(f"[dbpedia-openai-1536] Using cached file at {hdf5_file}")

    print("[dbpedia-openai-1536] Reading HDF5 file ...")
    with h5py.File(hdf5_file, "r") as hf:
        _validate_hdf5_keys(hf, required=["train", "test", "neighbors", "distances"])
        np = _require_numpy()
        n_train = hf["train"].shape[0]
        subset_indices = random_subset_indices(np, n_train, subset, subset_seed)
        if subset_indices is not None:
            order = np.argsort(subset_indices)
            read_indices = subset_indices[order]
            train_sorted = np.asarray(hf["train"][read_indices], dtype=np.float32)
            inverse = np.empty_like(order)
            inverse[order] = np.arange(len(order))
            train = train_sorted[inverse]
        else:
            train = hf["train"][:]
        test = np.asarray(hf["test"][:], dtype=np.float32)
        gt_native = None if subset_indices is not None else hf["neighbors"][:]

    train = np.asarray(train, dtype=np.float32)

    _validate_dims(train, test, expected_dim=1536, name="dbpedia-openai-1536")

    if subset_indices is not None:
        print(f"[dbpedia-openai-1536] Randomly selected {subset} train vectors with seed {subset_seed}, recomputing ground truth ...")
        gt = compute_ground_truth(train, test, "angular", k)
    else:
        subset = len(train)
        gt = _adapt_groundtruth(np.asarray(gt_native, dtype=np.int32), k, len(train))

    dataset_dir.mkdir(parents=True, exist_ok=True)
    print("[dbpedia-openai-1536] Writing output files ...")
    write_fvecs(dataset_dir / "train.fvecs", train)
    write_fvecs(dataset_dir / "test.fvecs", test)
    write_ivecs(dataset_dir / "groundtruth.ivecs", gt)
    write_metadata(dataset_dir, {
        "n_train": len(train),
        "n_test": len(test),
        "dim": 1536,
        "metric": "angular",
        "k": k,
        "subset_method": "random_without_replacement" if subset_indices is not None else "full",
        "subset_seed": subset_seed if subset_indices is not None else None,
        "source_n_train": int(n_train),
    })
    print(f"[dbpedia-openai-1536] Done. Output at {dataset_dir}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _output_exists(dataset_dir: Path) -> bool:
    required = ["train.fvecs", "test.fvecs", "groundtruth.ivecs", "metadata.json"]
    return all((dataset_dir / f).exists() for f in required)


def _validate_dims(train, test, expected_dim: int, name: str) -> None:
    if train.ndim != 2 or train.shape[1] != expected_dim:
        raise ValueError(f"[{name}] Expected train shape (n, {expected_dim}), got {train.shape}")
    if test.ndim != 2 or test.shape[1] != expected_dim:
        raise ValueError(f"[{name}] Expected test shape (n, {expected_dim}), got {test.shape}")


def _validate_hdf5_keys(hf, required: list) -> None:
    missing = [k for k in required if k not in hf]
    if missing:
        raise ValueError(f"HDF5 file missing expected keys: {missing}. Found: {list(hf.keys())}")


def _adapt_groundtruth(gt_native, k: int, n_train: int):
    """Truncate or raise if native ground truth doesn't cover k neighbours."""
    np = _require_numpy()
    gt_native = np.asarray(gt_native, dtype=np.int32)
    native_k = gt_native.shape[1]
    if k <= native_k:
        return gt_native[:, :k]
    raise ValueError(
        f"Requested k={k} exceeds native ground truth depth ({native_k}). "
        f"Use --subset to trigger recomputation, or choose k <= {native_k}."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_DATASETS = {
    "sift-128": fetch_sift128,
    "dbpedia-openai-1536": fetch_dbpedia1536,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and convert ANN benchmark datasets for HD-Index.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dataset",
        required=True,
        choices=list(_DATASETS.keys()),
        help="Dataset to fetch.",
    )
    parser.add_argument(
        "--subset",
        type=int,
        default=0,
        metavar="N",
        help="Randomly select N training vectors without replacement (0 = all). Recomputes ground truth.",
    )
    parser.add_argument(
        "--subset-seed",
        type=int,
        default=42,
        help="Seed for deterministic random subset selection.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/marmot/benchdata"),
        help="Root directory for output and cache.",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=100,
        help="Ground truth depth (number of nearest neighbours).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and reprocess even if output already exists.",
    )
    args = parser.parse_args()

    if args.k < 1:
        parser.error("--k must be >= 1")
    if args.subset < 0:
        parser.error("--subset must be >= 0")

    # Ensure numpy is available before any heavy work
    _require_numpy()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    fetch_fn = _DATASETS[args.dataset]
    fetch_fn(
        output_dir=args.output_dir,
        subset=args.subset,
        k=args.k,
        force=args.force,
        subset_seed=args.subset_seed,
    )


if __name__ == "__main__":
    main()
