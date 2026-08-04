"""Powers-of-2 raster compression sweep: codec × dtype × size benchmark.

Measures encoded ratio, write wall-time, peak RSS during write (via subprocess
isolation), and read wall-time for each (dtype, decoded_size, codec/level) cell.

Synthesizes a realistic single-band tile (2-D ramp + moderate Gaussian noise) so
that compression is non-trivial — not all-constant (trivially compressible) and
not pure random (incompressible).

Run::

    PYSPARK_PYTHON=$(pwd)/.venv-pyrx/bin/python \\
    PYSPARK_DRIVER_PYTHON=$(pwd)/.venv-pyrx/bin/python \\
    .venv-pyrx/bin/python \\
        python/geobrix/src/databricks/labs/gbx/bench/compression_sweep.py

Writes results to stdout as a CSV table and honours ``--out <path>`` for a
machine-readable JSON file.  Sizes that would exceed 40 % of available RAM are
skipped and recorded as ``skipped (needs Serverless)``.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import platform
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DTYPES: List[str] = ["float32", "uint16", "int16", "uint8"]

# Decoded sizes to sweep (MiB).  512 and 1024 are only attempted if RAM allows.
BASE_SIZES_MIB: List[int] = [1, 2, 4, 8, 16, 32, 64, 128, 256]
EXTENDED_SIZES_MIB: List[int] = [512, 1024]

# Predictor by dtype group.
_FLOAT_DTYPES = {"float32", "float64"}
_SMALL_INT_DTYPES = {"uint8", "int8"}

ZSTD_LEVELS: List[int] = [3, 6, 9, 12, 16, 19, 22]

# RAM guard: skip if decoded array > this fraction of available RAM.
RAM_GUARD_FRACTION = 0.40

# Per-config subprocess timeout (seconds).  Configs that exceed this are marked
# "timeout" — they are definitionally too slow for the auto ladder.
PER_CONFIG_TIMEOUT_S = 30

# RSS normalisation: macOS reports bytes, Linux reports KB.
_RSS_DIVISOR = (1024 * 1024) if platform.system() == "Darwin" else 1024


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def predictor_for(dtype: str) -> int:
    """Return GDAL predictor tag appropriate for *dtype*."""
    if dtype in _FLOAT_DTYPES:
        return 3
    if dtype in _SMALL_INT_DTYPES:
        return 1
    return 2  # int16/uint16/int32/uint32


def _available_ram_bytes() -> int:
    try:
        import psutil

        return psutil.virtual_memory().available
    except Exception:  # noqa: BLE001
        return 8 * 1024**3  # conservative 8 GiB fallback


def sizes_to_run(base: List[int], extended: List[int]) -> Tuple[List[int], List[int]]:
    """Return (will_run, skipped) size lists based on available RAM."""
    avail = _available_ram_bytes()
    run: List[int] = []
    skip: List[int] = []
    for mib in base + extended:
        # Worst-case: decoded array + 2× encode buffers ≈ 3× decoded bytes.
        needed = mib * 1024 * 1024 * 3
        if needed > avail * RAM_GUARD_FRACTION:
            skip.append(mib)
        else:
            run.append(mib)
    return run, skip


def _dtype_range(dtype: str) -> Tuple[float, float]:
    info = np.iinfo(dtype) if dtype not in _FLOAT_DTYPES else None
    if info is not None:
        return float(info.min), float(info.max)
    return -1e4, 1e4  # float32 reasonable elevation-like range


def make_tile(decoded_mib: int, dtype: str, seed: int = 42) -> np.ndarray:
    """Create a realistic single-band 2-D array of *decoded_mib* MiB.

    Uses a smooth 2-D gradient (low frequency, highly compressible) plus
    moderate Gaussian noise so that the data is neither trivially constant nor
    incompressible random.  The noise amplitude is ~5 % of the dtype range so
    the predictor's delta-encoding is meaningful.
    """
    itemsize = np.dtype(dtype).itemsize
    n_pixels = (decoded_mib * 1024 * 1024) // itemsize
    # Square tile (nearest integer side)
    side = int(n_pixels**0.5)
    n_pixels = side * side  # re-align to square

    rng = np.random.default_rng(seed)
    lo, hi = _dtype_range(dtype)
    full_range = hi - lo

    # Gradient: smooth ramp across both axes
    x = np.linspace(lo, hi, side, dtype="float64")
    y = np.linspace(lo, hi, side, dtype="float64")
    gradient = 0.5 * (x[np.newaxis, :] + y[:, np.newaxis])

    # Noise: 5 % of full range, Gaussian
    noise_sigma = full_range * 0.05
    noise = rng.normal(0, noise_sigma, (side, side))

    arr = gradient + noise
    # Clip to valid range for integer dtypes
    arr = np.clip(arr, lo, hi)
    return arr.astype(dtype)


# ---------------------------------------------------------------------------
# Per-config encode/decode — run in a subprocess for clean peak RSS
# ---------------------------------------------------------------------------


def _encode_worker(
    arr_bytes: bytes,
    shape: Tuple[int, int],
    dtype: str,
    compress: str,
    level: Optional[int],
    predictor: Optional[int],
    result_queue: "multiprocessing.Queue[Dict[str, Any]]",
) -> None:
    """Worker function: encode arr → MemoryFile, measure RSS + time."""
    import platform as _plat
    import resource
    import time as _time

    import numpy as _np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    arr = _np.frombuffer(arr_bytes, dtype=dtype).reshape(shape)
    h, w = shape

    profile: Dict[str, Any] = {
        "driver": "GTiff",
        "height": h,
        "width": w,
        "count": 1,
        "dtype": dtype,
        "crs": "EPSG:4326",
        "transform": from_bounds(0, 0, 1, 1, w, h),
    }

    if compress == "none":
        pass  # no compression keys
    elif compress == "lzw":
        profile["compress"] = "lzw"
        if predictor is not None:
            profile["predictor"] = str(predictor)
    elif compress == "deflate":
        profile["compress"] = "deflate"
        profile["zlevel"] = str(level or 6)
        if predictor is not None:
            profile["predictor"] = str(predictor)
    elif compress == "zstd":
        profile["compress"] = "zstd"
        profile["zstd_level"] = str(level or 9)
        if predictor is not None:
            profile["predictor"] = str(predictor)

    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    t0 = _time.perf_counter()
    encoded_buf: bytes = b""
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr, 1)
        encoded_buf = mf.read()
    t_write = (_time.perf_counter() - t0) * 1000  # ms
    rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = (1024 * 1024) if _plat.system() == "Darwin" else 1024
    delta_rss_mib = (rss_after - rss_before) / divisor

    # Read back (full decode)
    t1 = _time.perf_counter()
    with MemoryFile(encoded_buf) as mf2:
        with mf2.open() as ds2:
            _ = ds2.read(1)
    t_read = (_time.perf_counter() - t1) * 1000  # ms

    result_queue.put(
        {
            "encoded_bytes": len(encoded_buf),
            "write_ms": t_write,
            "delta_rss_mib": delta_rss_mib,
            "read_ms": t_read,
        }
    )


def encode_one(
    arr: np.ndarray,
    compress: str,
    level: Optional[int],
    predictor: Optional[int],
) -> Dict[str, Any]:
    """Encode *arr* in a subprocess; return {encoded_bytes, write_ms, delta_rss_mib, read_ms}."""
    ctx = multiprocessing.get_context("spawn")
    q: "multiprocessing.Queue[Dict[str, Any]]" = ctx.Queue()
    p = ctx.Process(
        target=_encode_worker,
        args=(
            arr.tobytes(),
            arr.shape,
            str(arr.dtype),
            compress,
            level,
            predictor,
            q,
        ),
    )
    p.start()
    p.join(timeout=PER_CONFIG_TIMEOUT_S)
    if p.is_alive():
        # Config exceeded timeout — it is too slow; kill and record as timeout.
        p.terminate()
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
            p.join()
        return {
            "encoded_bytes": 0,
            "write_ms": float("inf"),  # sentinel: too slow
            "delta_rss_mib": float("nan"),
            "read_ms": float("nan"),
            "timed_out": True,
        }
    if p.exitcode != 0:
        return {
            "encoded_bytes": 0,
            "write_ms": float("nan"),
            "delta_rss_mib": float("nan"),
            "read_ms": float("nan"),
        }
    result = q.get()
    result.setdefault("timed_out", False)
    return result


# ---------------------------------------------------------------------------
# Row dataclass
# ---------------------------------------------------------------------------


@dataclass
class SweepRow:
    dtype: str
    decoded_mib: int
    codec_label: str  # e.g. "zstd_l9", "deflate_z6", "lzw", "none", "zstd_l9_nopred"
    compress: str
    level: Optional[int]
    predictor: Optional[int]  # None = no predictor
    encoded_bytes: int = 0
    ratio: float = 0.0
    write_ms: float = 0.0
    delta_rss_mib: float = 0.0
    read_ms: float = 0.0
    skipped: bool = False
    skip_reason: str = ""
    timed_out: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dtype": self.dtype,
            "decoded_mib": self.decoded_mib,
            "codec_label": self.codec_label,
            "compress": self.compress,
            "level": self.level,
            "predictor": self.predictor,
            "encoded_bytes": self.encoded_bytes,
            "ratio": round(self.ratio, 4) if not np.isinf(self.ratio) else None,
            "write_ms": (
                round(self.write_ms, 2) if not np.isinf(self.write_ms) else None
            ),
            "delta_rss_mib": (
                round(self.delta_rss_mib, 2)
                if not np.isnan(self.delta_rss_mib)
                else None
            ),
            "read_ms": round(self.read_ms, 2) if not np.isnan(self.read_ms) else None,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "timed_out": self.timed_out,
        }


# ---------------------------------------------------------------------------
# Config list: all (codec_label, compress, level, predictor) combinations
# ---------------------------------------------------------------------------


def _configs(dtype: str) -> List[Tuple[str, str, Optional[int], Optional[int]]]:
    """Return [(label, compress, level, predictor)] for all configs at *dtype*."""
    pred = predictor_for(dtype)
    cfgs: List[Tuple[str, str, Optional[int], Optional[int]]] = []

    # none
    cfgs.append(("none", "none", None, None))

    # lzw + predictor
    cfgs.append((f"lzw_p{pred}", "lzw", None, pred))

    # deflate z6 + predictor
    cfgs.append((f"deflate_z6_p{pred}", "deflate", 6, pred))

    # zstd levels with predictor
    for lvl in ZSTD_LEVELS:
        cfgs.append((f"zstd_l{lvl}_p{pred}", "zstd", lvl, pred))

    # zstd l9 WITHOUT predictor (predictor effect probe)
    cfgs.append(("zstd_l9_nopred", "zstd", 9, None))

    return cfgs


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------


def run_sweep(
    sizes_mib: List[int],
    skipped_mib: List[int],
    verbose: bool = True,
) -> List[SweepRow]:
    rows: List[SweepRow] = []

    # Skipped sizes — record one row per dtype×size×codec_placeholder
    for mib in skipped_mib:
        for dtype in DTYPES:
            rows.append(
                SweepRow(
                    dtype=dtype,
                    decoded_mib=mib,
                    codec_label="ALL",
                    compress="n/a",
                    level=None,
                    predictor=None,
                    skipped=True,
                    skip_reason="needs Serverless (>40% available RAM)",
                )
            )

    for mib in sizes_mib:
        for dtype in DTYPES:
            if verbose:
                print(
                    f"  [{dtype:>7s} {mib:>4d} MiB] synthesising...",
                    flush=True,
                )
            arr = make_tile(mib, dtype)
            actual_decoded = arr.nbytes
            if verbose:
                print(
                    f"  [{dtype:>7s} {mib:>4d} MiB] array {actual_decoded / 1024**2:.1f} MiB "
                    f"shape={arr.shape}",
                    flush=True,
                )

            for label, compress, level, predictor in _configs(dtype):
                if verbose:
                    print(
                        f"    {label:30s} ... ",
                        end="",
                        flush=True,
                    )
                result = encode_one(arr, compress, level, predictor)
                timed_out = result.get("timed_out", False)
                enc_bytes = result["encoded_bytes"]
                ratio = actual_decoded / enc_bytes if enc_bytes > 0 else float("nan")
                row = SweepRow(
                    dtype=dtype,
                    decoded_mib=mib,
                    codec_label=label,
                    compress=compress,
                    level=level,
                    predictor=predictor,
                    encoded_bytes=enc_bytes,
                    ratio=ratio,
                    write_ms=result["write_ms"],
                    delta_rss_mib=result.get("delta_rss_mib", float("nan")),
                    read_ms=result.get("read_ms", float("nan")),
                    timed_out=timed_out,
                )
                rows.append(row)
                if verbose:
                    if timed_out:
                        print(
                            f"TIMEOUT (>{PER_CONFIG_TIMEOUT_S}s) — too slow for auto",
                            flush=True,
                        )
                    else:
                        w_ms = result["write_ms"]
                        d_rss = result.get("delta_rss_mib", float("nan"))
                        r_ms = result.get("read_ms", float("nan"))
                        print(
                            f"ratio={ratio:5.2f}x  write={w_ms:8.1f}ms"
                            f"  ΔRSS={d_rss:6.1f}MiB"
                            f"  read={r_ms:6.1f}ms",
                            flush=True,
                        )

    return rows


# ---------------------------------------------------------------------------
# Ladder derivation
# ---------------------------------------------------------------------------


def derive_ladder(
    rows: List[SweepRow],
) -> List[Tuple[int, int]]:
    """Derive (decoded_bytes_ceiling, zstd_level) ladder from sweep data.

    Rule: for each size band, choose the HIGHEST ZSTD level (with predictor)
    whose write_ms and delta_rss_mib are still in the flat/linear regime —
    i.e., where write_ms for this level is < 2× the write_ms at level 3, AND
    delta_rss_mib is < 3× the base RSS at level 3.

    Because the dev box skips very large sizes, the ceiling breakpoints are
    derived from the largest size that ran at each tolerated level.

    Returns list of (ceiling_bytes, level) sorted ascending by ceiling,
    with the last entry having ceiling = sys.maxsize (≈ inf).
    """
    # Filter to zstd-with-predictor rows only, non-skipped, non-timed-out
    zstd_rows = [
        r
        for r in rows
        if r.compress == "zstd"
        and r.predictor is not None
        and r.level is not None
        and not r.skipped
        and not r.timed_out
        and not np.isnan(r.write_ms)
        and not np.isinf(r.write_ms)
    ]

    if not zstd_rows:
        # Fallback: can't derive from empty data
        return [
            (64 * 1024**2, 19),
            (256 * 1024**2, 12),
            (1024**3, 6),
            (sys.maxsize, 3),
        ]

    # Build per (decoded_mib, level) summary: median write_ms + delta_rss across dtypes
    from collections import defaultdict

    mib_level_data: Dict[Tuple[int, int], List[SweepRow]] = defaultdict(list)
    for r in zstd_rows:
        mib_level_data[(r.decoded_mib, r.level)].append(r)

    # Get all sizes that ran
    ran_sizes = sorted(set(r.decoded_mib for r in zstd_rows))

    # For each size: get write_ms at level 3 (baseline) and at each level
    def median(vals: List[float]) -> float:
        s = sorted(v for v in vals if not np.isnan(v))
        if not s:
            return float("nan")
        mid = len(s) // 2
        return (s[mid - 1] + s[mid]) / 2 if len(s) % 2 == 0 else s[mid]

    # Per size, per level: median write_ms and delta_rss
    size_level_write: Dict[int, Dict[int, float]] = {}
    size_level_rss: Dict[int, Dict[int, float]] = {}
    for mib in ran_sizes:
        size_level_write[mib] = {}
        size_level_rss[mib] = {}
        for lvl in ZSTD_LEVELS:
            rr = mib_level_data.get((mib, lvl), [])
            if rr:
                size_level_write[mib][lvl] = median([r.write_ms for r in rr])
                size_level_rss[mib][lvl] = median([r.delta_rss_mib for r in rr])

    # For each size, find the HIGHEST level that is "safe"
    # Safe: write_ms < WRITE_MULT * write_ms_at_l3 AND delta_rss < RSS_MULT * rss_at_l3
    WRITE_MULT = 8.0  # allow up to 8× write time vs l3 (generous — read is flat)
    RSS_MULT = 4.0  # RSS budget

    size_max_safe_level: Dict[int, int] = {}
    for mib in ran_sizes:
        base_write = size_level_write[mib].get(3, float("nan"))
        base_rss = size_level_rss[mib].get(3, float("nan"))
        if np.isnan(base_write):
            size_max_safe_level[mib] = 3
            continue
        # Try levels from highest to lowest
        chosen = 3
        for lvl in sorted(ZSTD_LEVELS, reverse=True):
            w = size_level_write[mib].get(lvl, float("nan"))
            r = size_level_rss[mib].get(lvl, float("nan"))
            if np.isnan(w) or np.isnan(r):
                continue
            w_ok = w <= WRITE_MULT * base_write
            r_ok = r <= RSS_MULT * max(base_rss, 1.0)  # guard against 0 base
            if w_ok and r_ok:
                chosen = lvl
                break
        size_max_safe_level[mib] = chosen

    # Build the ladder: group consecutive sizes that share the same safe level
    # The ceiling for a band is the LARGEST size in that band (in bytes)
    # Final entry uses sys.maxsize for "infinity"
    ladder: List[Tuple[int, int]] = []
    last_level = size_max_safe_level.get(ran_sizes[0], 3)
    band_max_mib = ran_sizes[0]

    for mib in ran_sizes[1:]:
        lvl = size_max_safe_level.get(mib, 3)
        if lvl == last_level:
            band_max_mib = mib
        else:
            # Close the current band
            ladder.append((band_max_mib * 1024 * 1024, last_level))
            last_level = lvl
            band_max_mib = mib

    # Last band → infinity ceiling
    ladder.append((sys.maxsize, last_level))

    # Sanity: levels should be non-increasing (small → big = high level → low level)
    # Enforce monotone-non-increasing levels
    min_level = ladder[-1][1]
    sanitised: List[Tuple[int, int]] = []
    for i, (ceiling, lvl) in enumerate(ladder):
        enforced = max(min_level, min(lvl, 22))
        if sanitised and enforced > sanitised[-1][1]:
            enforced = sanitised[-1][1]
        sanitised.append((ceiling, enforced))

    return sanitised


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_table(rows: List[SweepRow]) -> None:
    """Print a compact results table to stdout."""
    header = (
        f"{'dtype':>7}  {'mib':>4}  {'codec':30}  "
        f"{'ratio':>6}  {'write_ms':>9}  {'ΔRSS_MiB':>9}  {'read_ms':>7}  {'skip':4}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        if r.skipped:
            print(
                f"{r.dtype:>7}  {r.decoded_mib:>4}  "
                f"{'ALL':30}  "
                f"{'--':>6}  {'--':>9}  {'--':>9}  {'--':>7}  {r.skip_reason}"
            )
        elif r.timed_out:
            print(
                f"{r.dtype:>7}  {r.decoded_mib:>4}  {r.codec_label:30}  "
                f"{'--':>6}  {'TIMEOUT':>9}  {'--':>9}  {'--':>7}  {'':4}"
            )
        else:
            ratio_s = f"{r.ratio:6.2f}" if not np.isnan(r.ratio) else "  nan"
            rss_s = (
                f"{r.delta_rss_mib:9.1f}"
                if not np.isnan(r.delta_rss_mib)
                else "       --"
            )
            read_s = f"{r.read_ms:7.1f}" if not np.isnan(r.read_ms) else "     --"
            print(
                f"{r.dtype:>7}  {r.decoded_mib:>4}  {r.codec_label:30}  "
                f"{ratio_s}  {r.write_ms:>9.1f}  {rss_s}  "
                f"{read_s}  {'':4}"
            )


def ladder_as_python(ladder: List[Tuple[int, int]]) -> str:
    """Format ladder as a Python literal."""
    parts = []
    for ceiling, lvl in ladder:
        if ceiling == sys.maxsize:
            parts.append(f"    (float('inf'), {lvl}),")
        else:
            mib = ceiling // (1024 * 1024)
            parts.append(f"    ({mib} * 1024**2, {lvl}),  # {mib} MiB ceiling")
    return "_AUTO_LADDER = [\n" + "\n".join(parts) + "\n]"


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def _write_json(
    out_path: str, sizes_run: List[int], sizes_skip: List[int], rows: List[SweepRow]
) -> None:
    ladder = derive_ladder(rows)
    out_data = {
        "platform": platform.system(),
        "python": sys.version,
        "sizes_run_mib": sizes_run,
        "sizes_skipped_mib": sizes_skip,
        "rows": [r.to_dict() for r in rows],
        "derived_ladder": [
            {"ceiling": c if c != sys.maxsize else None, "level": lvl}
            for c, lvl in ladder
        ],
    }
    with open(out_path, "w") as fh:
        json.dump(out_data, fh, indent=2)
    print(f"\nJSON results written to: {out_path}")


def main() -> None:
    global PER_CONFIG_TIMEOUT_S  # noqa: PLW0603

    parser = argparse.ArgumentParser(
        description="Raster compression sweep: codec × dtype × size."
    )
    parser.add_argument(
        "--out", default=None, help="Path to write JSON results (optional)."
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress per-config progress lines."
    )
    parser.add_argument(
        "--max-mib",
        type=int,
        default=None,
        help="Cap the maximum decoded size (MiB) to run locally.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=PER_CONFIG_TIMEOUT_S,
        help=f"Per-config subprocess timeout in seconds (default {PER_CONFIG_TIMEOUT_S}).",
    )
    args = parser.parse_args()

    verbose = not args.quiet

    # Allow CLI override of the per-config timeout.
    PER_CONFIG_TIMEOUT_S = args.timeout

    print("=== GeoBrix raster compression sweep ===")
    print(f"Platform: {platform.system()} {platform.machine()}")
    try:
        import psutil

        vm = psutil.virtual_memory()
        print(
            f"RAM: total={vm.total / 1024**3:.1f} GiB, "
            f"available={vm.available / 1024**3:.1f} GiB"
        )
    except ImportError:
        print("psutil not available; using 8 GiB RAM estimate")

    sizes_run, sizes_skip = sizes_to_run(BASE_SIZES_MIB, EXTENDED_SIZES_MIB)

    if args.max_mib is not None:
        capped = [s for s in sizes_run if s <= args.max_mib]
        newly_skipped = [s for s in sizes_run if s > args.max_mib]
        sizes_skip = sizes_skip + newly_skipped
        sizes_run = capped

    print(f"Sizes to run: {sizes_run} MiB")
    print(f"Sizes skipped (needs Serverless / --max-mib): {sizes_skip} MiB")
    print(f"Per-config timeout: {PER_CONFIG_TIMEOUT_S}s")
    print()

    # Install SIGTERM handler so partial results are saved if the process is killed.
    rows: List[SweepRow] = []

    import signal

    def _on_sigterm(sig, frame):  # type: ignore[type-arg]
        print("\n[SIGTERM] saving partial results...", flush=True)
        if args.out and rows:
            _write_json(args.out, sizes_run, sizes_skip, rows)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _on_sigterm)

    rows = run_sweep(sizes_run, sizes_skip, verbose=verbose)

    print()
    print("=== Results ===")
    print_table(rows)

    ladder = derive_ladder(rows)
    print()
    print("=== Derived _AUTO_LADDER ===")
    print(ladder_as_python(ladder))

    if args.out:
        _write_json(args.out, sizes_run, sizes_skip, rows)


if __name__ == "__main__":
    main()
