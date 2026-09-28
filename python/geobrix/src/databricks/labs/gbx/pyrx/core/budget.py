"""Decoded-memory budget resolution + layout-aware tile geometry.

`splitStrategy` resolves to a per-tile DECODED-byte budget; `plan_layout`
turns that budget into concrete windows honoring physical layout (row-bands
for striped sources, block-snapped grid for tiled). Budget math is always on
decoded size (w*h*bands*itemsize), never encoded bytes.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

_MIB = 1024 * 1024
# serverless: 64 MiB decoded/tile. The Serverless PySpark UDF has a hard 1 GB
# memory cap. With one-tile-per-partition each task encodes exactly ONE tile, so
# the constraint is the single-tile encode PEAK plus Serverless worker overhead
# (Spark/Arrow/GDAL working set, ~350+ MiB) — NOT tile count or concurrency.
# Empirically bisected on an 8-core Serverless worker (0.5 GiB striped source):
# 96 MiB/tile passes, 128 MiB fails. 64 MiB is chosen for ~50% margin under the
# proven-safe 96, absorbing multiband/dtype/worker-size and encode-peak variation.
# classic has no UDF cap (bounded by executor memory); its larger budget is fine
# on typical instances — very large tiles carry a ~10x encode-peak multiplier, so
# small classic executors may need a smaller sizeInMB override.
_BUDGETS = {"serverless": 64 * _MIB, "classic": 1536 * _MIB, "none": 0}
_MAX_TILES = 512


def runtime_kind() -> str:
    """Driver-side runtime probe (NO Spark API). Defaults to 'serverless' (safe)."""
    if os.environ.get("IS_SERVERLESS", "").lower() in ("true", "1"):
        return "serverless"
    # Classic clusters expose an executor/worker memory env; Serverless does not.
    if os.environ.get("SPARK_WORKER_MEMORY") or os.environ.get("SPARK_EXECUTOR_MEMORY"):
        return "classic"
    return "serverless"


def resolve_strategy(strategy: str) -> str:
    s = (strategy or "auto").strip().lower()
    if s == "auto":
        return runtime_kind()
    if s not in _BUDGETS:
        raise ValueError(
            f"splitStrategy must be one of auto|serverless|classic|none; got '{strategy}'"
        )
    return s


def decoded_budget_bytes(strategy: str) -> int:
    return _BUDGETS[resolve_strategy(strategy)]


@dataclass(frozen=True)
class LayoutPlan:
    tiles: List[Tuple[int, int, int, int]]  # (col_off, row_off, w, h)
    degraded: bool


def _row_bands(width, height, per_row_bytes, budget_bytes, max_tiles):
    rows_per_band = max(1, budget_bytes // max(1, per_row_bytes))
    n = math.ceil(height / rows_per_band)
    degraded = False
    if n > max_tiles:
        rows_per_band = math.ceil(height / max_tiles)
        n = math.ceil(height / rows_per_band)
        degraded = True
    tiles = []
    row = 0
    while row < height:
        h = min(rows_per_band, height - row)
        tiles.append((0, row, width, h))
        row += rows_per_band
    return LayoutPlan(tiles=tiles, degraded=degraded)


def _block_grid(width, height, bytes_per_px, budget_bytes, bx, by, max_tiles):
    # Power-of-4 rounds until per-tile decoded bytes <= budget or tile cap hit.
    decoded = width * height * bytes_per_px
    k = 0
    degraded = False
    while (decoded >> (2 * k)) > budget_bytes:
        if (1 << (2 * (k + 1))) > max_tiles:
            degraded = True
            break
        k += 1
    n = 1 << k
    # Snap tile dims up to a block multiple so reads pull whole blocks.
    tile_w = min(width, _ceil_to(math.ceil(width / n), bx or 1))
    tile_h = min(height, _ceil_to(math.ceil(height / n), by or 1))
    tiles = []
    row = 0
    while row < height:
        col = 0
        h = min(tile_h, height - row)
        while col < width:
            w = min(tile_w, width - col)
            tiles.append((col, row, w, h))
            col += tile_w
        row += tile_h
    return LayoutPlan(tiles=tiles, degraded=degraded)


def _ceil_to(v, m):
    return v if m <= 1 else ((v + m - 1) // m) * m


def plan_layout(
    width: int,
    height: int,
    bands: int,
    dtype_itemsize: int,
    tiled: bool,
    blockxsize: Optional[int],
    blockysize: Optional[int],
    budget_bytes: int,
    max_tiles: int = _MAX_TILES,
) -> LayoutPlan:
    bytes_per_px = max(1, bands) * max(1, dtype_itemsize)
    if budget_bytes <= 0:
        return LayoutPlan(tiles=[(0, 0, width, height)], degraded=False)
    if width * height * bytes_per_px <= budget_bytes:
        return LayoutPlan(tiles=[(0, 0, width, height)], degraded=False)
    if tiled:
        return _block_grid(
            width, height, bytes_per_px, budget_bytes, blockxsize, blockysize, max_tiles
        )
    per_row_bytes = width * bytes_per_px
    return _row_bands(width, height, per_row_bytes, budget_bytes, max_tiles)


# ---------------------------------------------------------------------------
# Budget authority — driver_merge (Phase 1) + dense_alloc (Phase 2) intents
# ---------------------------------------------------------------------------

_MERGE_FALLBACK_BYTES = 512 * _MIB  # conservative floor if the RAM probe fails
_DENSE_RESERVE_MB = (
    6 * 1024
)  # 6 GiB — GPU dense-alloc node baseline (= mvs.recommend_dense_allocation default)


@dataclass(frozen=True)
class BudgetDecision:
    """Result of budget_for: action + the budget that applied + a log/error reason."""

    action: str  # "stream" | "fuse" | "driver" | "error" | "ok"
    budget_bytes: int
    reason: str = ""


def _import_task_context():
    """Return pyspark.TaskContext (or None if pyspark absent). Seam for tests."""
    try:
        from pyspark import TaskContext

        return TaskContext
    except Exception:  # noqa: BLE001
        return None


def _assert_driver(intent: str) -> None:
    """RAM-measured intents are DRIVER-ONLY: /proc/meminfo on a Spark worker reports
    node-total RAM (not the ~1 GB task quota), so a worker-side budget would be
    dangerously large. Fail loud rather than risk OOM."""
    tc = _import_task_context()
    if tc is not None and tc.get() is not None:
        raise RuntimeError(
            f"budget_for({intent!r}) is driver-only: /proc/meminfo on a Spark worker "
            f"reports node-total RAM, not the task quota. Call it from the driver "
            f"(e.g. DataSource commit()), never from write()/a UDF."
        )


def _probe_infra():
    """(host_ram_available_mb, gpu_count) via gpu_infra(); (0, 0) on any failure.
    DRIVER-ONLY (see _assert_driver)."""
    try:
        from databricks.labs.gbx.pyrx.mvs import gpu_infra

        infra = gpu_infra()
        avail = int(infra.get("host_ram_available_mb") or infra.get("host_ram_mb") or 0)
        return avail, int(infra.get("gpu_count", 0))
    except Exception:  # noqa: BLE001
        return 0, 0


def _resolve_avail(infra=None):
    """(avail_mb, gpu_count) from an injected infra dict, or probe live via _probe_infra().
    When infra is provided (hermetic tests, or callers that already ran gpu_infra()), no
    shell-out occurs. Otherwise delegates to _probe_infra() -> gpu_infra()."""
    if infra is not None:
        avail = int(infra.get("host_ram_available_mb") or infra.get("host_ram_mb") or 0)
        return avail, int(infra.get("gpu_count", 0))
    return _probe_infra()


def _driver_merge_budget(override_mb, infra=None):
    """Returns (budget_bytes, avail_mb, gpu). Budget = (avail - reserve)/2 (2x merge
    peak); reserve 6 GiB if GPU else 2 GiB; floor _MERGE_FALLBACK_BYTES; override wins.
    """
    override = (
        override_mb
        or os.environ.get("GBX_LIDAR_MERGE_MAX_MB")
        or os.environ.get("GBX_MERGE_MAX_MB")
    )
    if override:
        return max(int(float(override) * _MIB), 0), 0, 0
    avail_mb, gpu = _resolve_avail(infra)
    if avail_mb <= 0:
        return _MERGE_FALLBACK_BYTES, avail_mb, gpu
    reserve_mb = (6 if gpu > 0 else 2) * 1024
    usable_mb = max(0, avail_mb - reserve_mb)
    return max((usable_mb * _MIB) // 2, _MERGE_FALLBACK_BYTES), avail_mb, gpu


def budget_for(intent, est_bytes=None, *, override_mb=None, infra=None):
    """Driver-side RAM-measured budget authority (driver_merge, dense_alloc only).

    worker_read and cog_write are intentionally NOT budget_for intents: a Serverless
    worker cannot RAM-probe its ~1 GB task quota (/proc/meminfo reports node total),
    so those use the empirically bisected binary cap in materialize_decision (file_gbx.py).
    tile_split uses decoded_budget_bytes(strategy) in this module instead.

    Both implemented intents are _assert_driver-guarded. Returns a BudgetDecision.

    session/kind/override_bytes are intentionally NOT parameters: they would only
    serve the deferred worker/cog facade over materialize_decision/decoded_budget_bytes.
    Add them alongside that facade if it is ever built, not speculatively (phase2 spec)."""
    if intent == "driver_merge":
        _assert_driver(intent)
        budget, avail_mb, gpu = _driver_merge_budget(override_mb, infra=infra)
        ctx = (
            f"{gpu}xGPU detected, RAM avail {avail_mb} MiB"
            if avail_mb
            else "RAM probe unavailable -> floor"
        )
        if est_bytes is not None and est_bytes > budget:
            remedy = (
                "increase mergeMaxMB or run on a node with more RAM"
                if override_mb
                else "use mergeMaxMB=<MiB> to override or run on a node with more RAM"
            )
            return BudgetDecision(
                "error",
                budget,
                f"lidar_gbx merge: estimated {est_bytes / 1e6:.0f} MB exceeds the "
                f"compute-aware budget of {budget / 1e6:.0f} MB (host-RAM-based; {ctx}); "
                f"{remedy}.",
            )
        merging = f", merging {est_bytes / 1e6:.0f} MB" if est_bytes is not None else ""
        return BudgetDecision(
            "ok",
            budget,
            f"{gpu}xGPU · RAM avail {avail_mb} MiB → budget {budget / 1e6:.0f} MB{merging}",
        )
    if intent == "dense_alloc":
        _assert_driver(intent)
        override = override_mb or os.environ.get("GBX_DENSE_ALLOC_MAX_MB")
        if override:
            usable_bytes = max(int(float(override) * _MIB), 0)
            return BudgetDecision(
                "ok", usable_bytes, f"dense_alloc: override {override} MiB"
            )
        avail_mb, gpu = _resolve_avail(infra)
        usable_bytes = max(0, avail_mb - _DENSE_RESERVE_MB) * _MIB
        ctx = (
            f"{gpu}xGPU detected, RAM avail {avail_mb} MiB"
            if avail_mb
            else "RAM probe unavailable"
        )
        return BudgetDecision(
            "ok",
            usable_bytes,
            f"dense_alloc: {usable_bytes / 1e9:.1f} GB usable ({ctx})",
        )
    raise ValueError(
        f"budget_for: unknown intent {intent!r}. "
        f"RAM-measured driver intents: driver_merge, dense_alloc. "
        f"Worker-side binary caps: use materialize_decision (file_gbx.py) for "
        f"worker_read/cog_write. Tile splitting: use decoded_budget_bytes(strategy)."
    )
