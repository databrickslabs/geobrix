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
# serverless: 64 MiB decoded/tile fallback (when no cgroup limit is detectable).
# The Serverless PySpark UDF has a hard 1 GB memory cap. With one-tile-per-partition
# each task encodes exactly ONE tile, so the constraint is the single-tile encode PEAK
# plus Serverless worker overhead (Spark/Arrow/GDAL working set, ~350+ MiB) — NOT tile
# count or concurrency.  Empirically bisected on an 8-core Serverless worker (0.5 GiB
# striped source): 96 MiB/tile passes, 128 MiB fails.  64 MiB is chosen for ~50% margin
# under the proven-safe 96, absorbing multiband/dtype/worker-size and encode-peak
# variation.  When the cgroup memory limit is readable (K8s pods), 15% of the pod limit
# is used instead (floor: 32 MiB).
# classic has no UDF cap (bounded by executor memory); its larger budget is fine on
# typical instances — very large tiles carry a ~10x encode-peak multiplier, so small
# classic executors may need a smaller sizeInMB override.
_BUDGETS = {"serverless": 64 * _MIB, "classic": 1536 * _MIB, "none": 0}
_MAX_TILES = 512

# cgroup memory-limit paths (worker-safe: readable from any Spark task on Linux)
_CGROUP_V2_MEM = "/sys/fs/cgroup/memory.max"
_CGROUP_V1_MEM = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
_1_TIB = 1024 * _MIB * 1024  # treat > 1 TiB as "unlimited"


def _read_cgroup_file(path: str) -> str:
    """Read and return the stripped contents of a cgroup file. Seam for tests."""
    with open(path) as fh:
        return fh.read().strip()


def _cgroup_task_limit_bytes() -> Optional[int]:
    """Return the cgroup memory limit for this task (worker-safe).

    Does NOT call _assert_driver; does NOT read /proc/meminfo.
    Tries cgroup v2 (/sys/fs/cgroup/memory.max) first, then cgroup v1
    (/sys/fs/cgroup/memory/memory.limit_in_bytes).  Returns None when the
    limit is effectively unlimited or all cgroup files are unreadable.
    Never raises.
    """
    for path in (_CGROUP_V2_MEM, _CGROUP_V1_MEM):
        try:
            raw = _read_cgroup_file(path)
        except OSError:
            continue
        if raw == "max":
            # cgroup v2 unlimited sentinel
            return None
        try:
            val = int(raw)
        except ValueError:
            continue
        # cgroup v1 uses 2^63-ish (~9.2e18) as the unlimited sentinel;
        # also treat any value above 1 TiB as effectively unlimited.
        if val > _1_TIB:
            return None
        return val
    return None


# The environment keys a classic cluster sets and Serverless does not. Tests that fake a compute kind read
# these names from here, so the names stay in this one file.
CLASSIC_MEMORY_ENV = ("SPARK_WORKER_MEMORY", "SPARK_EXECUTOR_MEMORY")


def runtime_kind() -> str:
    """Driver-side runtime probe (NO Spark API). Defaults to 'serverless' (safe)."""
    if os.environ.get("IS_SERVERLESS", "").lower() in ("true", "1"):
        return "serverless"
    # Classic clusters expose an executor/worker memory env; Serverless does not.
    if any(os.environ.get(k) for k in CLASSIC_MEMORY_ENV):
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


# The session key of the read cap: the decoded size, in bytes, a materialized tile may have where a function reads its
# bytes (the JVM's RasterDriver.readFromBytes and the light _serde.open_tile). It lives here, not in ds, so the heavy
# tier's registrar can name it without importing the light readers (rasterio).
READ_CAP_KEY = "spark.databricks.labs.gbx.read_cap"


def decoded_budget_bytes(strategy: str) -> int:
    s = resolve_strategy(strategy)
    if s == "serverless":
        limit = _cgroup_task_limit_bytes()
        if limit is not None:
            return max(int(limit * 0.15), 32 * _MIB)
        return _BUDGETS["serverless"]
    return _BUDGETS[s]


# GDAL's raster block cache, for the sites that cap it (the raster reader's opens, the STAC window
# download, the COG file conversion; all through ``_env.capped_block_cache``). Left alone it is a
# share of the machine's RAM, which on a Serverless task is far more than the task may hold.
# Those sites take a share of the tile budget instead: sequential reads of one window need to keep a
# few blocks, not the window, so 1/16 of the decoded bytes a tile may take, and never more than
# _BLOCK_CACHE_MAX. The streaming COG copy passes a lower ceiling (STREAMING_BLOCK_CACHE_MAX):
# the memory of that copy, and of the streamed merge built on it, grows with its cache.
_BLOCK_CACHE_SHARE = 16
_BLOCK_CACHE_MAX = 128 * _MIB
STREAMING_BLOCK_CACHE_MAX = 4 * _MIB
# GDAL's cache holds a block at a time at the least: a block of a 512-pixel float32 tile is 1 MiB, and a cache smaller
# than the block being read thrashes (the same block read again for each of its rows). A fold within a tiny budget (the
# rows that lower the cap to a few hundred bytes) still gets this much.
_BLOCK_CACHE_MIN = 1 * _MIB


def block_cache_bytes(
    strategy: str = "auto", *, ceiling: int = _BLOCK_CACHE_MAX
) -> int:
    """Size in bytes for GDAL's block cache in a block of work that reads one tile.

    ``strategy`` is the one :func:`decoded_budget_bytes` takes. ``none`` (no tile splitting)
    says nothing about memory, so it is sized for the runtime it runs on (``auto``).
    """
    s = resolve_strategy(strategy)
    if s == "none":
        s = runtime_kind()
    return block_cache_for(decoded_budget_bytes(s), ceiling=ceiling)


def block_cache_for(budget_bytes: int, *, ceiling: int = _BLOCK_CACHE_MAX) -> int:
    """Size in bytes for GDAL's block cache in a fold that works within ``budget_bytes`` (the same share of it as
    :func:`block_cache_bytes` takes of a tile budget): the many members a fold keeps open are read a window at a
    time, and their blocks must not pile up to the machine's default cache. Never less than one block of a large tile
    (1 MiB), whatever the budget, unless the ceiling is lower."""
    share = max(_BLOCK_CACHE_MIN, int(budget_bytes) // _BLOCK_CACHE_SHARE)
    return min(int(ceiling), share)


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
# Budget authority — driver_merge + dense_alloc intents
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
        from databricks.labs.gbx.rasterx.mvs import gpu_infra

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

    Worker-side reads, writes, ingests and COG writes (the kinds of materialize_decision) are
    intentionally NOT budget_for intents: a Serverless worker cannot RAM-probe its ~1 GB task
    quota (/proc/meminfo reports node total), so those use the empirically bisected binary cap
    in materialize_decision (file_gbx.py).
    tile_split uses decoded_budget_bytes(strategy) in this module instead.

    Both implemented intents are _assert_driver-guarded. Returns a BudgetDecision.

    session/kind/override_bytes are intentionally NOT parameters: they would only
    serve the deferred worker/cog facade over materialize_decision/decoded_budget_bytes.
    Add them alongside that facade if it is ever built, not speculatively (phase2 spec).
    """
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
        f"a read, write, ingest or cog_write. Tile splitting: use decoded_budget_bytes(strategy)."
    )


def session_read_cap(spark) -> Optional[int]:
    """Driver side: the read cap a registration, or a Column-API call, carries to its workers.

    On a classic session it is the session's :data:`READ_CAP_KEY` when set, else the classic decoded budget (the cap the
    JVM reads there). On a Spark Connect session, whose configuration is not read here, it is the classic budget when the
    DRIVER's compute is not Serverless (standard access mode is Connect too, and its workers can read neither
    ``SPARK_WORKER_MEMORY`` nor a cgroup limit, so their own budget would be Serverless's 64 MiB), else None: a
    Serverless worker then uses its own budget (``ds.file_gbx.tile_read_cap``), which is the right one there.

    The light tier keeps the cap it was registered with: it is read here, once per registration, and travels with the
    function. Changing :data:`READ_CAP_KEY` afterwards does not reach a function already registered (the JVM reads
    the key per query): call ``register()`` again after changing it.
    """
    if spark is None:
        return None
    if "connect" in type(spark).__module__:
        return decoded_budget_bytes("classic") if _driver_compute_is_classic() else None
    try:
        value = spark.conf.get(READ_CAP_KEY, None)
    except (
        Exception
    ):  # noqa: BLE001 - a session without a readable configuration carries the default
        value = None
    try:
        cap = int(str(value).strip()) if value is not None else 0
    except ValueError:
        cap = 0
    return cap if cap > 0 else decoded_budget_bytes("classic")


def _driver_compute_is_classic() -> bool:
    """True when the driver's compute is not Serverless, by the two probes the rest of the package uses (the runtime
    version the manager keys on, and the budget's own ``runtime_kind``, which defaults to Serverless when it sees
    nothing). Reads the environment only: no Spark call, so it is safe on a Connect session.
    """
    from databricks.labs.gbx.manager import capabilities

    return not capabilities.is_serverless() and runtime_kind() == "classic"
