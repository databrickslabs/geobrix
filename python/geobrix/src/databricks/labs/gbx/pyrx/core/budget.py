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
_BUDGETS = {"serverless": 512 * _MIB, "classic": 1536 * _MIB, "none": 0}
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
