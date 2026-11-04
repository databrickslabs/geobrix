"""The tile checks the engine's UDFs make on every row, without importing the light function module.

``pyrx/functions.py`` is about 10,000 lines and costs a fresh Python worker over 0.3 s to import; a worker that only
runs the engine must not pay it. The light module re-exports these under its old names.
"""

from __future__ import annotations

from typing import Optional

from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _as_dict(tile) -> dict:
    return tile.asDict() if hasattr(tile, "asDict") else dict(tile)


def is_empty(tile) -> bool:
    """True when a tile carries neither raster bytes nor a virtual ``path``.

    A v1/v2 materialized tile has ``raster``; a virtual tile has ``path`` (+ ``window``). Either is openable, so
    only a tile missing both is "empty" (the null-guard the UDF bodies branch on). Mirrors the old
    ``tile is None or tile["raster"] is None`` guard for materialized tiles while additionally admitting virtual
    tiles.
    """
    if tile is None:
        return True
    if isinstance(tile, (bytes, bytearray)):
        return len(bytes(tile)) == 0
    if isinstance(tile, VirtualTile):
        return tile.raster is None and tile.path is None
    d = _as_dict(tile)
    return d.get("raster") is None and d.get("path") is None


def cellid(tile) -> int:
    """Extract ``cellid`` from any tile shape (defaults to 0)."""
    if isinstance(tile, (bytes, bytearray)):
        return 0
    if isinstance(tile, VirtualTile):
        return int(tile.cellid)
    return int(_as_dict(tile).get("cellid") or 0)


def error_tile_of(tile) -> Optional[dict]:
    """*tile* as a dict when it is an error tile, else None. The one test every pass-through shares.

    An error tile is a swallowed failure: no raster, no path, and its reason under ``last_error`` or
    ``error_message``. A tile that still has pixels is never one (the merge aggregates stamp a warning
    under ``last_error`` on good tiles).
    """
    if tile is None or isinstance(tile, (bytes, bytearray, VirtualTile)):
        return None
    d = _as_dict(tile)
    if d.get("raster") is not None or d.get("path") is not None:
        return None
    md = d.get("metadata") or {}
    return d if ("last_error" in md or "error_message" in md) else None
