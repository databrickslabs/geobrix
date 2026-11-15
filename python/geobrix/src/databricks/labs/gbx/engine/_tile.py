"""The tile checks the engine's UDFs make on every row, without importing the light function module.

``pyrx/functions.py`` is about 10,000 lines and costs a fresh Python worker over 0.3 s to import; a worker that only
runs the engine must not pay it. The light module re-exports these under its old names.
"""

from __future__ import annotations

import sys
from typing import Optional

_VIRTUAL_TILE_MODULE = "databricks.labs.gbx.pyrx.core.virtual_tile"


def _is_virtual_tile(tile) -> bool:
    """Whether *tile* is a ``VirtualTile``. Its module imports pyspark, so it is not imported here: an instance exists only
    once that module is loaded, so a process that never loaded it holds none (a Unity Catalog sandbox).
    """
    mod = sys.modules.get(_VIRTUAL_TILE_MODULE)
    return mod is not None and isinstance(tile, mod.VirtualTile)


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
    if _is_virtual_tile(tile):
        return tile.raster is None and tile.path is None
    d = _as_dict(tile)
    return d.get("raster") is None and d.get("path") is None


def cellid(tile) -> int:
    """Extract ``cellid`` from any tile shape (defaults to 0)."""
    if isinstance(tile, (bytes, bytearray)):
        return 0
    if _is_virtual_tile(tile):
        return int(tile.cellid)
    return int(_as_dict(tile).get("cellid") or 0)


def error_tile_of(tile) -> Optional[dict]:
    """*tile* as a dict when it is an error tile, else None. The one test every pass-through shares.

    An error tile is a swallowed failure: no raster, no path, and its reason under ``last_error`` or
    ``error_message``. A tile that still has pixels is never one (the merge aggregates stamp a warning
    under ``last_error`` on good tiles).
    """
    if tile is None or isinstance(tile, (bytes, bytearray)) or _is_virtual_tile(tile):
        return None
    d = _as_dict(tile)
    if d.get("raster") is not None or d.get("path") is not None:
        return None
    md = d.get("metadata") or {}
    return d if ("last_error" in md or "error_message" in md) else None


# The fields of the v2 tile struct, in order (``pyrx._serde.V2_TILE_SCHEMA``; a contract test holds the two equal). Here
# so that a Unity Catalog handler builds the session's tile without pyspark.
TILE_FIELDS = (
    "cellid",
    "raster",
    "path",
    "path_mode",
    "window",
    "clip_polygon",
    "clip_crs",
    "crs",
    "metadata",
)


def engine_metadata(res) -> dict:
    """The metadata of an engine tile, the same on every level and surface: the output's format and the command for a
    success; GDAL's reason under `last_error` and `error_message` for a failure."""
    if res.error is not None:
        return {"last_error": res.error, "error_message": res.error}
    return {"driver": "GTiff", "compression": "ZSTD", "last_command": res.command}


def engine_tile(res, cellid) -> Optional[dict]:
    """The tile of an engine result (``run.Result``), None for a NULL result: the raster, or an error tile with the reason,
    and the cell id of the input; every other field empty."""
    if res is None:
        return None
    row = dict.fromkeys(TILE_FIELDS)
    row.update(cellid=cellid, raster=res.raster, metadata=engine_metadata(res))
    return row
